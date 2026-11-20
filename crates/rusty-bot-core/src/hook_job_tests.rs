use super::*;

#[derive(Default)]
struct Reports(Mutex<Vec<HookReport>>);
impl HookReporter for Reports {
    fn report(&self, report: HookReport) {
        self.0.lock().unwrap().push(report);
    }
}

fn invocation(
    supervisor: &HookJobSupervisor,
    reports: Arc<Reports>,
) -> (HookJobSpawner, HookInvocationGuard) {
    supervisor.invocation(
        "extension".into(),
        "hook".into(),
        HookPoint::BeforeRequest,
        reports,
    )
}

async fn drain(supervisor: &HookJobSupervisor) {
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        supervisor.seal_and_drain(),
    )
    .await
    .expect("supervisor did not drain");
}

async fn wait<T>(job: HookJob<T>) -> Result<T, HookJobError> {
    tokio::time::timeout(std::time::Duration::from_secs(2), job.wait())
        .await
        .expect("job did not settle")
}

#[test]
fn policy_rejects_zero_capacity() {
    for policy in [
        HookJobPolicy {
            max_running: 0,
            max_jobs_per_run: 1,
        },
        HookJobPolicy {
            max_running: 1,
            max_jobs_per_run: 0,
        },
    ] {
        assert!(!policy.is_valid());
        assert!(matches!(
            HookJobSupervisor::new(policy, CancellationToken::new()),
            Err(HookJobError::Unavailable)
        ));
    }
    assert!(HookJobPolicy::default().is_valid());
}

#[tokio::test]
async fn invocation_expiry_and_cancelled_run_refuse_admission() {
    let cancelled = CancellationToken::new();
    let supervisor = HookJobSupervisor::new(HookJobPolicy::default(), cancelled.clone()).unwrap();
    let (spawner, guard) = invocation(&supervisor, Arc::new(Reports::default()));
    drop(guard);
    assert!(matches!(
        spawner.spawn(async { Ok(1) }),
        Err(HookJobError::Unavailable)
    ));
    let (spawner, _guard) = invocation(&supervisor, Arc::new(Reports::default()));
    cancelled.cancel();
    assert!(matches!(
        spawner.spawn(async { Ok(1) }),
        Err(HookJobError::Unavailable)
    ));
    supervisor.cancel_and_drain().await;
}

#[tokio::test]
async fn registration_rejects_duplicate_and_foreign_invocations() {
    let supervisor =
        HookJobSupervisor::new(HookJobPolicy::default(), CancellationToken::new()).unwrap();
    let reports = Arc::new(Reports::default());
    let (a, _guard_a) = invocation(&supervisor, reports.clone());
    let (b, _guard_b) = invocation(&supervisor, reports);
    let job = a.spawn(async { Ok(42) }).unwrap();
    assert_eq!(a.register(&job).unwrap(), job.sequence());
    assert!(matches!(a.register(&job), Err(HookJobError::Unavailable)));
    assert!(matches!(b.register(&job), Err(HookJobError::Unavailable)));
    drain(&supervisor).await;
    assert_eq!(job.status(), HookJobStatus::Succeeded);
    assert_eq!(wait(job).await.unwrap(), 42);
    assert!(matches!(
        a.spawn(async { Ok(0) }),
        Err(HookJobError::Unavailable)
    ));
}

#[tokio::test]
async fn running_cap_queues_work_and_releases_permits_in_order() {
    let supervisor = HookJobSupervisor::new(
        HookJobPolicy {
            max_running: 1,
            max_jobs_per_run: 2,
        },
        CancellationToken::new(),
    )
    .unwrap();
    let (spawner, _guard) = invocation(&supervisor, Arc::new(Reports::default()));
    let (release, released) = oneshot::channel();
    let first = spawner
        .spawn(async move {
            released.await.unwrap();
            Ok(1)
        })
        .unwrap();
    let second = spawner.spawn(async { Ok(2) }).unwrap();
    assert_eq!(first.sequence(), 0);
    assert_eq!(second.sequence(), 1);
    assert_eq!(first.status(), HookJobStatus::Running);
    assert_eq!(second.status(), HookJobStatus::Queued);
    assert!(matches!(
        spawner.spawn(async { Ok(3) }),
        Err(HookJobError::Capacity)
    ));
    release.send(()).unwrap();
    assert_eq!(wait(first).await.unwrap(), 1);
    assert_eq!(wait(second).await.unwrap(), 2);
    drain(&supervisor).await;
}

#[tokio::test]
async fn cancellation_drains_running_and_queued_jobs() {
    let supervisor = HookJobSupervisor::new(
        HookJobPolicy {
            max_running: 1,
            max_jobs_per_run: 2,
        },
        CancellationToken::new(),
    )
    .unwrap();
    let (spawner, _guard) = invocation(&supervisor, Arc::new(Reports::default()));
    let first = spawner
        .spawn(std::future::pending::<Result<(), HookError>>())
        .unwrap();
    let second = spawner.spawn(async { Ok(2) }).unwrap();
    second.cancel();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        supervisor.cancel_and_drain(),
    )
    .await
    .unwrap();
    assert_eq!(first.status(), HookJobStatus::Cancelled);
    assert_eq!(second.status(), HookJobStatus::Cancelled);
    assert!(matches!(wait(first).await, Err(HookJobError::Cancelled)));
    assert!(matches!(wait(second).await, Err(HookJobError::Cancelled)));
}

#[tokio::test]
async fn failures_and_panics_report_only_sanitized_lifecycle_codes() {
    let supervisor =
        HookJobSupervisor::new(HookJobPolicy::default(), CancellationToken::new()).unwrap();
    let reports = Arc::new(Reports::default());
    let (spawner, _guard) = invocation(&supervisor, reports.clone());
    let failed = spawner
        .spawn(async { Err::<(), _>(HookError::new("secret-token", "private detail")) })
        .unwrap();
    let panicked = spawner
        .spawn(async {
            panic!("private panic");
            #[allow(unreachable_code)]
            Ok(())
        })
        .unwrap();
    drain(&supervisor).await;
    assert_eq!(failed.status(), HookJobStatus::Failed);
    assert_eq!(panicked.status(), HookJobStatus::Panicked);
    assert!(matches!(wait(failed).await, Err(HookJobError::Failed(_))));
    assert!(matches!(wait(panicked).await, Err(HookJobError::Panicked)));
    let mut codes = reports
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.code.clone())
        .collect::<Vec<_>>();
    codes.sort();
    assert_eq!(codes, ["job_failed", "job_panicked"]);
}

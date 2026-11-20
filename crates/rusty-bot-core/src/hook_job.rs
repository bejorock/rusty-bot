//! Process-local, run-owned hook jobs. These are not durable tasks.
use crate::{HookError, HookPoint, HookReport, HookReporter};
use std::{
    any::Any,
    collections::{HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
};
use tokio::{sync::oneshot, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HookJobPolicy {
    pub max_running: usize,
    pub max_jobs_per_run: usize,
}
impl Default for HookJobPolicy {
    fn default() -> Self {
        Self {
            max_running: 8,
            max_jobs_per_run: 64,
        }
    }
}
impl HookJobPolicy {
    pub fn is_valid(self) -> bool {
        self.max_running > 0 && self.max_jobs_per_run > 0
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HookJobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Panicked,
}
#[derive(Debug, thiserror::Error)]
pub enum HookJobError {
    #[error("job capacity exhausted")]
    Capacity,
    #[error("job scope unavailable or closed")]
    Unavailable,
    #[error("job cancelled")]
    Cancelled,
    #[error("job panicked")]
    Panicked,
    #[error("job failed: {0}")]
    Failed(HookError),
}

type Output = Box<dyn Any + Send>;
type Work = Pin<Box<dyn Future<Output = Result<Output, HookError>> + Send>>;
type Completion = Box<dyn FnOnce(Result<Output, HookJobError>) + Send>;

struct JobState {
    status: HookJobStatus,
}
pub struct HookJob<T> {
    result: oneshot::Receiver<Result<T, HookJobError>>,
    state: Arc<Mutex<JobState>>,
    cancel: CancellationToken,
    scope: Weak<Inner>,
    invocation: Arc<InvocationState>,
    sequence: u64,
}
impl<T> HookJob<T> {
    pub fn status(&self) -> HookJobStatus {
        self.state.lock().unwrap().status
    }
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    pub async fn wait(self) -> Result<T, HookJobError> {
        self.result.await.unwrap_or(Err(HookJobError::Cancelled))
    }
    /// Monotonic creation order within the run, not completion order.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

struct Queued {
    sequence: u64,
    start: oneshot::Sender<()>,
    state: Arc<Mutex<JobState>>,
    cancel: CancellationToken,
    assigned: Arc<AtomicBool>,
}
struct State {
    closed: bool,
    created: u64,
    running: usize,
    queue: VecDeque<Queued>,
    tasks: Vec<JoinHandle<()>>,
    cancellations: Vec<CancellationToken>,
}
struct Inner {
    policy: HookJobPolicy,
    state: Mutex<State>,
    cancelled: CancellationToken,
}
struct InvocationState {
    open: Mutex<bool>,
    registered: Mutex<HashSet<u64>>,
}
/// A callback's admission capability. Clones share expiry with its guard.
#[derive(Clone)]
pub struct HookJobSpawner {
    inner: Arc<Inner>,
    invocation: Arc<InvocationState>,
    report: HookReport,
    reporter: Arc<dyn HookReporter>,
}
/// Closes admission when the hook callback ends, including during unwinding.
pub struct HookInvocationGuard {
    invocation: Arc<InvocationState>,
}
impl Drop for HookInvocationGuard {
    fn drop(&mut self) {
        *self.invocation.open.lock().unwrap() = false;
    }
}
/// The unique run owner. Dropping it aborts monitors and their children; use
/// `cancel_and_drain` or `seal_and_drain` to await cleanup while the runtime lives.
pub struct HookJobSupervisor {
    inner: Arc<Inner>,
}
impl HookJobSupervisor {
    pub fn new(policy: HookJobPolicy, cancelled: CancellationToken) -> Result<Self, HookJobError> {
        if !policy.is_valid() {
            return Err(HookJobError::Unavailable);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                policy,
                cancelled,
                state: Mutex::new(State {
                    closed: false,
                    created: 0,
                    running: 0,
                    queue: VecDeque::new(),
                    tasks: Vec::new(),
                    cancellations: Vec::new(),
                }),
            }),
        })
    }
    pub fn invocation(
        &self,
        extension: String,
        hook: String,
        point: HookPoint,
        reporter: Arc<dyn HookReporter>,
    ) -> (HookJobSpawner, HookInvocationGuard) {
        let invocation = Arc::new(InvocationState {
            open: Mutex::new(true),
            registered: Mutex::new(HashSet::new()),
        });
        (
            HookJobSpawner {
                inner: self.inner.clone(),
                invocation: invocation.clone(),
                report: HookReport {
                    extension,
                    hook,
                    point,
                    code: String::new(),
                },
                reporter,
            },
            HookInvocationGuard { invocation },
        )
    }
    /// Close admission and wait for all accepted work, including unregistered jobs.
    pub async fn seal_and_drain(&self) {
        self.drain(false).await;
    }
    /// Close admission, cancel queued/running work and wait for its monitors.
    pub async fn cancel_and_drain(&self) {
        self.drain(true).await;
    }
    async fn drain(&self, cancel: bool) {
        let tasks = {
            let mut state = self.inner.state.lock().unwrap();
            state.closed = true;
            if cancel {
                for token in &state.cancellations {
                    token.cancel();
                }
            }
            std::mem::take(&mut state.tasks)
        };
        let mut tasks: Vec<_> = tasks.into_iter().map(AbortOnDrop).collect();
        for task in &mut tasks {
            let _ = (&mut task.0).await;
        }
    }
}
impl Drop for HookJobSupervisor {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.closed = true;
        for token in &state.cancellations {
            token.cancel();
        }
        for task in &state.tasks {
            task.abort();
        }
    }
}
impl HookJobSpawner {
    /// Validate and claim a lifecycle registration from this exact invocation.
    /// Returns its creation sequence; duplicates and foreign jobs are rejected.
    pub fn register<T>(&self, job: &HookJob<T>) -> Result<u64, HookJobError> {
        if !Weak::ptr_eq(&job.scope, &Arc::downgrade(&self.inner))
            || !Arc::ptr_eq(&job.invocation, &self.invocation)
        {
            return Err(HookJobError::Unavailable);
        }
        if !self
            .invocation
            .registered
            .lock()
            .unwrap()
            .insert(job.sequence)
        {
            return Err(HookJobError::Unavailable);
        }
        Ok(job.sequence)
    }
    pub(crate) fn spawn<T, F>(&self, future: F) -> Result<HookJob<T>, HookJobError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, HookError>> + Send + 'static,
    {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| HookJobError::Unavailable)?;
        let open = self.invocation.open.lock().unwrap();
        let mut state = self.inner.state.lock().unwrap();
        if !*open || state.closed || self.inner.cancelled.is_cancelled() {
            return Err(HookJobError::Unavailable);
        }
        if state.created >= self.inner.policy.max_jobs_per_run as u64 {
            return Err(HookJobError::Capacity);
        }
        let sequence = state.created;
        state.created += 1;
        let job_state = Arc::new(Mutex::new(JobState {
            status: HookJobStatus::Queued,
        }));
        let cancel = self.inner.cancelled.child_token();
        let (result_tx, result) = oneshot::channel();
        let work: Work = Box::pin(async move { future.await.map(|v| Box::new(v) as Output) });
        let completion: Completion = Box::new(move |outcome| {
            let typed = outcome.map(|value| {
                *value
                    .downcast::<T>()
                    .expect("job output type is fixed at creation")
            });
            let _ = result_tx.send(typed);
        });
        let (start, started) = oneshot::channel();
        let assigned = Arc::new(AtomicBool::new(false));
        let monitor = Monitor {
            inner: Arc::downgrade(&self.inner),
            state: job_state.clone(),
            cancel: cancel.clone(),
            sequence,
            assigned: assigned.clone(),
            report: self.report.clone(),
            reporter: self.reporter.clone(),
        };
        let task = runtime.spawn(monitor.run(started, work, completion));
        state.tasks.push(task);
        state.cancellations.push(cancel.clone());
        state.queue.push_back(Queued {
            sequence,
            start,
            state: job_state.clone(),
            cancel: cancel.clone(),
            assigned,
        });
        schedule(&self.inner, &mut state);
        Ok(HookJob {
            result,
            state: job_state,
            cancel,
            scope: Arc::downgrade(&self.inner),
            invocation: self.invocation.clone(),
            sequence,
        })
    }
}
fn schedule(inner: &Inner, state: &mut State) {
    while state.running < inner.policy.max_running {
        let Some(job) = state.queue.pop_front() else {
            break;
        };
        if job.cancel.is_cancelled() {
            continue;
        }
        *job.state.lock().unwrap() = JobState {
            status: HookJobStatus::Running,
        };
        state.running += 1;
        job.assigned.store(true, Ordering::Release);
        let _ = job.start.send(());
    }
}
struct AbortOnDrop<T>(JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Monitor {
    inner: Weak<Inner>,
    state: Arc<Mutex<JobState>>,
    cancel: CancellationToken,
    sequence: u64,
    assigned: Arc<AtomicBool>,
    report: HookReport,
    reporter: Arc<dyn HookReporter>,
}
impl Monitor {
    async fn run(self, started: oneshot::Receiver<()>, work: Work, completion: Completion) {
        let start = tokio::select! { biased; _ = self.cancel.cancelled() => false, result = started => result.is_ok() };
        let result = if start {
            let mut child = AbortOnDrop(tokio::spawn(work));
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => { child.0.abort(); let _ = (&mut child.0).await; Err(HookJobError::Cancelled) },
                outcome = &mut child.0 => match outcome {
                    Ok(Ok(value)) => Ok(value),
                    Ok(Err(error)) => Err(HookJobError::Failed(error)),
                    Err(error) if error.is_panic() => Err(HookJobError::Panicked),
                    Err(_) => Err(HookJobError::Cancelled),
                }
            }
        } else {
            Err(HookJobError::Cancelled)
        };
        let status = match &result {
            Ok(_) => HookJobStatus::Succeeded,
            Err(HookJobError::Failed(_)) => HookJobStatus::Failed,
            Err(HookJobError::Panicked) => HookJobStatus::Panicked,
            Err(_) => HookJobStatus::Cancelled,
        };
        *self.state.lock().unwrap() = JobState { status };
        if matches!(status, HookJobStatus::Failed | HookJobStatus::Panicked) {
            let mut report = self.report;
            report.code = if status == HookJobStatus::Failed {
                "job_failed"
            } else {
                "job_panicked"
            }
            .into();
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.reporter.report(report)
            }));
        }
        completion(result);
        if let Some(inner) = self.inner.upgrade() {
            let mut state = inner.state.lock().unwrap();
            // A permit may have been assigned before cancellation won the select.
            if let Some(index) = state
                .queue
                .iter()
                .position(|job| job.sequence == self.sequence)
            {
                state.queue.remove(index);
            }
            if self.assigned.load(Ordering::Acquire) {
                state.running -= 1;
            }
            schedule(&inner, &mut state);
        }
    }
}

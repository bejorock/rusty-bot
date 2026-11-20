use super::*;
use rusty_bot_engine::session::{SubmitRequest, WorkItem};
struct Silent;
impl LiveSink for Silent {
    fn publish(&self, _: SessionId, _: LiveFrame) {}
}
#[tokio::test]
async fn placed_input_cancellation_aborts_but_committed_final_answer_wins() {
    for final_answer in [false, true] {
        let (b, mut e, _) = engine().await;
        e.submit(SubmitRequest::Input {
            request_id: "placed".into(),
            text: "hello".into(),
        })
        .await
        .unwrap();
        let task = match e.plan_next().await.unwrap() {
            WorkItem::Generate { task_id, .. } => task_id,
            _ => panic!("expected generation"),
        };
        if final_answer {
            e.complete_generation(task, answer()).await.unwrap();
        }
        assert_eq!(
            e.cancel_accepted_input("placed").await.unwrap(),
            !final_answer
        );
        assert!(!e.cancel_accepted_input("placed").await.unwrap());
        let stored = b.inner.storage.get_task(task).await.unwrap().unwrap();
        assert!(stored.record.state.is_terminal());
        if final_answer {
            assert!(!matches!(
                stored.record.state,
                TaskState::Terminal {
                    outcome: TaskOutcome::Aborted { .. }
                }
            ));
        } else {
            assert!(matches!(
                stored.record.state,
                TaskState::Terminal {
                    outcome: TaskOutcome::Aborted { .. }
                }
            ));
        }
        assert!(matches!(e.plan_next().await.unwrap(), WorkItem::Idle));
    }
}

async fn engine() -> (Bot, SessionEngine, SessionId) {
    let (b, _) = bot(vec![]);
    let cfg = config();
    let id = SessionId::new();
    let e = SessionEngine::open(
        b.deps(workspace(), Arc::new(Silent), &cfg),
        SessionSpec::for_registry(id, cfg, &b.inner.registry).unwrap(),
    )
    .await
    .unwrap();
    (b, e, id)
}
#[tokio::test]
async fn submission_kinds_deduplicate_reject_changes_and_resume() {
    let (b, mut e, id) = engine().await;
    assert!(matches!(
        e.submit(SubmitRequest::Write {
            request_id: "data".into(),
            data: json!({"a":1})
        })
        .await
        .unwrap(),
        SubmitOutcome::Accepted { .. }
    ));
    assert!(matches!(
        e.submit(SubmitRequest::Write {
            request_id: "data".into(),
            data: json!({"a":1})
        })
        .await
        .unwrap(),
        SubmitOutcome::Duplicate { .. }
    ));
    assert!(matches!(
        e.submit(SubmitRequest::Write {
            request_id: "data".into(),
            data: json!(2)
        })
        .await
        .unwrap(),
        SubmitOutcome::Rejected { .. }
    ));
    assert!(matches!(
        e.submit(SubmitRequest::Continue {
            request_id: "data".into()
        })
        .await
        .unwrap(),
        SubmitOutcome::Rejected { .. }
    ));
    e.submit(SubmitRequest::Continue {
        request_id: "continue".into(),
    })
    .await
    .unwrap();
    e.run_until_idle(CancellationToken::new()).await.unwrap();
    assert!(matches!(e.plan_next().await.unwrap(), WorkItem::Idle));
    let cfg = config();
    let reopened = SessionEngine::open(
        b.deps(workspace(), Arc::new(Silent), &cfg),
        SessionSpec::for_registry(id, cfg, &b.inner.registry).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(reopened.session_id(), id);
    assert!(!reopened.registry_version().is_empty());
    assert_eq!(reopened.config().model.model_id, "fake");
    assert!(reopened.selected_hooks().is_empty());
}
#[tokio::test]
async fn accepted_input_can_be_cancelled_before_planning() {
    let (_, mut e, _) = engine().await;
    assert!(e.cancel_accepted_input("missing").await.is_err());
    e.submit(SubmitRequest::Input {
        request_id: "queued".into(),
        text: "hello".into(),
    })
    .await
    .unwrap();
    assert!(e.cancel_accepted_input("queued").await.unwrap());
    assert!(!e.cancel_accepted_input("queued").await.unwrap());
    assert!(matches!(e.plan_next().await.unwrap(), WorkItem::Idle));
}
#[tokio::test]
async fn explicit_abort_records_terminal_state_and_is_idempotent() {
    let (b, mut e, _) = engine().await;
    e.request_abort(None, "nothing").await.unwrap();
    e.abort_task(TaskId(999), "missing").await.unwrap();
    e.submit(SubmitRequest::Input {
        request_id: "abort".into(),
        text: "hello".into(),
    })
    .await
    .unwrap();
    let task = match e.plan_next().await.unwrap() {
        WorkItem::Generate { task_id, .. } => task_id,
        other => panic!("{other:?}"),
    };
    e.request_abort(Some(task), "stop").await.unwrap();
    e.abort_task(task, "stop").await.unwrap();
    e.abort_task(task, "again").await.unwrap();
    e.request_abort(Some(task), "again").await.unwrap();
    let stored = b.inner.storage.get_task(task).await.unwrap().unwrap();
    assert!(matches!(
        stored.record.state,
        TaskState::Terminal {
            outcome: TaskOutcome::Aborted { .. }
        }
    ));
    assert!(stored.record.ended_at.is_some());
    assert!(matches!(e.plan_next().await.unwrap(), WorkItem::Idle));
}
#[tokio::test]
async fn abort_after_generation_closes_pending_tool_without_effect() {
    let (b, mut e, _) = engine().await;
    e.submit(SubmitRequest::Input {
        request_id: "abort-tool".into(),
        text: "hello".into(),
    })
    .await
    .unwrap();
    let task = match e.plan_next().await.unwrap() {
        WorkItem::Generate { task_id, .. } => task_id,
        _ => panic!(),
    };
    e.complete_generation(task, tool_turn("missing", json!({})))
        .await
        .unwrap();
    e.abort_task(task, "recovered cancellation").await.unwrap();
    assert!(b
        .inner
        .storage
        .get_task(task)
        .await
        .unwrap()
        .unwrap()
        .record
        .state
        .is_terminal());
    assert!(matches!(e.plan_next().await.unwrap(), WorkItem::Idle));
}

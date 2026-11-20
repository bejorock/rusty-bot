use super::*;
use rusty_bot_core::{TaskId, ToolOutput};

fn operation(status: ToolOperationStatus, result: Option<Value>) -> ToolOperationRecord {
    ToolOperationRecord {
        task_id: TaskId(1),
        idempotency_key: "read:c1".into(),
        tool_name: "read".into(),
        status,
        request: json!({}),
        result,
        updated_at: 0,
    }
}

#[test]
fn versioned_intents_round_trip_safe_and_unsafe_policies() {
    for policy in [ReplayPolicy::Safe, ReplayPolicy::Unsafe] {
        let record = operation(
            ToolOperationStatus::Intent,
            Some(parallel_intent(policy, false)),
        );
        assert_eq!(recorded_replay(&record), Ok(Some(policy)));
        assert_eq!(record.result.as_ref().unwrap()["replayed"], false);
    }
    assert_eq!(
        recorded_replay(&operation(ToolOperationStatus::Intent, None)),
        Ok(None)
    );
    assert_eq!(
        recorded_replay(&operation(
            ToolOperationStatus::Intent,
            Some(json!({"replayed":true}))
        )),
        Ok(None)
    );
}

#[test]
fn malformed_or_future_intent_metadata_is_rejected() {
    for metadata in [
        json!(null),
        json!({"version":2,"replay":"safe","replayed":false}),
        json!({"version":1,"replay":"safe"}),
        json!({"version":1,"replay":"safe","replayed":"false"}),
        json!({"version":1,"replayed":false}),
        json!({"version":1,"replay":"unknown","replayed":false}),
    ] {
        assert_eq!(
            recorded_replay(&operation(
                ToolOperationStatus::Intent,
                Some(json!({PARALLEL_METADATA:metadata}))
            )),
            Err(())
        );
    }
}

#[test]
fn absent_operations_execute_and_terminal_outcomes_are_reused() {
    assert_eq!(
        recovery_decision(None, ReplayPolicy::Unsafe, ReplayPolicy::Unsafe),
        RecoveryDecision::Execute
    );
    for status in [ToolOperationStatus::Succeeded, ToolOperationStatus::Failed] {
        let record = operation(status, None);
        assert_eq!(
            recovery_decision(Some(&record), ReplayPolicy::Unsafe, ReplayPolicy::Unsafe),
            RecoveryDecision::Reuse
        );
    }
}

#[test]
fn uncertain_effects_require_both_safe_policies_and_allow_only_one_replay() {
    for status in [
        ToolOperationStatus::Intent,
        ToolOperationStatus::Running,
        ToolOperationStatus::Interrupted,
        ToolOperationStatus::Uncertain,
    ] {
        let mut record = operation(status, None);
        assert_eq!(
            recovery_decision(Some(&record), ReplayPolicy::Safe, ReplayPolicy::Safe),
            RecoveryDecision::Reexecute
        );
        assert_eq!(
            recovery_decision(Some(&record), ReplayPolicy::Unsafe, ReplayPolicy::Safe),
            RecoveryDecision::Stop
        );
        assert_eq!(
            recovery_decision(Some(&record), ReplayPolicy::Safe, ReplayPolicy::Unsafe),
            RecoveryDecision::Stop
        );
        record.result = Some(parallel_intent(ReplayPolicy::Safe, true));
        assert_eq!(
            recovery_decision(Some(&record), ReplayPolicy::Safe, ReplayPolicy::Safe),
            RecoveryDecision::Stop
        );
    }
}

#[test]
fn malformed_versioned_replay_flag_fails_closed_and_overrides_legacy_flag() {
    for result in [
        json!({PARALLEL_METADATA:{},"replayed":false}),
        json!({PARALLEL_METADATA:{"replayed":"no"}}),
        json!({"replayed":true}),
    ] {
        let record = operation(ToolOperationStatus::Running, Some(result));
        assert_eq!(
            recovery_decision(Some(&record), ReplayPolicy::Safe, ReplayPolicy::Safe),
            RecoveryDecision::Stop
        );
    }
    let record = operation(
        ToolOperationStatus::Running,
        Some(json!({PARALLEL_METADATA:{"replayed":false},"replayed":true})),
    );
    assert_eq!(
        recovery_decision(Some(&record), ReplayPolicy::Safe, ReplayPolicy::Safe),
        RecoveryDecision::Reexecute
    );
}

#[test]
fn completion_status_follows_error_flag_and_keys_are_repeatable() {
    let output = ToolOutput {
        content: vec![],
        details: None,
        is_error: false,
    };
    assert_eq!(completion_status(&output), ToolOperationStatus::Succeeded);
    assert_eq!(
        completion_status(&ToolOutput {
            is_error: true,
            ..output
        }),
        ToolOperationStatus::Failed
    );
    assert_eq!(idempotency_key("edit", "call-7"), "edit:call-7");
    assert_eq!(
        idempotency_key("edit", "call-7"),
        idempotency_key("edit", "call-7")
    );
    assert_ne!(
        idempotency_key("edit", "call-7"),
        idempotency_key("read", "call-7")
    );
}

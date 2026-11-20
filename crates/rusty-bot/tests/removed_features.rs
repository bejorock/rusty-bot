//! Guard against reintroducing retired SDK plumbing in any workspace crate.
use std::path::Path;

fn assert_clean_sources(directory: &Path) {
    const RETIRED: &[&str] = &[
        "PowerShell",
        "powershell",
        "pwsh",
        "cfg(windows)",
        "cfg(not(unix))]",
        "max_parallel_tools",
        "event_capacity",
        "human_review",
        "tool_interrupts",
        "ExecutionMode",
        "execution_mode",
        "ReviewGate",
        "review_gate",
        "PendingReview",
        "ReviewDriveOutcome",
        "SdkReviewDriver",
        "review_enabled",
        "review_state",
        "PendingInterrupt",
        "ToolInterrupt",
        "interrupt_port",
        "interrupt_state",
        "interrupt_driver",
        "interrupt_enabled",
        "InterruptDriveOutcome",
        "SdkInterruptDriver",
        "InterruptProtocol",
        "ask_user",
        "ChannelSink",
        "RecoveryDecision::Interrupt",
    ];
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            assert_clean_sources(&path);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let source = std::fs::read_to_string(&path).unwrap();
            for retired in RETIRED {
                // A crate-level Unix-only compile error is intentional; runtime
                // non-Unix fallback branches are not.
                if *retired == "cfg(not(unix))]"
                    && source.contains("compile_error!(\"rusty-bot-local requires a Unix host\")")
                {
                    assert_eq!(source.matches(retired).count(), 1);
                    continue;
                }
                assert!(
                    !source.contains(retired),
                    "retired feature {retired:?} remains in {}",
                    path.display()
                );
            }
        }
    }
}

#[test]
fn workspace_sources_do_not_contain_retired_feature_plumbing() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    for entry in std::fs::read_dir(crates).unwrap() {
        let src = entry.unwrap().path().join("src");
        if src.is_dir() {
            assert_clean_sources(&src);
        }
    }
}

#[test]
fn uncertain_effects_are_not_reexecuted_without_explicit_safe_policies() {
    use rusty_bot::{ReplayPolicy, TaskId, ToolOperationRecord, ToolOperationStatus};
    use rusty_bot_engine::{recovery_decision, RecoveryDecision};
    for status in [
        ToolOperationStatus::Intent,
        ToolOperationStatus::Running,
        ToolOperationStatus::Interrupted,
        ToolOperationStatus::Uncertain,
    ] {
        let mut operation = ToolOperationRecord {
            task_id: TaskId(1),
            idempotency_key: "effect:one".into(),
            tool_name: "effect".into(),
            status,
            request: serde_json::json!({}),
            result: None,
            updated_at: 0,
        };
        for (checkpoint, tool) in [
            (ReplayPolicy::Unsafe, ReplayPolicy::Unsafe),
            (ReplayPolicy::Safe, ReplayPolicy::Unsafe),
            (ReplayPolicy::Unsafe, ReplayPolicy::Safe),
        ] {
            assert_eq!(
                recovery_decision(Some(&operation), checkpoint, tool),
                RecoveryDecision::Stop
            );
        }
        assert_eq!(
            recovery_decision(Some(&operation), ReplayPolicy::Safe, ReplayPolicy::Safe),
            RecoveryDecision::Reexecute
        );
        operation.result = Some(serde_json::json!({"replayed":true}));
        assert_eq!(
            recovery_decision(Some(&operation), ReplayPolicy::Safe, ReplayPolicy::Safe),
            RecoveryDecision::Stop
        );
    }
}

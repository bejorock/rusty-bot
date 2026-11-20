//! Pure task-selection helpers. `plan_next` owns commits; this module only
//! decides what committed state is runnable.

use rusty_bot_core::{StoredTask, TaskState};

pub const TURN_TASK_KIND: &str = "rusty-bot.turn";

/// v1 runs one turn at a time: the earliest non-terminal turn task.
pub fn select_task(tasks: &[StoredTask]) -> Option<&StoredTask> {
    tasks
        .iter()
        .filter(|task| !task.record.state.is_terminal())
        .min_by_key(|task| task.record.id)
}

/// Sleeping/Waiting tasks are inert in v1: no wakeups, no busy polling.
pub fn is_runnable(state: &TaskState) -> bool {
    matches!(
        state,
        TaskState::Pending { .. } | TaskState::Running { .. } | TaskState::Completing { .. }
    )
}

pub fn is_terminal(state: &TaskState) -> bool {
    state.is_terminal()
}

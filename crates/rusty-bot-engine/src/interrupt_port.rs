//! Invocation-local interrupt control, not yet attached to the engine scheduler.
use std::sync::Mutex;

use rusty_bot_core::{ToolError, ToolInterruptPort, ToolOutput};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LatchedInterrupt {
    pub(crate) index: u64,
    pub(crate) payload: Value,
}

pub(crate) enum InvocationResult {
    Returned(Result<ToolOutput, ToolError>),
    Interrupted(LatchedInterrupt),
    ProtocolViolation,
}

struct State {
    active: bool,
    cursor: usize,
    answers: Vec<Value>,
    pending: Option<LatchedInterrupt>,
    sentinel: ToolError,
}

pub(crate) struct InvocationInterruptPort {
    state: Mutex<State>,
}

impl InvocationInterruptPort {
    pub(crate) fn new(answers: Vec<Value>) -> Self {
        Self {
            state: Mutex::new(State {
                active: true,
                cursor: 0,
                answers,
                pending: None,
                sentinel: ToolError::new("interrupt_control", uuid::Uuid::new_v4().to_string()),
            }),
        }
    }

    /// Atomically invalidates all retained handles and classifies the returned
    /// result by the authoritative latch, not by an error's public code.
    pub(crate) fn finish(&self, result: Result<ToolOutput, ToolError>) -> InvocationResult {
        let Ok(mut state) = self.state.lock() else {
            return InvocationResult::ProtocolViolation;
        };
        if !state.active {
            return InvocationResult::ProtocolViolation;
        }
        state.active = false;
        if let Some(pending) = &state.pending {
            if result.as_ref().err() == Some(&state.sentinel) {
                return InvocationResult::Interrupted(pending.clone());
            }
            return InvocationResult::ProtocolViolation;
        }
        if state.cursor != state.answers.len() {
            return InvocationResult::ProtocolViolation;
        }
        InvocationResult::Returned(result)
    }
}

impl ToolInterruptPort for InvocationInterruptPort {
    fn interrupt(&self, payload: Value) -> Result<Value, ToolError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if !state.active {
            return Err(unavailable());
        }
        if state.pending.is_some() {
            return Err(state.sentinel.clone());
        }
        if let Some(answer) = state.answers.get(state.cursor).cloned() {
            state.cursor += 1;
            return Ok(answer);
        }
        let index = u64::try_from(state.cursor).map_err(|_| unavailable())?;
        state.pending = Some(LatchedInterrupt { index, payload });
        Err(state.sentinel.clone())
    }
}

fn unavailable() -> ToolError {
    ToolError::new(
        "interrupt_unavailable",
        "no active interrupt-capable invocation",
    )
}

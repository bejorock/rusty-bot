//! Batched cumulative drafts committed to the caller's shared session adapter.
use super::*;
use temporalio_client::{Client, WorkflowExecutionInfo, WorkflowHandle, WorkflowSignalOptions};

/// Cumulative text, not a delta: repeated or out-of-order deliveries cannot duplicate text.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartialReply {
    pub turn_id: u64,
    pub sequence: u64,
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivityInput {
    pub session_id: String,
    pub turn_id: u64,
    pub request: GenerateInput,
}

pub(super) struct ProgressTarget {
    pub client: Client,
    pub execution: WorkflowExecutionInfo,
    pub turn_id: u64,
    pub storage: Arc<dyn SessionStorage>,
    pub session_id: String,
    pub run_id: String,
}

impl ProgressTarget {
    pub async fn forward(
        self,
        mut events: tokio::sync::mpsc::Receiver<rusty_bot_core::StreamEvent>,
    ) -> Result<(), ModelError> {
        let session = WorkflowHandle::<_, ChatSessionWorkflow>::new(self.client, self.execution);
        let mut text = String::new();
        let mut dirty = false;
        let mut sequence = 0;
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let (finished, flush) = tokio::select! {
                event = events.recv() => match event {
                    Some(rusty_bot_core::StreamEvent::TextDelta(delta)) => {
                        text.push_str(&delta);
                        dirty = true;
                        (false, false)
                    }
                    Some(_) => (false, false),
                    None => (true, true),
                },
                _ = tick.tick() => (false, true),
            };
            // Batch at the interval boundary, or flush remaining text at completion.
            if dirty && flush {
                sequence += 1;
                self.storage
                    .change(
                        &self.session_id,
                        rusty_bot_session::SessionChange::Draft {
                            run_id: self.run_id.clone(),
                            turn_id: self.turn_id,
                            sequence,
                            text: text.clone(),
                        },
                    )
                    .await
                    .map_err(|error| {
                        ModelError::new(
                            rusty_bot_core::ModelErrorKind::Network,
                            error.to_string(),
                            true,
                        )
                    })?;
                session
                    .signal(
                        ChatSessionWorkflow::partial_reply,
                        PartialReply {
                            turn_id: self.turn_id,
                            sequence,
                            text: text.clone(),
                        },
                        WorkflowSignalOptions::default(),
                    )
                    .await
                    .map_err(|error| {
                        ModelError::new(
                            rusty_bot_core::ModelErrorKind::Network,
                            format!("partial reply signal failed: {error}"),
                            false,
                        )
                    })?;
                dirty = false;
            }
            if finished {
                return Ok(());
            }
        }
    }
}

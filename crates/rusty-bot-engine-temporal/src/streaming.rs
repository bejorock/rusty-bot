//! Validated batched canonical drafts committed to the shared session adapter.
use super::*;
use temporalio_client::{Client, WorkflowExecutionInfo, WorkflowHandle, WorkflowSignalOptions};

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

pub(super) async fn consume(
    mut events: tokio::sync::mpsc::Receiver<MessageStreamEvent>,
    message_id: String,
    progress: Option<ProgressTarget>,
) -> Result<MessageAccumulator, ModelError> {
    let mut accumulator = MessageAccumulator::default();
    let mut sequence = 0;
    let mut dirty = false;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let (finished, flush) = tokio::select! {
            event = events.recv() => match event {
                Some(event) => {
                    if event.message_id != message_id {
                        return Err(protocol("stream message identity mismatch"));
                    }
                    accumulator.apply(event).map_err(|e| protocol(&e.to_string()))?;
                    dirty = true;
                    (false, false)
                }
                None => (true, true),
            },
            _ = tick.tick() => (false, true),
        };
        if dirty && flush {
            if let Some(target) = &progress {
                // Never publish executable calls or opaque incomplete arguments in a draft.
                let mut blocks: Vec<_> = accumulator
                    .blocks()
                    .iter()
                    .filter(|b| {
                        !matches!(
                            b,
                            ContentBlock::ToolCall { .. } | ContentBlock::InvalidToolCall { .. }
                        ) && b.tool_call().is_none()
                            && !matches!(b, ContentBlock::Annotated { block, .. } if matches!(block.as_ref(), ContentBlock::InvalidToolCall { .. }))
                    })
                    .cloned()
                    .collect();
                if let Some((kind, text)) = accumulator.partial() {
                    match kind {
                        BlockStart::Text => blocks.push(ContentBlock::Text { text: text.into() }),
                        BlockStart::Reasoning => {
                            blocks.push(ContentBlock::Reasoning { text: text.into() })
                        }
                        _ => {}
                    }
                }
                if !blocks.is_empty() {
                    let mut response = HistoryEntry::completed(
                        &message_id,
                        ConversationMessage::Assistant { content: blocks },
                    );
                    response.turn_id = Some(target.turn_id);
                    response.status = MessageStatus::Generating;
                    response.validate().map_err(|e| protocol(&e.to_string()))?;
                    sequence += 1;
                    let text = rusty_bot_session::visible_text(&response.message);
                    target
                        .storage
                        .change(
                            &target.session_id,
                            SessionChange::DraftResponse {
                                run_id: target.run_id.clone(),
                                turn_id: target.turn_id,
                                sequence,
                                response: Box::new(response),
                            },
                        )
                        .await
                        .map_err(|e| {
                            ModelError::new(ModelErrorKind::Network, e.to_string(), true)
                        })?;
                    let session = WorkflowHandle::<_, ChatSessionWorkflow>::new(
                        target.client.clone(),
                        target.execution.clone(),
                    );
                    session
                        .signal(
                            ChatSessionWorkflow::partial_reply,
                            PartialReply {
                                turn_id: target.turn_id,
                                sequence,
                                text,
                            },
                            WorkflowSignalOptions::default(),
                        )
                        .await
                        .map_err(|e| {
                            ModelError::new(
                                ModelErrorKind::Network,
                                format!("partial reply signal failed: {e}"),
                                false,
                            )
                        })?;
                }
            }
            dirty = false;
        }
        if finished {
            return Ok(accumulator);
        }
    }
}
fn protocol(message: &str) -> ModelError {
    ModelError::new(ModelErrorKind::Protocol, message.to_string(), false)
}

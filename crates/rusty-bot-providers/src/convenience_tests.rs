use crate::{
    convenience::*, DiscoveryRequest, GenerationRequest, ModelPage, OutputFormat, Provider,
};
use crate::{ModelError, ModelErrorKind};
use async_trait::async_trait;
use rusty_bot_messages::{
    ChatHistory, ContentBlock, ConversationMessage, FinishReason, GenerationMetadata, HistoryEntry,
    MessageStreamEvent, MessageStreamPayload,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
struct Fake {
    format: OutputFormat,
    entry: HistoryEntry,
    events: usize,
    wrong_identity: bool,
    sequence_gap: bool,
    hang_after_terminal: bool,
}
impl Fake {
    fn new(format: OutputFormat, content: Vec<ContentBlock>) -> Self {
        let mut entry = HistoryEntry::completed("id", ConversationMessage::Assistant { content });
        entry.generation = Some(GenerationMetadata {
            finish_reason: Some(FinishReason::Stop),
            ..Default::default()
        });
        Self {
            format,
            entry,
            events: 0,
            wrong_identity: false,
            sequence_gap: false,
            hang_after_terminal: false,
        }
    }
}
#[async_trait]
impl Provider for Fake {
    fn operation_timeout(&self) -> Option<std::time::Duration> {
        Some(std::time::Duration::from_millis(100))
    }
    fn output_format(&self) -> &OutputFormat {
        &self.format
    }
    fn build_request(&self, _: &GenerationRequest) -> Result<Value, ModelError> {
        Ok(json!({}))
    }
    async fn list_models(
        &self,
        _: DiscoveryRequest,
        _: CancellationToken,
    ) -> Result<ModelPage, ModelError> {
        Err(ModelError::unsupported("fixture"))
    }
    async fn stream(
        &self,
        _: &GenerationRequest,
        tx: mpsc::Sender<MessageStreamEvent>,
        _: CancellationToken,
    ) -> Result<HistoryEntry, ModelError> {
        for sequence in 1..=self.events as u64 {
            tx.send(MessageStreamEvent {
                message_id: "id".into(),
                sequence,
                event: MessageStreamPayload::Start,
            })
            .await
            .unwrap();
        }
        tx.send(MessageStreamEvent {
            message_id: if self.wrong_identity { "other" } else { "id" }.into(),
            sequence: self.events as u64 + if self.sequence_gap { 2 } else { 1 },
            event: MessageStreamPayload::Completed {
                response: Box::new(self.entry.clone()),
            },
        })
        .await
        .unwrap();
        if self.hang_after_terminal {
            std::future::pending::<()>().await;
        }
        Ok(self.entry.clone())
    }
}
fn request() -> GenerationRequest {
    GenerationRequest {
        message_id: "id".into(),
        history: ChatHistory::default(),
        tools: vec![],
    }
}
fn object() -> OutputFormat {
    OutputFormat::JsonObject
}
fn txt(text: &str) -> ContentBlock {
    ContentBlock::text(text)
}
async fn parse(fake: &Fake) -> Result<StructuredResponse<Value>, ModelError> {
    generate_structured(fake, &request(), CancellationToken::new()).await
}
#[tokio::test]
async fn drains_more_than_channel_capacity_and_preserves_authoritative_entry() {
    let mut fake = Fake::new(
        object(),
        vec![
            txt("{\"a\":"),
            ContentBlock::Reasoning {
                text: "ignored".into(),
            },
            txt("1}"),
        ],
    );
    fake.events = 256;
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), parse(&fake))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.value, json!({"a":1}));
    assert_eq!(result.entry, fake.entry);
}
#[tokio::test]
async fn observer_never_forwards_terminal_before_inner_success() {
    let mut fake = Fake::new(object(), vec![txt("{}")]);
    fake.hang_after_terminal = true;
    let provider = ObservedProvider::new(Arc::new(fake), Arc::new(Recorder::default()));
    let (tx, mut rx) = mpsc::channel(8);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        provider.stream(&request(), tx, CancellationToken::new()),
    )
    .await
    .unwrap();
    assert_eq!(result.unwrap_err().kind, ModelErrorKind::Network);
    while let Ok(event) = rx.try_recv() {
        assert!(!matches!(
            event.event,
            MessageStreamPayload::Completed { .. }
        ));
    }
}
#[tokio::test]
async fn observed_buffer_drain_obeys_whole_operation_budget() {
    let provider = ObservedProvider::new(
        Arc::new(Fake::new(object(), vec![txt("{}")])),
        Arc::new(Recorder::default()),
    );
    let (tx, _rx) = mpsc::channel(1);
    // Force a full downstream buffer even though the inner provider can finish.
    tx.send(MessageStreamEvent {
        message_id: "placeholder".into(),
        sequence: 1,
        event: MessageStreamPayload::Start,
    })
    .await
    .unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        provider.stream(&request(), tx, CancellationToken::new()),
    )
    .await
    .unwrap();
    assert_eq!(result.unwrap_err().kind, ModelErrorKind::Network);
}
#[tokio::test]
async fn refuses_wrong_terminal_identity_and_sequence() {
    let mut fake = Fake::new(object(), vec![txt("{}")]);
    fake.wrong_identity = true;
    assert!(parse(&fake).await.is_err());
    fake.wrong_identity = false;
    fake.sequence_gap = true;
    assert!(parse(&fake).await.is_err());
}
#[tokio::test]
async fn enforces_json_mode_and_sanitizes_failures() {
    for (format, text) in [
        (OutputFormat::Text, "{}"),
        (object(), "[1]"),
        (object(), "```json\n{}\n```"),
        (object(), "secret invalid json"),
    ] {
        let err = parse(&Fake::new(format, vec![txt(text)]))
            .await
            .unwrap_err();
        assert!(!err.to_string().contains("secret"));
    }
    let fake = Fake::new(object(), vec![txt("{\"x\":\"secret\"}")]);
    let err = generate_structured::<std::collections::HashMap<String, u64>>(
        &fake,
        &request(),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(!err.to_string().contains("secret"));
}
#[tokio::test]
async fn checks_exact_schema_and_refuses_external_references() {
    let schema = OutputFormat::JsonSchema {
        name: "x".into(),
        schema: json!({"type":"object","properties":{"n":{"type":"integer","minimum":2}},"required":["n"],"additionalProperties":false}),
        strict: false,
    };
    assert!(parse(&Fake::new(schema.clone(), vec![txt("{\"n\":2}")]))
        .await
        .is_ok());
    assert!(parse(&Fake::new(schema, vec![txt("{\"n\":1}")]))
        .await
        .is_err());
    let schema = OutputFormat::JsonSchema {
        name: "x".into(),
        schema: json!({"$ref":"https://example.invalid/schema"}),
        strict: true,
    };
    assert_eq!(
        parse(&Fake::new(schema, vec![txt("{}")]))
            .await
            .unwrap_err()
            .kind,
        ModelErrorKind::InvalidRequest
    );
}
#[tokio::test]
async fn refuses_rich_content_and_non_stop_outcomes() {
    for block in [
        ContentBlock::Refusal {
            text: "secret".into(),
        },
        ContentBlock::Json { value: json!({}) },
        ContentBlock::Extension {
            namespace: "x".into(),
            kind: "x".into(),
            value: json!({}),
        },
    ] {
        let err = parse(&Fake::new(object(), vec![txt("{}"), block]))
            .await
            .unwrap_err();
        assert!(!err.to_string().contains("secret"));
    }
    for reason in [
        FinishReason::Length,
        FinishReason::Refusal,
        FinishReason::ToolCalls,
        FinishReason::Error,
        FinishReason::Other,
    ] {
        let mut fake = Fake::new(object(), vec![txt("{}")]);
        fake.entry.generation.as_mut().unwrap().finish_reason = Some(reason);
        assert!(parse(&fake).await.is_err());
    }
}
#[derive(Default)]
struct Recorder(Mutex<Vec<Observation>>);
impl Observer for Recorder {
    fn observe(&self, observation: Observation) {
        self.0.lock().unwrap().push(observation);
    }
}
#[tokio::test]
async fn observes_outcomes_without_content_and_preserves_events() {
    let mut fixture = Fake::new(object(), vec![txt("{\"secret\":1}")]);
    let mut usage = rusty_bot_messages::TokenUsage {
        input_tokens: Some(8),
        ..Default::default()
    };
    usage
        .input_details
        .insert("secret-content-in-provider-key".into(), 1);
    fixture.entry.generation.as_mut().unwrap().usage = Some(usage);
    let fake = Arc::new(fixture);
    let observer = Arc::new(Recorder::default());
    let observed = ObservedProvider::new(fake.clone(), observer.clone());
    let result: StructuredResponse<Value> =
        generate_structured(&observed, &request(), CancellationToken::new())
            .await
            .unwrap();
    assert_eq!(result.entry, fake.entry);
    assert!(observed
        .list_models(DiscoveryRequest::default(), CancellationToken::new())
        .await
        .is_err());
    let observations = observer.0.lock().unwrap();
    assert!(matches!(
        observations[0],
        Observation::Started {
            operation: ObservedOperation::Stream
        }
    ));
    assert!(matches!(
        observations[1],
        Observation::Completed {
            operation: ObservedOperation::Stream,
            first_event: Some(_),
            ..
        }
    ));
    assert!(matches!(
        observations[3],
        Observation::Failed {
            operation: ObservedOperation::ListModels,
            kind: ModelErrorKind::Unsupported,
            ..
        }
    ));
    assert!(!format!("{observations:?}").contains("secret"));
}
struct Pending {
    cancelled: Arc<AtomicBool>,
}
#[async_trait]
impl Provider for Pending {
    fn build_request(&self, _: &GenerationRequest) -> Result<Value, ModelError> {
        Ok(json!({}))
    }
    async fn list_models(
        &self,
        _: DiscoveryRequest,
        _: CancellationToken,
    ) -> Result<ModelPage, ModelError> {
        unreachable!()
    }
    async fn stream(
        &self,
        _: &GenerationRequest,
        _: mpsc::Sender<MessageStreamEvent>,
        cancel: CancellationToken,
    ) -> Result<HistoryEntry, ModelError> {
        struct OnDrop(CancellationToken, Arc<AtomicBool>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                self.1.store(self.0.is_cancelled(), Ordering::SeqCst);
            }
        }
        let _drop = OnDrop(cancel, self.cancelled.clone());
        std::future::pending().await
    }
}
#[tokio::test]
async fn dropping_generate_drops_stream_and_does_not_cancel_caller() {
    let flag = Arc::new(AtomicBool::new(false));
    let fake = Pending {
        cancelled: flag.clone(),
    };
    let cancel = CancellationToken::new();
    let req = request();
    {
        let future = generate(&fake, &req, cancel.clone());
        tokio::pin!(future);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut future)
                .await
                .is_err()
        );
    }
    assert!(flag.load(Ordering::SeqCst));
    assert!(!cancel.is_cancelled());
}
#[tokio::test]
async fn cancellation_interrupts_pending_provider() {
    let flag = Arc::new(AtomicBool::new(false));
    let fake = Pending {
        cancelled: flag.clone(),
    };
    let cancel = CancellationToken::new();
    let req = request();
    let future = generate(&fake, &req, cancel.clone());
    tokio::pin!(future);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut future)
            .await
            .is_err()
    );
    cancel.cancel();
    assert_eq!(future.await.unwrap_err().kind, ModelErrorKind::Cancelled);
    assert!(flag.load(Ordering::SeqCst));
}

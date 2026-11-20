use super::*;
use rusty_bot_engine::tools::{LegacyEditTool, LegacyLsTool, LegacyReadTool, WriteTool};
struct Sink;
impl LiveSink for Sink {
    fn publish(&self, _: SessionId, _: LiveFrame) {}
}
fn ctx() -> ToolContext {
    ToolContext {
        session_id: SessionId::new(),
        conversation_id: ConversationId(1),
        task_id: TaskId(2),
        files: Arc::new(Files),
        shell: None,
        live: Arc::new(Sink),
        cancel: CancellationToken::new(),
        now_ms: 0,
        image_limits: None,
    }
}
#[tokio::test]
async fn legacy_read_checks_arguments_and_selects_numbered_lines() {
    let c = ctx();
    assert_eq!(LegacyReadTool.replay(), ReplayPolicy::Safe);
    assert_eq!(LegacyReadTool.spec().name, "read");
    for args in [
        json!({}),
        json!({"path":"x","offset":0}),
        json!({"path":"x","limit":0}),
    ] {
        assert!(LegacyReadTool.execute(args, &c).await.unwrap().is_error);
    }
    let out = LegacyReadTool
        .execute(json!({"path":"x","offset":2,"limit":1}), &c)
        .await
        .unwrap();
    assert!(!out.is_error);
    assert!(out.text_content().contains("2\tworld"));
    assert_eq!(out.details.unwrap()["lines"], 1);
    assert!(LegacyReadTool
        .execute(json!({"path":"x","offset":99}), &c)
        .await
        .unwrap()
        .text_content()
        .is_empty());
}
#[tokio::test]
async fn legacy_edit_refuses_empty_missing_and_ambiguous_matches() {
    let c = ctx();
    assert_eq!(LegacyEditTool.spec().name, "edit");
    for args in [
        json!({}),
        json!({"path":"x","old_text":"","new_text":"x"}),
        json!({"path":"x","old_text":"absent","new_text":"x"}),
        json!({"path":"x","old_text":"l","new_text":"x"}),
    ] {
        assert!(LegacyEditTool.execute(args, &c).await.unwrap().is_error);
    }
    let out = LegacyEditTool
        .execute(json!({"path":"x","old_text":"hello","new_text":"new"}), &c)
        .await
        .unwrap();
    assert!(!out.is_error);
    assert_eq!(out.details.unwrap()["bytes"], 9);
}
#[tokio::test]
async fn write_validates_data_types_and_legacy_ls_empty_result() {
    let c = ctx();
    assert!(WriteTool.execute(json!({}), &c).await.unwrap().is_error);
    for value in [Value::Null, json!(true), json!(4), json!({}), json!([])] {
        assert!(
            WriteTool
                .execute(json!({"path":"dir/file","content":value}), &c)
                .await
                .unwrap()
                .is_error
        );
    }
    assert!(
        !WriteTool
            .execute(json!({"path":"dir/file","content":"new"}), &c)
            .await
            .unwrap()
            .is_error
    );
    assert_eq!(LegacyLsTool.spec().name, "ls");
    assert_eq!(LegacyLsTool.replay(), ReplayPolicy::Safe);
    assert!(
        LegacyLsTool
            .execute(json!({"path":3}), &c)
            .await
            .unwrap()
            .is_error
    );
    assert!(!LegacyLsTool.execute(json!({}), &c).await.unwrap().is_error);
}

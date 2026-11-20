use super::*;
use rusty_bot_core::*;
use rusty_bot_memory::MemoryStorage;
use serde_json::json;
fn address() -> DocumentAddress {
    DocumentAddress::new(
        "unit",
        DocumentScope::Conversation(ConversationId(1)),
        "state",
    )
}
fn update(value: i32) -> DocumentWrite {
    DocumentWrite::PutLatest {
        address: address(),
        value: json!(value),
    }
}
#[tokio::test]
async fn clean_reads_and_empty_commit_have_no_effect() {
    let s = MemoryStorage::new();
    let ctx = CommitContext::session(SessionId::new());
    let mut tx = Transaction::new(&s, ctx.clone());
    assert_eq!(tx.context().session_id, ctx.session_id);
    assert!(tx.is_empty());
    assert!(tx
        .read_entry(ConversationId(1), EntryId(1))
        .await
        .unwrap()
        .is_none());
    assert!(tx.read_task(TaskId(1)).await.unwrap().is_none());
    assert!(tx.read_submission(SubmissionId(1)).await.unwrap().is_none());
    assert!(tx
        .read_submission_by_request(ConversationId(1), "x")
        .await
        .unwrap()
        .is_none());
    assert!(tx
        .read_document(&address(), DocumentPoint::Current)
        .await
        .unwrap()
        .is_none());
    tx.boundary();
    assert!(tx.staged_batches().is_empty());
    assert!(tx.commit().await.unwrap().batches.is_empty());
}
#[tokio::test]
async fn table_writes_block_all_table_reads_across_boundaries_but_not_documents() {
    let s = MemoryStorage::new();
    let mut tx = Transaction::new(&s, CommitContext::session(SessionId::new()));
    tx.write(Write::Conversation(ConversationRecord {
        id: ConversationId(1),
        parent: None,
        owner: None,
    }))
    .unwrap();
    assert!(matches!(
        tx.read_entry(ConversationId(1), EntryId(1)).await,
        Err(TxError::ReadAfterWrite { table: "entries" })
    ));
    assert!(matches!(
        tx.read_task(TaskId(1)).await,
        Err(TxError::ReadAfterWrite { table: "tasks" })
    ));
    assert!(matches!(
        tx.read_submission(SubmissionId(1)).await,
        Err(TxError::ReadAfterWrite {
            table: "submissions"
        })
    ));
    assert!(tx
        .read_submission_by_request(ConversationId(1), "x")
        .await
        .is_err());
    assert!(tx
        .read_document(&address(), DocumentPoint::Current)
        .await
        .unwrap()
        .is_none());
    tx.boundary();
    assert_eq!(tx.staged_batches().len(), 1);
    assert!(!tx.is_empty());
    assert!(tx.read_task(TaskId(1)).await.is_err());
    assert!(tx.commit().await.unwrap().is_fully_committed());
}
#[tokio::test]
async fn boundaries_and_deferred_documents_preserve_commit_order() {
    let s = MemoryStorage::new();
    let mut tx = Transaction::new(&s, CommitContext::session(SessionId::new()));
    tx.write(Write::Document(update(1))).unwrap();
    assert!(tx.read_task(TaskId(1)).await.unwrap().is_none());
    tx.boundary();
    tx.defer_document(update(2)).unwrap();
    assert!(!tx.is_empty());
    assert!(matches!(
        tx.defer_document(DocumentWrite::Retire { address: address() }),
        Err(TxError::IneligibleDeferredWrite)
    ));
    let receipt = tx.commit().await.unwrap();
    assert_eq!(receipt.batches.len(), 2);
    assert_eq!(
        s.get_document(&address(), DocumentPoint::Seq(receipt.batches[0].seq))
            .await
            .unwrap()
            .unwrap()
            .value,
        json!(1)
    );
    assert_eq!(
        s.get_document(&address(), DocumentPoint::Current)
            .await
            .unwrap()
            .unwrap()
            .value,
        json!(2)
    );
}

use super::*;
use rusty_bot_session::{SessionChange, SessionStorage, StoredSession};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeInput {
    pub session_id: String,
    pub change: SessionChange,
}

pub struct SessionActivities {
    pub(crate) storage: Arc<dyn SessionStorage>,
}
impl SessionActivities {
    pub fn new(storage: Arc<dyn SessionStorage>) -> Self {
        Self { storage }
    }
}
#[activities]
impl SessionActivities {
    #[activity]
    pub async fn load(
        self: Arc<Self>,
        ctx: ActivityContext,
        session_id: String,
    ) -> Result<StoredSession, ActivityError> {
        self.storage
            .change(
                &session_id,
                SessionChange::Claim {
                    run_id: ctx
                        .info()
                        .workflow_run_id
                        .clone()
                        .expect("Workflow Activity"),
                },
            )
            .await
            .map_err(storage_error)
    }
    #[activity]
    pub async fn change(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ChangeInput,
    ) -> Result<StoredSession, ActivityError> {
        self.storage
            .change(&input.session_id, input.change)
            .await
            .map_err(storage_error)
    }
}

fn storage_error(error: rusty_bot_session::StorageError) -> ActivityError {
    match error {
        rusty_bot_session::StorageError::Backend(_) => error.into(),
        _ => ActivityError::application(temporalio_sdk::ApplicationFailure::non_retryable(error)),
    }
}

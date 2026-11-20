use rusty_bot_core::{HistoryRequest, ModelError};

/// The projection may only change messages. The engine independently checks
/// the reset shape and boundary; adapters enforce identity and wire validity.
pub(crate) fn same_options(
    original: &HistoryRequest,
    candidate: &HistoryRequest,
) -> Result<(), ModelError> {
    let mut comparable = candidate.clone();
    comparable.messages = original.messages.clone();
    if &comparable != original {
        return Err(ModelError::unsupported(
            "native context reset identity mismatch",
        ));
    }
    Ok(())
}

//! Fail-closed structural validation for the restricted provider payload hook.
#[cfg(test)]
#[path = "payload_validation_tests.rs"]
mod tests;

use crate::{ModelError, ModelErrorKind};
use serde_json::{Map, Value};

fn invalid() -> ModelError {
    ModelError::new(
        ModelErrorKind::InvalidRequest,
        "invalid provider payload mutation",
        false,
    )
}
fn validate_fields(
    map: &Map<String, Value>,
    temperature: &str,
    tokens: &str,
    max: f64,
    required: bool,
) -> Result<(), ModelError> {
    if let Some(value) = map.get(temperature) {
        let number = value
            .as_f64()
            .filter(|n| n.is_finite() && *n >= 0.0 && *n <= max)
            .ok_or_else(invalid)?;
        let _ = number;
    }
    match map.get(tokens) {
        Some(value) if value.as_u64().is_some_and(|n| (1..=1_000_000).contains(&n)) => {}
        None if !required => {}
        _ => return Err(invalid()),
    }
    Ok(())
}
/// Compare each candidate to the original request, never to the previous candidate.
pub fn validate_provider_payload(
    api: &str,
    original: &Value,
    candidate: &Value,
) -> Result<(), ModelError> {
    let (temp, tokens, max, required, gemini) = match api {
        "openai.responses.v1" => ("temperature", "max_output_tokens", 2.0, false, false),
        "anthropic.messages.v1" => ("temperature", "max_tokens", 1.0, true, false),
        "openai.chat_completions.v1" => ("temperature", "max_tokens", 2.0, false, false),
        "gemini.generate_content.v1" => ("temperature", "maxOutputTokens", 2.0, false, true),
        _ => {
            return Err(ModelError::new(
                ModelErrorKind::Unsupported,
                "provider payload hooks unsupported",
                false,
            ))
        }
    };
    let original = original.as_object().ok_or_else(invalid)?;
    let candidate = candidate.as_object().ok_or_else(invalid)?;
    let mut left = original.clone();
    let mut right = candidate.clone();
    if gemini {
        let a = left.remove("generationConfig");
        let b = right.remove("generationConfig");
        match (a, b) {
            (None, None) => {}
            (None, Some(Value::Object(mut new))) => {
                validate_fields(&new, temp, tokens, max, false)?;
                if new.is_empty() {
                    return Err(invalid());
                }
                new.remove(temp);
                new.remove(tokens);
                if !new.is_empty() {
                    return Err(invalid());
                }
            }
            (Some(Value::Object(mut old)), Some(Value::Object(mut new))) => {
                validate_fields(&new, temp, tokens, max, false)?;
                old.remove(temp);
                old.remove(tokens);
                new.remove(temp);
                new.remove(tokens);
                if old != new {
                    return Err(invalid());
                }
            }
            _ => return Err(invalid()),
        }
    } else {
        validate_fields(&right, temp, tokens, max, required)?;
        left.remove(temp);
        left.remove(tokens);
        right.remove(temp);
        right.remove(tokens);
    }
    if left != right {
        return Err(invalid());
    }
    Ok(())
}

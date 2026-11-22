//! Local preflight sizing, deliberately separate from provider-reported usage.
use crate::ModelError;
use crate::{invalid, GenerationRequest, Provider};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tiktoken_rs::{
    tokenizer::{get_tokenizer, Tokenizer},
    CoreBPE,
};

/// Auto uses a known OpenAI encoding, otherwise an explicitly labelled o200k proxy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EstimationEncoding {
    #[default]
    Auto,
    O200kBase,
    Cl100kBase,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenEstimate {
    /// BPE count of the translated request JSON after removing media and opaque
    /// state. Includes JSON/configuration overhead, NOT native chat framing.
    pub estimated_input_tokens: u64,
    pub encoding: String,
    /// A library encoding mapping was found, not a guarantee of billing accuracy.
    pub model_encoding_known: bool,
    pub used_fallback: bool,
    pub omitted_media: u64,
    pub omitted_opaque_fields: u64,
    /// False when media or opaque native state could not be counted.
    pub includes_all_input: bool,
}

/// Estimates the actual translated request, including tools and output schemas.
/// No credentials, network access, tokenizer downloads or model calls are needed.
/// This is a sizing heuristic, NOT a safe upper bound or billing calculation.
/// For Claude/DeepSeek/unknown models Auto uses an o200k text proxy.
pub fn estimate_input_tokens(
    provider: &dyn Provider,
    request: &GenerationRequest,
    encoding: EstimationEncoding,
) -> Result<TokenEstimate, ModelError> {
    let mut body = provider.build_request(request)?;
    let model = body.get("model").and_then(Value::as_str).unwrap_or("");
    let known = get_tokenizer(model);
    let (tokenizer, fallback) = match encoding {
        EstimationEncoding::Auto => (known.unwrap_or(Tokenizer::O200kBase), known.is_none()),
        EstimationEncoding::O200kBase => (Tokenizer::O200kBase, false),
        EstimationEncoding::Cl100kBase => (Tokenizer::Cl100kBase, false),
    };
    let bpe = tiktoken_rs::bpe_for_tokenizer(tokenizer)
        .map_err(|_| invalid("tokenizer initialization failed"))?;
    let mut result = TokenEstimate {
        estimated_input_tokens: 0,
        encoding: format!("{tokenizer:?}"),
        model_encoding_known: known == Some(tokenizer),
        used_fallback: fallback,
        omitted_media: 0,
        omitted_opaque_fields: 0,
        includes_all_input: true,
    };
    scrub(&mut body, &mut result, 0)?;
    let text = serde_json::to_string(&body).map_err(|_| invalid("cannot estimate request"))?;
    result.estimated_input_tokens = count(bpe, &text)?;
    result.includes_all_input = result.omitted_media == 0 && result.omitted_opaque_fields == 0;
    Ok(result)
}
fn count(bpe: &CoreBPE, text: &str) -> Result<u64, ModelError> {
    // Treat literal special-token-looking user text as ordinary text, not control tokens.
    u64::try_from(bpe.encode_ordinary(text).len()).map_err(|_| invalid("token estimate overflow"))
}
fn scrub(value: &mut Value, result: &mut TokenEstimate, depth: usize) -> Result<(), ModelError> {
    if depth > 128 {
        return Err(invalid("request too deep for estimation"));
    }
    match value {
        Value::Object(map) => {
            // Entire native image blocks are placeholders: counting a base64 or URL
            // string as if it were text would be misleading and potentially enormous.
            let image = map
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|t| matches!(t, "image" | "image_url" | "input_image"));
            if image {
                result.omitted_media += 1;
                map.clear();
                return Ok(());
            }
            let native_type = map.get("type").and_then(Value::as_str).unwrap_or("");
            let keys: &[&str] = match native_type {
                "reasoning" => &["encrypted_content"],
                "thinking" => &["signature"],
                "redacted_thinking" => &["data"],
                _ => &[],
            };
            for key in keys {
                if map.remove(*key).is_some() {
                    result.omitted_opaque_fields += 1;
                }
            }
            // Descend only through native message containers, never user tool
            // argument objects, JSON Schema examples, or arbitrary metadata.
            for key in ["input", "messages", "system", "content"] {
                if let Some(child) = map.get_mut(key) {
                    // Anthropic tool_use.input is user data, not a message container.
                    if key != "input" || depth == 0 {
                        scrub(child, result, depth + 1)?;
                    }
                }
            }
        }
        Value::Array(items) => {
            for child in items {
                scrub(child, result, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

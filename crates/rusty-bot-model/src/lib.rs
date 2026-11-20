//! OpenAI-compatible streaming model adapter.
//!
//! Wire types live in `rusty_bot_core::model`; this crate only implements the
//! HTTP/SSE transport and re-exports the shared types for convenience.

pub mod anthropic;
pub mod gemini;
mod native_images;
mod native_reset;
pub mod native_transport;
pub mod openai;
pub mod responses;
pub mod sse;

pub use rusty_bot_core::model::*;

pub use anthropic::AnthropicMessagesProvider;
pub use gemini::GeminiGenerateContentProvider;
pub use native_transport::StaticCredentialResolver;
pub use openai::{OpenAiCompatConfig, OpenAiCompatProvider};
pub use responses::OpenAiResponsesProvider;
pub use sse::SseParser;

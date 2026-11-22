//! Provider-specific prompt caching controls. These affect requests, not history.

/// A breakpoint identifies an existing history entry and its zero-based content block.
/// Anthropic TTL is optional (the provider defaults to five minutes); it must be
/// absent on OpenAI breakpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheMarker {
    pub message_id: String,
    pub block_index: usize,
    pub ttl: Option<AnthropicCacheTtl>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnthropicCacheTtl {
    FiveMinutes,
    OneHour,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenAiCacheRetention {
    InMemory,
    TwentyFourHours,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenAiResponsesCacheMode {
    Implicit,
    Explicit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenAiResponsesCacheTtl {
    ThirtyMinutes,
    TwelveHours,
}

/// Modern Responses cache settings. Explicit breakpoints can also accompany
/// implicit mode; the provider chooses the implicit breakpoint independently.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenAiResponsesCache {
    pub mode: OpenAiResponsesCacheMode,
    pub ttl: Option<OpenAiResponsesCacheTtl>,
    pub markers: Vec<CacheMarker>,
}

/// Model support for cache features and retention varies by provider and model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PromptCache {
    OpenAi {
        key: Option<String>,
        retention: Option<OpenAiCacheRetention>,
        responses: Option<OpenAiResponsesCache>,
    },
    Anthropic {
        markers: Vec<CacheMarker>,
    },
}

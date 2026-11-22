//! Provider-independent token statistics. Unknown counts remain `None`.
use rusty_bot_messages::TokenUsage;

/// Cache counters are subsets of input; reasoning tokens are a subset of output.
/// `cold_input_tokens` excludes both cache reads and cache writes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UsageStatistics {
    pub input_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
    pub cold_input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

/// Read normalized counts from the token usage returned by native providers.
/// Values are only derived where all necessary operands are known.
pub fn statistics(usage: &TokenUsage) -> UsageStatistics {
    let input = &usage.input_details;
    let output = &usage.output_details;
    UsageStatistics {
        input_tokens: usage.input_tokens,
        cached_input_tokens: input.get("cached_input_tokens").copied(),
        cache_read_tokens: input.get("cache_read_tokens").copied(),
        cache_write_tokens: input.get("cache_write_tokens").copied(),
        cold_input_tokens: input.get("cold_input_tokens").copied(),
        output_tokens: usage.output_tokens,
        reasoning_tokens: output.get("reasoning_tokens").copied(),
        total_tokens: usage.total_tokens,
    }
}

/// Normalize after parsing/merging provider usage updates. Source counters are
/// retained, alongside canonical aliases, in the detail maps.
pub(crate) fn normalize(api: crate::Api, u: &mut TokenUsage) {
    let d = &mut u.input_details;
    if api == crate::Api::Anthropic {
        if let Some(n) = u.input_tokens {
            d.insert("cold_input_tokens".into(), n);
        }
        if let Some(n) = d.get("cache_read_input_tokens").copied() {
            d.insert("cache_read_tokens".into(), n);
            d.insert("cached_input_tokens".into(), n);
        }
        if let Some(n) = d.get("cache_creation_input_tokens").copied() {
            d.insert("cache_write_tokens".into(), n);
        }
        u.input_tokens = d
            .get("cold_input_tokens")
            .zip(d.get("cache_read_tokens"))
            .and_then(|(cold, read)| cold.checked_add(*read))
            .zip(d.get("cache_write_tokens"))
            .and_then(|(sum, write)| sum.checked_add(*write));
    } else {
        let read = d
            .get("cached_tokens")
            .or_else(|| d.get("prompt_cache_hit_tokens"))
            .copied();
        if let Some(n) = read {
            d.insert("cached_input_tokens".into(), n);
            d.insert("cache_read_tokens".into(), n);
        }
        let write = d.get("cache_write_tokens").copied();
        // Modern OpenAI write tokens are disjoint from cache reads and cold input.
        // Older APIs omit this field; retain unknown writes, but their miss count
        // (when explicitly provided) remains authoritative.
        if let Some(n) = d.get("prompt_cache_miss_tokens").copied().or_else(|| {
            u.input_tokens
                .zip(read)
                .and_then(|(total, read)| total.checked_sub(read))
                .zip(write)
                .and_then(|(remaining, write)| remaining.checked_sub(write))
        }) {
            d.insert("cold_input_tokens".into(), n);
        }
    }
    if api == crate::Api::Anthropic || u.total_tokens.is_none() {
        u.total_tokens = u
            .input_tokens
            .zip(u.output_tokens)
            .and_then(|(i, o)| i.checked_add(o));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wire_usage_preserves_raw_provider_counts() {
        use serde_json::json;
        for api in [crate::Api::Chat, crate::Api::Deepseek] {
            let mut decoder = crate::wire::Decoder::new(api, "requested-model", "answer");
            for frame in [
                crate::tests::chat(
                    json!({"role":"assistant","content":"hello"}),
                    serde_json::Value::Null,
                ),
                crate::tests::chat(json!({}), json!("stop")),
                json!({"id":"wire-id","model":"actual-model","choices":[],"usage":{"prompt_tokens":8,"completion_tokens":4,"total_tokens":12,"completion_tokens_details":{"reasoning_tokens":2},"prompt_cache_hit_tokens":3,"prompt_cache_miss_tokens":5}}),
            ] {
                decoder.push(&frame.to_string()).unwrap();
            }
            decoder.push("[DONE]").unwrap();
            let entry = decoder.finish().unwrap();
            let meta = entry.generation.unwrap();
            let stats = statistics(meta.usage.as_ref().unwrap());
            assert_eq!(
                (
                    stats.input_tokens,
                    stats.cached_input_tokens,
                    stats.cold_input_tokens,
                    stats.reasoning_tokens,
                    stats.total_tokens
                ),
                (Some(8), Some(3), Some(5), Some(2), Some(12))
            );
            assert_eq!(
                meta.extras["native_usage"]["prompt_cache_hit_tokens"],
                json!(3)
            );
        }
        let mut decoder =
            crate::wire::Decoder::new(crate::Api::Responses, "requested-model", "answer");
        for frame in crate::tests::responses_frames() {
            decoder.push(&frame.to_string()).unwrap();
        }
        let meta = decoder.finish().unwrap().generation.unwrap();
        let stats = statistics(meta.usage.as_ref().unwrap());
        assert_eq!(
            (
                stats.input_tokens,
                stats.output_tokens,
                stats.reasoning_tokens,
                stats.total_tokens
            ),
            (Some(7), Some(8), Some(3), Some(15))
        );
        assert_eq!(
            meta.extras["native_usage"]["output_tokens_details"]["reasoning_tokens"],
            json!(3)
        );
    }

    #[test]
    fn cache_writes_are_not_cold_and_anthropic_totals_are_recomputed() {
        let mut u = TokenUsage {
            input_tokens: Some(12000),
            output_tokens: Some(100),
            ..Default::default()
        };
        u.input_details.insert("cached_tokens".into(), 4000);
        u.input_details.insert("cache_write_tokens".into(), 7000);
        normalize(crate::Api::Responses, &mut u);
        let stats = statistics(&u);
        assert_eq!(
            (
                stats.cold_input_tokens,
                stats.cache_write_tokens,
                stats.total_tokens
            ),
            (Some(1000), Some(7000), Some(12100))
        );
        let mut a = TokenUsage {
            input_tokens: Some(10),
            output_tokens: Some(2),
            total_tokens: Some(12),
            ..Default::default()
        };
        a.input_details.insert("cache_read_input_tokens".into(), 5);
        a.input_details
            .insert("cache_creation_input_tokens".into(), 3);
        normalize(crate::Api::Anthropic, &mut a);
        assert_eq!((a.input_tokens, a.total_tokens), (Some(18), Some(20)));
        let mut missing = TokenUsage {
            input_tokens: Some(10),
            output_tokens: Some(2),
            total_tokens: Some(12),
            ..Default::default()
        };
        normalize(crate::Api::Anthropic, &mut missing);
        assert_eq!((missing.input_tokens, missing.total_tokens), (None, None));
    }

    #[test]
    fn unknowns_are_not_zero_and_arithmetic_is_checked() {
        let mut u = TokenUsage {
            input_tokens: Some(12),
            output_tokens: Some(4),
            ..Default::default()
        };
        u.input_details.insert("cached_tokens".into(), 5);
        normalize(crate::Api::Responses, &mut u);
        let s = statistics(&u);
        assert_eq!(
            (
                s.cached_input_tokens,
                s.cold_input_tokens,
                s.cache_write_tokens,
                s.total_tokens
            ),
            (Some(5), None, None, Some(16))
        );
        let mut a = TokenUsage {
            input_tokens: Some(12),
            output_tokens: Some(4),
            ..Default::default()
        };
        a.input_details.insert("cache_read_input_tokens".into(), 5);
        a.input_details
            .insert("cache_creation_input_tokens".into(), 3);
        normalize(crate::Api::Anthropic, &mut a);
        assert_eq!((a.input_tokens, a.total_tokens), (Some(20), Some(24)));
        let mut missing = TokenUsage {
            input_tokens: Some(12),
            ..Default::default()
        };
        normalize(crate::Api::Anthropic, &mut missing);
        assert_eq!(missing.input_tokens, None);
        assert_eq!(statistics(&missing).cold_input_tokens, Some(12));
    }
}

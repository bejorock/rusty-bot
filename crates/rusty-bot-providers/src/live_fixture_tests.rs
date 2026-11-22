use super::*;
use rusty_bot_messages::{ContentBlock, MessageAccumulator};
use serde_json::{json, Value};

// Sanitized shapes from official SSE captures; identities, text and token counts are synthetic.
const OPENAI: &str = concat!(
    "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"model\":\"m\"}}\n\n",
    "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"r\",\"model\":\"m\"}}\n\n",
    "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"i\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[],\"phase\":\"final_answer\"}}\n\n",
    "event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"item_id\":\"i\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\",\"annotations\":[],\"logprobs\":[]}}\n\n",
    "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"i\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello\",\"logprobs\":[],\"obfuscation\":\"x\"}\n\n",
    "event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"item_id\":\"i\",\"output_index\":0,\"content_index\":0,\"text\":\"hello\",\"logprobs\":[]}\n\n",
    "event: response.content_part.done\ndata: {\"type\":\"response.content_part.done\",\"item_id\":\"i\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"hello\",\"annotations\":[],\"logprobs\":[]}}\n\n",
    "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"i\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\",\"annotations\":[],\"logprobs\":[]}],\"phase\":\"final_answer\"}}\n\n",
    "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"model\":\"m\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"id\":\"i\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\",\"annotations\":[],\"logprobs\":[]}],\"phase\":\"final_answer\"}],\"usage\":{\"input_tokens\":2,\"input_tokens_details\":{\"cached_tokens\":0,\"cache_write_tokens\":0,\"cache_write_tokens_details\":{\"ttl_30m_tokens\":0,\"ttl_12h_tokens\":0}},\"output_tokens\":1,\"output_tokens_details\":{\"reasoning_tokens\":0},\"total_tokens\":3}}}\n\n"
);
const ANTHROPIC: &str = concat!(
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"r\",\"model\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"container\":null,\"stop_reason\":null,\"stop_sequence\":null,\"stop_details\":null,\"usage\":{\"input_tokens\":2,\"output_tokens\":1,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0,\"cache_creation\":{\"ephemeral_5m_input_tokens\":0,\"ephemeral_1h_input_tokens\":0},\"service_tier\":\"standard\",\"inference_geo\":\"not_available\",\"speed\":\"standard\"},\"diagnostics\":null}}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "event: ping\ndata: {\"type\":\"ping\"}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null,\"stop_details\":null,\"container\":null},\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
);

fn decode(api: Api, fixture: &str) -> Result<HistoryEntry, ModelError> {
    let mut sse = sse::Sse::default();
    let mut decoder = wire::Decoder::new(api, "requested-model", "answer");
    let mut acc = MessageAccumulator::default();
    let mut sequence = 0;
    let mut apply = |event| {
        sequence += 1;
        acc.apply(MessageStreamEvent {
            message_id: "answer".into(),
            sequence,
            event,
        })
        .unwrap();
    };
    apply(MessageStreamPayload::Start);
    for chunk in fixture.as_bytes().chunks(17) {
        for data in sse.push(chunk)? {
            for event in decoder.push(&data)? {
                apply(event);
            }
        }
    }
    if let Some(data) = sse.finish()? {
        for event in decoder.push(&data)? {
            apply(event);
        }
    }
    let entry = decoder.finish()?;
    apply(MessageStreamPayload::Completed {
        response: Box::new(entry.clone()),
    });
    assert_eq!(acc.response(), Some(&entry));
    Ok(entry)
}

#[test]
fn official_stream_shapes_replay_roundtrip() {
    for (api, fixture) in [(Api::Responses, OPENAI), (Api::Anthropic, ANTHROPIC)] {
        let entry = decode(api, fixture).unwrap();
        assert_eq!(entry.message.content()[0], ContentBlock::text("hello"));
        let ContentBlock::Replay { state } = entry.message.content().last().unwrap() else {
            panic!("missing replay")
        };
        let replay = state.payload.clone();
        let body =
            request::build(api, &tests::config(), &tests::request(vec![entry.message])).unwrap();
        match api {
            Api::Responses => assert_eq!(body["input"], replay),
            Api::Anthropic => assert_eq!(body["messages"][0]["content"], replay),
            _ => unreachable!(),
        }
    }
}

#[test]
fn decoded_native_outputs_continue_on_foreign_targets() {
    for (source, fixture) in [(Api::Responses, OPENAI), (Api::Anthropic, ANTHROPIC)] {
        let entry = decode(source, fixture).unwrap();
        for target in [Api::Responses, Api::Chat, Api::Anthropic, Api::Deepseek] {
            if source == target {
                continue;
            }
            let body = request::build(
                target,
                &tests::config(),
                &tests::request(vec![entry.message.clone()]),
            )
            .unwrap();
            assert!(body.to_string().contains("hello"));
            assert!(!body.to_string().contains("final_answer"));
        }
    }
}

#[test]
fn responses_commentary_roundtrips_and_unknown_or_null_phase_is_rejected() {
    let fixture = OPENAI.replace("\"phase\":\"final_answer\"", "\"phase\":\"commentary\"");
    let entry = decode(Api::Responses, &fixture).unwrap();
    assert_eq!(entry.message.content()[0], ContentBlock::text("hello"));
    let ContentBlock::Replay { state } = entry.message.content().last().unwrap() else {
        panic!("missing replay")
    };
    assert_eq!(state.payload[0]["phase"], "commentary");
    let replay = state.payload.clone();
    let body = request::build(
        Api::Responses,
        &tests::config(),
        &tests::request(vec![entry.message]),
    )
    .unwrap();
    assert_eq!(body["input"], replay);

    for invalid in ["null", "\"future_phase\""] {
        let invalid_phase = format!("\"phase\":{invalid}");
        // Reject at item addition, item completion, and authoritative response projection.
        for occurrence in 0..3 {
            let mut fixture = OPENAI.to_owned();
            let original = "\"phase\":\"final_answer\"";
            let offset = fixture.match_indices(original).nth(occurrence).unwrap().0;
            fixture.replace_range(offset..offset + original.len(), &invalid_phase);
            assert!(decode(Api::Responses, &fixture).is_err());
        }
        assert!(wire::project(
            Api::Responses,
            &json!([{ "type":"message", "role":"assistant", "content":[], "phase":serde_json::from_str::<Value>(invalid).unwrap() }]),
        )
        .is_err());
    }
}

#[test]
fn unsupported_semantic_stream_metadata_is_rejected() {
    for fixture in [
        OPENAI.replace("\"logprobs\":[]", "\"logprobs\":[{\"token\":\"x\"}]"),
        OPENAI.replace(
            "\"annotations\":[]",
            "\"annotations\":[{\"type\":\"citation\"}]",
        ),
        ANTHROPIC.replace(
            "\"stop_details\":null,\"container\":null",
            "\"stop_details\":{\"type\":\"other\"},\"container\":null",
        ),
        ANTHROPIC.replace(
            "\"stop_details\":null,\"container\":null",
            "\"stop_details\":null,\"container\":{\"id\":\"c\"}",
        ),
    ] {
        let api = if fixture.starts_with("event: response") {
            Api::Responses
        } else {
            Api::Anthropic
        };
        assert!(decode(api, &fixture).is_err());
    }
}

#[test]
fn native_usage_is_retained() {
    let openai = decode(Api::Responses, OPENAI).unwrap();
    let anthropic = decode(Api::Anthropic, ANTHROPIC).unwrap();
    let openai_extras = &openai.generation.unwrap().extras;
    let anthropic_extras = &anthropic.generation.unwrap().extras;
    assert_eq!(
        openai_extras["native_usage"]["input_tokens_details"]["cache_write_tokens_details"]
            ["ttl_30m_tokens"],
        json!(0)
    );
    assert_eq!(
        anthropic_extras["native_usage_start"]["cache_creation"]["ephemeral_5m_input_tokens"],
        json!(0)
    );
    assert_eq!(
        anthropic_extras["native_usage_delta"]["output_tokens"],
        json!(3)
    );
    let _: &Value = &openai_extras["native_usage"];
}

#[test]
fn each_stream_layer_rejects_semantic_details() {
    for (from, to) in [
        (
            "\"delta\":\"hello\",\"logprobs\":[]",
            "\"delta\":\"hello\",\"logprobs\":[1]",
        ),
        (
            "\"text\":\"hello\",\"logprobs\":[]",
            "\"text\":\"hello\",\"logprobs\":[1]",
        ),
        (
            "\"text\":\"\",\"annotations\":[]",
            "\"text\":\"\",\"annotations\":[1]",
        ),
        ("\"phase\":\"final_answer\"", "\"phase\":\"future_phase\""),
        ("\"ttl_30m_tokens\":0", "\"ttl_30m_tokens\":\"invalid\""),
    ] {
        assert!(
            decode(Api::Responses, &OPENAI.replacen(from, to, 1)).is_err(),
            "{from}"
        );
    }
    assert!(decode(
        Api::Anthropic,
        &ANTHROPIC.replacen("\"diagnostics\":null", "\"diagnostics\":{}", 1)
    )
    .is_err());
}

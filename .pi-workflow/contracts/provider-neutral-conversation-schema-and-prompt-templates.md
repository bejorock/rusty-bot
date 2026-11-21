---
id: provider-neutral-conversation-schema-and-prompt-templates
type: interface
status: proposed
producer: main
consumer: SDK clients and session adapters
version: 1
created: 2026-11-21T09:47:02.569Z
updated: 2026-11-21T09:47:09.598Z
---

# Provider-neutral conversation schema and templates

## Scope
Add types in rusty-bot-session, re-export from Temporal engine. No provider-specific message translation, tool execution, network/media loading, or context compaction. Keep existing text API and stored JSON readable with serde defaults.

## Conversation
ChatHistory is versioned and contains ordered HistoryEntry records with stable caller-supplied IDs, optional turn ID, typed ConversationMessage, Generating/Completed/Failed status, optional failure, and arbitrary JSON metadata. ConversationMessage roles: system, developer, user, assistant (including tool calls), tool result (call ID/name/error). ContentBlock variants: text, image, audio, video, file, JSON, reasoning. MediaSource holds MIME type and inline base64, URL, or application-managed reference; no implicit fetching/decoding. Validation rejects blank identity/source fields, malformed role/body/tool-call structures, duplicate IDs/call IDs and mismatched/duplicate tool results. Complete history additionally rejects outstanding tool calls. Media/provider capabilities remain deferred.

## Storage and SDK
SessionConfig gains serde-defaulted initial_history; adapters reject incompatible config as before. StoredSession::history() returns initial history plus deterministic IDs for existing text turns and exposes failure details; the legacy snapshot API remains unchanged. SDK start_session_with_history stores immutable validated initial history; Session::history reads adapter state without a Workflow worker. Existing model path explicitly rejects initial rich histories until provider translation is implemented, never silently discards them. The legacy empty-history text path remains unchanged. General schema can describe tool/multimodal history without executing it.

## Templates
Pure serializable PromptTemplate supports {identifier}, escaped braces {{ and }}, strict syntax/missing/unknown-variable errors, immutable partial bindings/overrides, and nonrecursive substitution. ChatPromptTemplate composes role text templates and literal typed messages, allowing reusable multimodal examples. No expression evaluation/loops/dependency on LangChain. Templates render to schema messages at application boundary; store rendered output not mutable template references.

## Validation
Unit serialization/validation/template tests, adapter JSON roundtrip/backward fixture compatibility and SDK tests prove history retrieval and explicit refusal before model generation. Preserve existing text lifecycle regression suite. No unrequested live provider calls.
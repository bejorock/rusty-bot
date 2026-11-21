---
id: lossless-conversation-blocks-and-streaming-amendments
type: data
status: proposed
producer: assistant
consumer: session SDK and provider integrations
version: 1
created: 2026-11-21T10:10:07.502Z
updated: 2026-11-21T10:10:11.622Z
---

# Bounded message contract amendments

- Canonical assistant content is one ordered ContentBlock list, including ToolCall. No second authoritative tool_calls array. Deserialize prior assistant JSON with tool_calls by appending calls after legacy content (the only ordering prior documents represented); reject mixing both representations. Serialize canonical shape only. Text-session host APIs unchanged.
- Add refusal, namespaced extension, provider-scoped versioned replay state, invalid tool calls and annotated blocks (stable block ID, annotations, replay state). Replay compatibility is explicit provider/model/format/version; required state cannot be silently projected away. Debug redacts opaque replay payloads.
- HistoryEntry optional generation metadata, creation timestamp, and non-model artifact; defaults preserve old JSON. Generation metadata includes finish reason/raw reason, usage, provider/model/response identity. Message status remains persistence lifecycle, not finish reason.
- Separate serializable MessageStreamEvent and pure accumulator enforce message identity/sequence, block start/delta/end, final authority and failure. Tool argument deltas are raw text; malformed final arguments retained as InvalidToolCall, never executable calls. No transport/tool execution implementation.
- Add optional canonical response to StoredTurn plus atomic idempotent FinishResponse storage transition. Existing Finish strings and legacy snapshot work unchanged; history prefers canonical response and does not reconstruct it from text. Rich initial history and responses remain provider-neutral, translation unsupported. No automatic provider conversion or new tool execution.
- Existing text-only model Activity remains explicitly legacy until provider integration; do not claim it captures full provider replay metadata. SDK docs distinguish new schema/storage capabilities from current text generation path.
- Validation: JSON round trips and old-document reads, order preservation, replay compatibility and redacted Debug, malformed/partial calls, stream boundary failures, immutable canonical finish, legacy text projection refusal rather than loss, strict workspace checks. No live service access needed.
-- Rusty Bot M2 PostgreSQL schema. Normative source:
-- .pi-workflow/contracts/veronica-postgresql-storage-contract.md
--
-- IDs are BIGINT and JSON payloads are JSONB. Audit columns use TIMESTAMPTZ;
-- records carry epoch-ms timestamps for deterministic application tests.

CREATE TABLE rusty_bot_meta (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  next_id   BIGINT NOT NULL,
  next_seq  BIGINT NOT NULL
);
INSERT INTO rusty_bot_meta (singleton, next_id, next_seq) VALUES (TRUE, 1, 1);

CREATE TABLE commits (
  seq          BIGINT PRIMARY KEY,
  session_id   UUID NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  batch_count  INT NOT NULL
);
CREATE INDEX commits_by_session ON commits (session_id, seq);

CREATE TABLE conversations (
  id BIGINT PRIMARY KEY,
  parent_conversation_id BIGINT,
  parent_at BIGINT,
  owner_conversation_id BIGINT,
  owner_task_id BIGINT,
  -- `StoredConversation` carries the commit sequence; the trait shape needs
  -- this column even though the proposed schema sketch omitted it.
  commit_seq BIGINT NOT NULL REFERENCES commits(seq),
  record JSONB NOT NULL
);

CREATE TABLE entries (
  id BIGINT PRIMARY KEY,
  conversation_id BIGINT NOT NULL,
  head BIGINT,
  kind TEXT NOT NULL,
  commit_seq BIGINT NOT NULL REFERENCES commits(seq),
  record JSONB NOT NULL
);
CREATE INDEX entries_by_conversation ON entries (conversation_id, id DESC);
CREATE INDEX entries_heads ON entries (conversation_id, id DESC) WHERE head IS NOT NULL;
CREATE INDEX entries_by_task ON entries (conversation_id, id DESC)
  WHERE (record->>'by_task_id') IS NOT NULL;

CREATE TABLE tasks (
  id BIGINT PRIMARY KEY,
  conversation_id BIGINT NOT NULL,
  kind TEXT NOT NULL,
  version INT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending','running','waiting','sleeping','completing','terminal')),
  abort_requested BOOLEAN NOT NULL DEFAULT FALSE,
  background BOOLEAN NOT NULL DEFAULT FALSE,
  started_at BIGINT,
  ended_at BIGINT,
  commit_seq BIGINT NOT NULL REFERENCES commits(seq),
  record JSONB NOT NULL
);
CREATE INDEX tasks_by_status ON tasks (status, id);
CREATE INDEX tasks_by_conversation ON tasks (conversation_id, id);
CREATE INDEX tasks_by_kind ON tasks (kind, id);
CREATE INDEX tasks_by_abort ON tasks (abort_requested, id) WHERE abort_requested;
CREATE INDEX tasks_by_background ON tasks (background, id) WHERE background;

CREATE TABLE submissions (
  id BIGINT PRIMARY KEY,
  conversation_id BIGINT NOT NULL,
  request_id TEXT,
  kind TEXT NOT NULL CHECK (kind IN ('input','write','continue')),
  status TEXT NOT NULL CHECK (status IN ('queued','placed','done','unanswered')),
  commit_seq BIGINT NOT NULL REFERENCES commits(seq),
  record JSONB NOT NULL
);
CREATE UNIQUE INDEX submissions_request_unique
  ON submissions (conversation_id, request_id) WHERE request_id IS NOT NULL;
CREATE INDEX submissions_by_conversation ON submissions (conversation_id, id);
CREATE INDEX submissions_by_status ON submissions (status, id);

CREATE TABLE documents (
  id BIGINT PRIMARY KEY,
  kind TEXT NOT NULL,
  family BOOLEAN NOT NULL DEFAULT FALSE,
  key_value TEXT NOT NULL DEFAULT '',
  scope_kind TEXT NOT NULL CHECK (scope_kind IN ('session','conversation','task')),
  owner_id BIGINT NOT NULL,
  created_at BIGINT NOT NULL,
  retired_at BIGINT,
  record JSONB NOT NULL
);
CREATE INDEX documents_by_address
  ON documents (kind, scope_kind, owner_id, family, key_value, created_at DESC, retired_at);
CREATE INDEX documents_by_scope ON documents (scope_kind, owner_id, id);
CREATE UNIQUE INDEX documents_latest_unique
  ON documents (kind, scope_kind, owner_id, key_value)
  WHERE retired_at IS NULL AND (record->>'history') = 'latest';

CREATE TABLE document_revisions (
  document_id BIGINT NOT NULL REFERENCES documents(id),
  seq BIGINT NOT NULL REFERENCES commits(seq),
  kind TEXT NOT NULL CHECK (kind IN ('base','delta')),
  version INT NOT NULL,
  content JSONB NOT NULL,
  PRIMARY KEY (document_id, seq)
);

CREATE TABLE tool_operations (
  task_id BIGINT NOT NULL,
  idempotency_key TEXT NOT NULL,
  tool_name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('intent','running','succeeded','failed','interrupted','uncertain')),
  request JSONB NOT NULL,
  result JSONB,
  updated_at BIGINT NOT NULL,
  PRIMARY KEY (task_id, idempotency_key)
);

CREATE TABLE session_events (
  seq BIGINT NOT NULL REFERENCES commits(seq),
  ordinal INT NOT NULL,
  session_id UUID NOT NULL,
  conversation_id BIGINT,
  kind TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (seq, ordinal)
);
CREATE INDEX session_events_by_session ON session_events (session_id, seq, ordinal);

CREATE TABLE sessions (
  session_id UUID PRIMARY KEY,
  workspace_key TEXT NOT NULL,
  workspace JSONB NOT NULL,
  config JSONB NOT NULL,
  registry_version TEXT NOT NULL,
  extension_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
  host_id TEXT NOT NULL,
  temporal_workflow_id TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX sessions_by_workspace ON sessions (workspace_key, session_id);

CREATE TABLE workspace_bindings (
  workspace_key TEXT PRIMARY KEY,
  execution_env_id TEXT,
  root TEXT NOT NULL,
  host_id TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

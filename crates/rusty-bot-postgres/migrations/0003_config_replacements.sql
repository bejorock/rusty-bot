-- Durable deduplication survives activity retry and workflow continue-as-new.
CREATE TABLE session_config_replacements (
    session_id UUID NOT NULL REFERENCES sessions(session_id),
    request_id TEXT NOT NULL,
    request JSONB NOT NULL,
    result JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, request_id)
);

-- Explicitly associate sessions with their root conversation. Legacy rows are
-- backfilled only when exactly one opening event exists; ambiguous crash-window
-- histories remain NULL and are refused by the worker until reconciled.
ALTER TABLE sessions ADD COLUMN conversation_id BIGINT REFERENCES conversations(id);
UPDATE sessions s SET conversation_id = candidate.conversation_id
FROM (
    SELECT session_id, min(conversation_id) AS conversation_id
    FROM session_events
    WHERE kind = 'session.opened' AND conversation_id IS NOT NULL
    GROUP BY session_id HAVING count(DISTINCT conversation_id) = 1
) candidate
WHERE candidate.session_id = s.session_id;

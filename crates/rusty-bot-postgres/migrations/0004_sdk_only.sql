-- Retire host-specific metadata and configuration-replacement receipts.
-- Earlier migrations stay unchanged so existing SQLx migration checksums remain valid.
ALTER TABLE sessions DROP COLUMN temporal_workflow_id;
DROP TABLE session_config_replacements;

ALTER TABLE messages ADD COLUMN expires_at INTEGER NOT NULL DEFAULT 0;
-- v1/v2 have no receive time. Conservatively start their replay grace now.
UPDATE messages SET expires_at=max(unixepoch('now'),CAST(strftime('%s',issued_at) AS INTEGER))+604800;
CREATE TABLE retention_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1),message_floor INTEGER NOT NULL) STRICT;
INSERT INTO retention_state VALUES(1,0);
CREATE INDEX tasks_retention ON tasks(state,CAST(strftime('%s',updated_at) AS INTEGER),id);
CREATE INDEX messages_expiration ON messages(expires_at,id);
CREATE INDEX verification_evidence_operation ON verification_evidence(operation_id);

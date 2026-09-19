CREATE TABLE revocations (
 attempt_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL, evidence_id TEXT NOT NULL,
 reason TEXT NOT NULL CHECK(reason IN ('controller_restart','lease_expired','ownership_lost')),
 revoked_at TEXT NOT NULL,
 FOREIGN KEY(operation_id,attempt_id) REFERENCES attempts(operation_id,id),
 FOREIGN KEY(operation_id,attempt_id,evidence_id) REFERENCES evidence(operation_id,attempt_id,id)
) STRICT;
CREATE INDEX revocations_operation ON revocations(operation_id);

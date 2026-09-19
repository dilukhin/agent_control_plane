CREATE TABLE tasks (
 id TEXT PRIMARY KEY CHECK(id<>''), state TEXT NOT NULL CHECK(state IN ('planned','active','blocked','awaiting_decision','completed','failed','cancelled')),
 revision INTEGER NOT NULL CHECK(revision>0), created_at TEXT NOT NULL, updated_at TEXT NOT NULL
) STRICT;
CREATE TABLE operations (
 id TEXT PRIMARY KEY CHECK(id<>''), task_id TEXT NOT NULL REFERENCES tasks(id), name TEXT NOT NULL, target_ref TEXT NOT NULL,
 effect_class TEXT NOT NULL CHECK(effect_class IN ('read_only','mutation')),
 idempotency_mode TEXT NOT NULL CHECK(idempotency_mode IN ('unknown','idempotent','conditional')),
 verification_policy_ref TEXT NOT NULL, authorization_policy_ref TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('planned','ready','executing','verifying','unknown_outcome','succeeded','failed','cancelled')),
 revision INTEGER NOT NULL CHECK(revision>0), lease_generation INTEGER NOT NULL CHECK(lease_generation>=0),
 active_attempt_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(id,task_id),
 CHECK((state IN ('executing','verifying','unknown_outcome') AND active_attempt_id IS NOT NULL AND lease_generation>0) OR (state NOT IN ('executing','verifying','unknown_outcome') AND active_attempt_id IS NULL)),
 FOREIGN KEY(id,active_attempt_id,lease_generation) REFERENCES attempts(operation_id,id,lease_generation) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE INDEX operations_task ON operations(task_id);
CREATE INDEX operations_state ON operations(state,id);
CREATE TABLE operation_scopes (
 operation_id TEXT NOT NULL REFERENCES operations(id), scope TEXT NOT NULL CHECK(scope<>''), ordinal INTEGER NOT NULL CHECK(ordinal>=0),
 PRIMARY KEY(operation_id,scope), UNIQUE(operation_id,ordinal)
) STRICT;
CREATE TABLE attempts (
 id TEXT PRIMARY KEY CHECK(id<>''), operation_id TEXT NOT NULL REFERENCES operations(id), lease_id TEXT NOT NULL UNIQUE CHECK(lease_id<>''),
 lease_generation INTEGER NOT NULL CHECK(lease_generation>0), owner_actor_id TEXT NOT NULL CHECK(owner_actor_id<>''), retry_of TEXT,
 state TEXT NOT NULL CHECK(state IN ('created','running','reported','rejected','not_started','uncertain','closed')),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(operation_id,id), UNIQUE(operation_id,lease_generation), UNIQUE(operation_id,id,lease_generation), UNIQUE(operation_id,id,lease_id,lease_generation),
 FOREIGN KEY(operation_id,retry_of) REFERENCES attempts(operation_id,id)
) STRICT;
CREATE TABLE reservations (
 scope TEXT PRIMARY KEY, operation_id TEXT NOT NULL, attempt_id TEXT NOT NULL, lease_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0), created_at TEXT NOT NULL,
 FOREIGN KEY(operation_id,scope) REFERENCES operation_scopes(operation_id,scope),
 FOREIGN KEY(operation_id,attempt_id,lease_id,generation) REFERENCES attempts(operation_id,id,lease_id,lease_generation)
) STRICT;
CREATE INDEX reservations_operation ON reservations(operation_id);
CREATE TABLE evidence (
 id TEXT PRIMARY KEY CHECK(id<>''), task_id TEXT NOT NULL, operation_id TEXT NOT NULL, attempt_id TEXT,
 kind TEXT NOT NULL CHECK(kind IN ('delivery_receipt','worker_report','provider_report','transport_observation','target_observation','process_observation','artifact_observation','verification_result','user_decision','policy_decision')),
 source_actor_id TEXT NOT NULL CHECK(source_actor_id<>''), observed_at TEXT NOT NULL, subject_ref TEXT NOT NULL, summary TEXT NOT NULL, artifact_ref TEXT NOT NULL, digest TEXT NOT NULL,
 UNIQUE(operation_id,attempt_id,id), FOREIGN KEY(operation_id,task_id) REFERENCES operations(id,task_id),
 FOREIGN KEY(operation_id,attempt_id) REFERENCES attempts(operation_id,id)
) STRICT;
CREATE INDEX evidence_operation ON evidence(operation_id,id);
CREATE TABLE verifications (
 id TEXT PRIMARY KEY CHECK(id<>''), operation_id TEXT NOT NULL, attempt_id TEXT NOT NULL, policy_ref TEXT NOT NULL,
 verdict TEXT NOT NULL CHECK(verdict IN ('satisfied','not_satisfied_retryable','not_satisfied_terminal','inconclusive')),
 retry_execution_safe INTEGER NOT NULL CHECK(retry_execution_safe IN (0,1)), actual_state_summary TEXT NOT NULL, verified_at TEXT NOT NULL, verifier_actor_id TEXT NOT NULL,
 CHECK(verdict<>'not_satisfied_retryable' OR retry_execution_safe=1),
 UNIQUE(id,operation_id,attempt_id), FOREIGN KEY(operation_id,attempt_id) REFERENCES attempts(operation_id,id)
) STRICT;
CREATE INDEX verifications_operation ON verifications(operation_id,id);
CREATE TABLE verification_evidence (
 verification_id TEXT NOT NULL, operation_id TEXT NOT NULL, attempt_id TEXT NOT NULL, evidence_id TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>=0),
 PRIMARY KEY(verification_id,evidence_id), UNIQUE(verification_id,ordinal),
 FOREIGN KEY(verification_id,operation_id,attempt_id) REFERENCES verifications(id,operation_id,attempt_id),
 FOREIGN KEY(operation_id,attempt_id,evidence_id) REFERENCES evidence(operation_id,attempt_id,id)
) STRICT;
CREATE INDEX verification_evidence_record ON verification_evidence(evidence_id);
CREATE TABLE messages (
 id TEXT PRIMARY KEY CHECK(id<>''), task_id TEXT NOT NULL, operation_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
 digest TEXT NOT NULL CHECK(length(digest)=67 AND substr(digest,1,3)='v1:'), issued_at TEXT NOT NULL
) STRICT;
CREATE INDEX messages_task ON messages(task_id);

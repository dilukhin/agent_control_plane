package sqlite

import (
	"errors"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/evidence"
	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

type scanner interface{ Scan(...any) error }

const taskColumns = "id,state,revision,created_at,updated_at"

func scanTask(row scanner) (v state.Task, err error) {
	var createdAt string
	var updatedAt string
	err = dbError(row.Scan(&v.ID, &v.State, &v.Revision, &createdAt, &updatedAt))
	if err != nil {
		return v, err
	}
	v.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return v, p.ErrIntegrity
	}
	v.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return v, p.ErrIntegrity
	}
	return v, nil
}
func (t *transaction) Task(id protocol.TaskID) (state.Task, error) {
	v, err := scanTask(t.conn.QueryRowContext(t.ctx, "SELECT id,state,revision,created_at,updated_at FROM tasks WHERE id=?", id))
	if err != nil {
		return v, err
	}
	return v, err
}
func (t *transaction) InsertTask(v state.Task) error {
	_, err := t.exec("INSERT INTO tasks ("+taskColumns+") VALUES(?,?,?,?,?)", v.ID, v.State, v.Revision, timestamp(v.CreatedAt), timestamp(v.UpdatedAt))
	if err != nil {
		return err
	}
	return nil
}

const operationColumns = "id,task_id,name,target_ref,effect_class,idempotency_mode,verification_policy_ref,authorization_policy_ref,state,revision,lease_generation,active_attempt_id,created_at,updated_at"

func scanOperation(row scanner) (v state.Operation, err error) {
	var createdAt string
	var updatedAt string
	err = dbError(row.Scan(&v.Descriptor.ID, &v.Descriptor.TaskID, &v.Descriptor.Name, &v.Descriptor.TargetRef, &v.Descriptor.EffectClass, &v.Descriptor.IdempotencyMode, &v.Descriptor.VerificationPolicyRef, &v.Descriptor.AuthorizationPolicyRef, &v.State, &v.Revision, &v.LeaseGeneration, &v.ActiveAttemptID, &createdAt, &updatedAt))
	if err != nil {
		return v, err
	}
	v.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return v, p.ErrIntegrity
	}
	v.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return v, p.ErrIntegrity
	}
	return v, nil
}
func (t *transaction) Operation(id protocol.OperationID) (state.Operation, error) {
	v, err := scanOperation(t.conn.QueryRowContext(t.ctx, "SELECT id,task_id,name,target_ref,effect_class,idempotency_mode,verification_policy_ref,authorization_policy_ref,state,revision,lease_generation,COALESCE(active_attempt_id,''),created_at,updated_at FROM operations WHERE id=?", id))
	if err != nil {
		return v, err
	}
	v.Descriptor.ConflictScope, err = t.scopes(id)
	return v, err
}
func (t *transaction) InsertOperation(v state.Operation) error {
	_, err := t.exec("INSERT INTO operations ("+operationColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", v.Descriptor.ID, v.Descriptor.TaskID, v.Descriptor.Name, v.Descriptor.TargetRef, v.Descriptor.EffectClass, v.Descriptor.IdempotencyMode, v.Descriptor.VerificationPolicyRef, v.Descriptor.AuthorizationPolicyRef, v.State, v.Revision, v.LeaseGeneration, nullable(v.ActiveAttemptID), timestamp(v.CreatedAt), timestamp(v.UpdatedAt))
	if err != nil {
		return err
	}
	for i, scope := range v.Descriptor.ConflictScope {
		if _, err = t.exec("INSERT INTO operation_scopes VALUES(?,?,?)", v.Descriptor.ID, scope, i); err != nil {
			return err
		}
	}
	return nil
}

const attemptColumns = "id,operation_id,lease_id,lease_generation,owner_actor_id,retry_of,state,created_at,updated_at"

func scanAttempt(row scanner) (v state.Attempt, err error) {
	var createdAt string
	var updatedAt string
	err = dbError(row.Scan(&v.ID, &v.OperationID, &v.LeaseID, &v.LeaseGeneration, &v.OwnerActorID, &v.RetryOf, &v.State, &createdAt, &updatedAt))
	if err != nil {
		return v, err
	}
	v.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return v, p.ErrIntegrity
	}
	v.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return v, p.ErrIntegrity
	}
	return v, nil
}
func (t *transaction) Attempt(id protocol.AttemptID) (state.Attempt, error) {
	v, err := scanAttempt(t.conn.QueryRowContext(t.ctx, "SELECT id,operation_id,lease_id,lease_generation,owner_actor_id,COALESCE(retry_of,''),state,created_at,updated_at FROM attempts WHERE id=?", id))
	if err != nil {
		return v, err
	}
	return v, err
}
func (t *transaction) InsertAttempt(v state.Attempt) error {
	_, err := t.exec("INSERT INTO attempts ("+attemptColumns+") VALUES(?,?,?,?,?,?,?,?,?)", v.ID, v.OperationID, v.LeaseID, v.LeaseGeneration, v.OwnerActorID, nullable(v.RetryOf), v.State, timestamp(v.CreatedAt), timestamp(v.UpdatedAt))
	if err != nil {
		return err
	}
	return nil
}

const evidenceColumns = "id,task_id,operation_id,attempt_id,kind,source_actor_id,observed_at,subject_ref,summary,artifact_ref,digest"

func scanEvidence(row scanner) (v evidence.Record, err error) {
	var observedAt string
	err = dbError(row.Scan(&v.ID, &v.TaskID, &v.OperationID, &v.AttemptID, &v.Kind, &v.SourceActorID, &observedAt, &v.SubjectRef, &v.Summary, &v.ArtifactRef, &v.Digest))
	if err != nil {
		return v, err
	}
	v.ObservedAt, err = time.Parse(time.RFC3339Nano, observedAt)
	if err != nil {
		return v, p.ErrIntegrity
	}
	return v, nil
}
func (t *transaction) Evidence(id protocol.EvidenceID) (evidence.Record, error) {
	v, err := scanEvidence(t.conn.QueryRowContext(t.ctx, "SELECT id,task_id,operation_id,COALESCE(attempt_id,''),kind,source_actor_id,observed_at,subject_ref,summary,artifact_ref,digest FROM evidence WHERE id=?", id))
	if err != nil {
		return v, err
	}
	return v, err
}
func (t *transaction) InsertEvidence(v evidence.Record) error {
	_, err := t.exec("INSERT INTO evidence ("+evidenceColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?)", v.ID, v.TaskID, v.OperationID, nullable(v.AttemptID), v.Kind, v.SourceActorID, timestamp(v.ObservedAt), v.SubjectRef, v.Summary, v.ArtifactRef, v.Digest)
	if err != nil {
		return err
	}
	return nil
}

const verificationColumns = "id,operation_id,attempt_id,policy_ref,verdict,retry_execution_safe,actual_state_summary,verified_at,verifier_actor_id"

func scanVerification(row scanner) (v evidence.Verification, err error) {
	var verifiedAt string
	err = dbError(row.Scan(&v.ID, &v.OperationID, &v.AttemptID, &v.PolicyRef, &v.Verdict, &v.RetryExecutionSafe, &v.ActualStateSummary, &verifiedAt, &v.VerifierActorID))
	if err != nil {
		return v, err
	}
	v.VerifiedAt, err = time.Parse(time.RFC3339Nano, verifiedAt)
	if err != nil {
		return v, p.ErrIntegrity
	}
	return v, nil
}
func (t *transaction) Verification(id string) (evidence.Verification, error) {
	v, err := scanVerification(t.conn.QueryRowContext(t.ctx, "SELECT id,operation_id,attempt_id,policy_ref,verdict,retry_execution_safe,actual_state_summary,verified_at,verifier_actor_id FROM verifications WHERE id=?", id))
	if err != nil {
		return v, err
	}
	v.EvidenceIDs, err = t.verificationEvidence(id)
	return v, err
}
func (t *transaction) InsertVerification(v evidence.Verification) error {
	_, err := t.exec("INSERT INTO verifications ("+verificationColumns+") VALUES(?,?,?,?,?,?,?,?,?)", v.ID, v.OperationID, v.AttemptID, v.PolicyRef, v.Verdict, v.RetryExecutionSafe, v.ActualStateSummary, timestamp(v.VerifiedAt), v.VerifierActorID)
	if err != nil {
		return err
	}
	for i, id := range v.EvidenceIDs {
		if _, err = t.exec("INSERT INTO verification_evidence VALUES(?,?,?,?,?)", v.ID, v.OperationID, v.AttemptID, id, i); err != nil {
			return err
		}
	}
	return nil
}

const messageColumns = "id,task_id,operation_id,attempt_id,digest,issued_at"

func scanMessage(row scanner) (v p.Message, err error) {
	var issuedAt string
	err = dbError(row.Scan(&v.ID, &v.TaskID, &v.OperationID, &v.AttemptID, &v.Digest, &issuedAt))
	if err != nil {
		return v, err
	}
	v.IssuedAt, err = time.Parse(time.RFC3339Nano, issuedAt)
	if err != nil {
		return v, p.ErrIntegrity
	}
	return v, nil
}
func (t *transaction) Message(id protocol.MessageID) (p.Message, error) {
	v, err := scanMessage(t.conn.QueryRowContext(t.ctx, "SELECT id,task_id,operation_id,attempt_id,digest,issued_at FROM messages WHERE id=?", id))
	if err != nil {
		return v, err
	}
	return v, err
}
func (t *transaction) InsertMessage(v p.Message) error {
	_, err := t.exec("INSERT INTO messages ("+messageColumns+") VALUES(?,?,?,?,?,?)", v.ID, v.TaskID, v.OperationID, v.AttemptID, v.Digest, timestamp(v.IssuedAt))
	if err != nil {
		return err
	}
	return nil
}

const reservationColumns = "scope,operation_id,attempt_id,lease_id,generation,created_at"

func scanReservation(row scanner) (v p.Reservation, err error) {
	var createdAt string
	err = dbError(row.Scan(&v.Scope, &v.OperationID, &v.AttemptID, &v.LeaseID, &v.Generation, &createdAt))
	if err != nil {
		return v, err
	}
	v.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return v, p.ErrIntegrity
	}
	return v, nil
}
func (t *transaction) Reservation(id string) (p.Reservation, error) {
	v, err := scanReservation(t.conn.QueryRowContext(t.ctx, "SELECT scope,operation_id,attempt_id,lease_id,generation,created_at FROM reservations WHERE scope=?", id))
	if err != nil {
		return v, err
	}
	return v, err
}
func (t *transaction) Reserve(v p.Reservation) error {
	_, err := t.exec("INSERT INTO reservations ("+reservationColumns+") VALUES(?,?,?,?,?,?)", v.Scope, v.OperationID, v.AttemptID, v.LeaseID, v.Generation, timestamp(v.CreatedAt))
	if errors.Is(err, p.ErrDuplicateID) {
		return p.ErrConflictScope
	}
	if err != nil {
		return err
	}
	return nil
}

func (t *transaction) scopes(id protocol.OperationID) ([]string, error) {
	rows, err := t.conn.QueryContext(t.ctx, "SELECT scope FROM operation_scopes WHERE operation_id=? ORDER BY ordinal", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err = rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (t *transaction) verificationEvidence(id string) ([]protocol.EvidenceID, error) {
	rows, err := t.conn.QueryContext(t.ctx, "SELECT evidence_id FROM verification_evidence WHERE verification_id=? ORDER BY ordinal", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []protocol.EvidenceID
	for rows.Next() {
		var v protocol.EvidenceID
		if err = rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (t *transaction) EvidenceForOperation(id protocol.OperationID) ([]evidence.Record, error) {
	rows, err := t.conn.QueryContext(t.ctx, "SELECT id,task_id,operation_id,COALESCE(attempt_id,''),kind,source_actor_id,observed_at,subject_ref,summary,artifact_ref,digest FROM evidence WHERE operation_id=? ORDER BY id", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []evidence.Record{}
	for rows.Next() {
		v, e := scanEvidence(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (t *transaction) UpdateOperation(v state.Operation, expected uint64) error {
	if v.Revision != expected+1 {
		return p.ErrIntegrity
	}
	result, err := t.exec("UPDATE operations SET state=?,revision=?,lease_generation=?,active_attempt_id=?,updated_at=? WHERE id=? AND revision=?", v.State, v.Revision, v.LeaseGeneration, nullable(v.ActiveAttemptID), timestamp(v.UpdatedAt), v.Descriptor.ID, expected)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return state.ErrRevisionConflict
	}
	return nil
}
func (t *transaction) UpdateAttempt(v state.Attempt) error {
	result, err := t.exec("UPDATE attempts SET state=?,updated_at=? WHERE id=? AND operation_id=? AND lease_id=? AND lease_generation=? AND owner_actor_id=?", v.State, timestamp(v.UpdatedAt), v.ID, v.OperationID, v.LeaseID, v.LeaseGeneration, v.OwnerActorID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return p.ErrIntegrity
	}
	return nil
}
func (t *transaction) Release(id protocol.OperationID) error {
	_, err := t.exec("DELETE FROM reservations WHERE operation_id=?", id)
	return err
}

var _ p.Repository = (*Repository)(nil)
var _ p.Tx = (*transaction)(nil)

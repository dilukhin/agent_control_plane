package core

import (
	"context"
	"errors"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/evidence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

// RecordWorkerReport persists a claim, including a late claim from a revoked
// owner, but never verifies it. Call inside ProcessMessage. The authenticated
// adapter fixes the actor and record kind; worker input cannot choose them.
// A stale claim is retained without changing canonical state.
func (t *Transaction) RecordWorkerReport(record evidence.Record, lease protocol.LeaseID, generation uint64, expires time.Time) (applied bool, err error) {
	if t.failed != nil {
		return false, t.failed
	}
	defer func() {
		if err != nil {
			t.failed = err
		}
	}()
	op, err := t.tx.Operation(record.OperationID)
	if err != nil {
		return false, err
	}
	a, err := t.tx.Attempt(record.AttemptID)
	if err != nil {
		return false, err
	}
	if expires.IsZero() || record.Kind != evidence.KindWorkerReport || a.OperationID != record.OperationID || a.OwnerActorID != record.SourceActorID || a.LeaseID != lease || a.LeaseGeneration != generation || record.SubjectRef != op.Descriptor.TargetRef {
		return false, ErrEvidenceScope
	}
	if err = t.RegisterEvidence(record); err != nil {
		return false, err
	}
	err = checkActive(t.tx, record.OperationID, a.ID, lease, generation, record.SourceActorID)
	if errors.Is(err, ErrStaleOwnership) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !t.now.Before(expires) {
		return false, context.DeadlineExceeded
	}
	if op.State != state.OperationExecuting {
		return false, nil
	}
	_, err = t.BeginVerification(record.OperationID, op.Revision, t.now)
	return err == nil, err
}

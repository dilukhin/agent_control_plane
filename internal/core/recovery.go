package core

import (
	"context"
	"errors"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/evidence"
	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

type RecoveryClass string

const (
	RecoveryPending   RecoveryClass = "pending"
	RecoveryReady     RecoveryClass = "ready"
	RecoveryEvaluate  RecoveryClass = "evaluate_execution"
	RecoveryUnknown   RecoveryClass = "unknown_outcome"
	RecoveryVerified  RecoveryClass = "verified_completion"
	RecoveryFailed    RecoveryClass = "known_failure"
	RecoveryCancelled RecoveryClass = "cancelled"
)

type RecoveryItem struct {
	Operation  state.Operation
	Task       state.Task
	Attempt    *state.Attempt
	Revocation *p.Revocation
	Class      RecoveryClass
}
type RecoveryPage struct {
	Items []RecoveryItem
	Next  protocol.OperationID
}

// InspectRecovery reads one consistent page; it never dispatches work or changes
// state. Pages across calls are not one snapshot: rescan after concurrent inserts.
func (s *Store) InspectRecovery(ctx context.Context, after protocol.OperationID, limit int) (RecoveryPage, error) {
	return readValue(s, ctx, func(r p.Reader) (RecoveryPage, error) {
		ops, err := r.OperationsAfter(after, limit)
		if err != nil {
			return RecoveryPage{}, err
		}
		page := RecoveryPage{Items: make([]RecoveryItem, 0, len(ops))}
		for _, op := range ops {
			item := RecoveryItem{Operation: op}
			item.Task, err = r.Task(op.Descriptor.TaskID)
			if err != nil {
				return RecoveryPage{}, err
			}
			switch op.State {
			case state.OperationPlanned:
				item.Class = RecoveryPending
			case state.OperationReady:
				item.Class = RecoveryReady
			case state.OperationExecuting, state.OperationVerifying:
				item.Class = RecoveryEvaluate
			case state.OperationUnknownOutcome:
				item.Class = RecoveryUnknown
			case state.OperationSucceeded:
				item.Class = RecoveryVerified
			case state.OperationFailed:
				item.Class = RecoveryFailed
			case state.OperationCancelled:
				item.Class = RecoveryCancelled
			default:
				return RecoveryPage{}, p.ErrIntegrity
			}
			if op.ActiveAttemptID != "" {
				a, e := r.Attempt(op.ActiveAttemptID)
				if e != nil {
					return RecoveryPage{}, e
				}
				if a.OperationID != op.Descriptor.ID || a.LeaseGeneration != op.LeaseGeneration {
					return RecoveryPage{}, p.ErrIntegrity
				}
				item.Attempt = &a
				rev, e := r.Revocation(a.ID)
				if e == nil {
					item.Revocation = &rev
				} else if !errors.Is(e, ErrNotFound) {
					return RecoveryPage{}, e
				}
				if op.HoldsConflictReservation() {
					for _, scope := range op.Descriptor.ConflictScope {
						v, e := r.Reservation(scope)
						if e != nil {
							return RecoveryPage{}, e
						}
						if v.OperationID != a.OperationID || v.AttemptID != a.ID || v.LeaseID != a.LeaseID || v.Generation != a.LeaseGeneration {
							return RecoveryPage{}, p.ErrIntegrity
						}
					}
				}
			}
			page.Items = append(page.Items, item)
		}
		if len(ops) == limit {
			page.Next = ops[len(ops)-1].Descriptor.ID
		}
		return page, nil
	})
}

type LossReason string

const (
	ControllerRestart LossReason = "controller_restart"
	LeaseExpired      LossReason = "lease_expired"
	OwnershipLost     LossReason = "ownership_lost"
)

// InvalidateAttempt is a trusted controller decision. The observation describes
// why execution certainty was lost; it is never proof that the worker stopped.
// The caller must establish ownership of the affected workload before invoking
// this after its restart; opening a second controller must not revoke all work.
func (s *Store) InvalidateAttempt(ctx context.Context, id protocol.OperationID, revision uint64, attemptID protocol.AttemptID, reason LossReason, observation evidence.Record, now time.Time) (state.Operation, error) {
	return writeValue(s, ctx, func(t *Transaction) (state.Operation, error) {
		return t.InvalidateAttempt(id, revision, attemptID, reason, observation, now)
	})
}
func (t *Transaction) InvalidateAttempt(id protocol.OperationID, revision uint64, attemptID protocol.AttemptID, reason LossReason, observation evidence.Record, now time.Time) (op state.Operation, err error) {
	if t.failed != nil {
		return op, t.failed
	}
	defer func() {
		if err != nil {
			t.failed = err
		}
	}()
	if reason != ControllerRestart && reason != LeaseExpired && reason != OwnershipLost {
		return op, errors.New("invalid ownership loss reason")
	}
	op, err = t.tx.Operation(id)
	if err != nil {
		return op, err
	}
	if op.Revision != revision {
		return state.Operation{}, state.ErrRevisionConflict
	}
	if attemptID == "" || op.ActiveAttemptID != attemptID {
		return state.Operation{}, ErrStaleOwnership
	}
	if _, e := t.tx.Revocation(attemptID); e == nil {
		return state.Operation{}, ErrStaleOwnership
	} else if !errors.Is(e, ErrNotFound) {
		return state.Operation{}, e
	}
	if observation.OperationID != id || observation.AttemptID != attemptID || (observation.Kind != evidence.KindProcessObservation && observation.Kind != evidence.KindTransportObservation && observation.Kind != evidence.KindPolicyDecision) {
		return state.Operation{}, ErrEvidenceScope
	}
	next, err := state.LoseExecutionCertainty(op, revision, now.UTC())
	if err != nil {
		return state.Operation{}, err
	}
	if err = t.RegisterEvidence(observation); err != nil {
		return state.Operation{}, err
	}
	if err = t.tx.InsertRevocation(p.Revocation{AttemptID: attemptID, OperationID: id, EvidenceID: observation.ID, Reason: string(reason), RevokedAt: now.UTC()}); err != nil {
		return state.Operation{}, err
	}
	a, err := t.tx.Attempt(attemptID)
	if err != nil {
		return state.Operation{}, err
	}
	a.State = state.AttemptUncertain
	a.UpdatedAt = now.UTC()
	if err = t.tx.UpdateAttempt(a); err != nil {
		return state.Operation{}, err
	}
	if err = t.tx.UpdateOperation(next, revision); err != nil {
		return state.Operation{}, err
	}
	return next, nil
}

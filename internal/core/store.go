// Package core owns orchestration decisions independently of storage.
package core

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/evidence"
	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/persistence/memory"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

var (
	ErrDuplicateID        = p.ErrDuplicateID
	ErrNotFound           = p.ErrNotFound
	ErrConflictScope      = p.ErrConflictScope
	ErrEvidenceScope      = errors.New("evidence does not match operation")
	ErrMessageIDCollision = errors.New("message id collision")
	ErrStaleOwnership     = errors.New("stale or invalid execution ownership")
)

type Store struct{ repository p.Repository }

func NewStore() *Store                        { return NewWithRepository(memory.New()) }
func NewWithRepository(r p.Repository) *Store { return &Store{repository: r} }
func (s *Store) Close() error                 { return s.repository.Close() }

// Transaction exposes the same decisions for an atomic message/state update.
// It is valid only during the callback; do not perform external effects here.
type Transaction struct {
	tx     p.Tx
	failed error
}

func writeValue[T any](s *Store, ctx context.Context, fn func(*Transaction) (T, error)) (T, error) {
	var out T
	err := s.repository.Update(ctx, func(tx p.Tx) error {
		t := &Transaction{tx: tx}
		var e error
		out, e = fn(t)
		if e == nil {
			e = t.failed
		}
		return e
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}
func readValue[T any](s *Store, ctx context.Context, fn func(p.Reader) (T, error)) (T, error) {
	var out T
	err := s.repository.View(ctx, func(r p.Reader) error { var e error; out, e = fn(r); return e })
	if err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

// ProcessMessage commits deduplication and the callback together. Duplicate
// delivery skips the callback. Neither callback nor commit errors are retried.
func (s *Store) ProcessMessage(ctx context.Context, env protocol.Envelope, fn func(*Transaction) error) (bool, error) {
	return writeValue(s, ctx, func(t *Transaction) (bool, error) {
		duplicate, err := t.RegisterMessage(env)
		if err != nil || duplicate {
			return duplicate, err
		}
		if fn != nil {
			if err = fn(t); err != nil {
				return false, err
			}
		}
		return false, nil
	})
}
func (t *Transaction) registerMessage(env protocol.Envelope) (bool, error) {
	if err := env.Validate(); err != nil {
		return false, err
	}
	env.IssuedAt = env.IssuedAt.UTC()
	if env.DeadlineAt != nil {
		utc := env.DeadlineAt.UTC()
		env.DeadlineAt = &utc
	}
	encoded, err := json.Marshal(env)
	if err != nil {
		return false, err
	}
	digest := fmt.Sprintf("v1:%x", sha256.Sum256(encoded))
	existing, err := t.tx.Message(env.MessageID)
	if err == nil {
		if existing.Digest == digest {
			return true, nil
		}
		return false, ErrMessageIDCollision
	}
	if !errors.Is(err, ErrNotFound) {
		return false, err
	}
	return false, t.tx.InsertMessage(p.Message{ID: env.MessageID, TaskID: env.TaskID, OperationID: env.OperationID, AttemptID: env.AttemptID, Digest: digest, IssuedAt: env.IssuedAt})
}
func (t *Transaction) createTask(id protocol.TaskID, now time.Time) (state.Task, error) {
	if id == "" || now.IsZero() {
		return state.Task{}, errors.New("task id and time are required")
	}
	v := state.Task{ID: id, State: state.TaskPlanned, Revision: 1, CreatedAt: now.UTC(), UpdatedAt: now.UTC()}
	return v, t.tx.InsertTask(v)
}
func (t *Transaction) createOperation(d protocol.OperationDescriptor, now time.Time) (state.Operation, error) {
	d.ConflictScope = slices.Clone(d.ConflictScope)
	if err := d.Validate(); err != nil {
		return state.Operation{}, err
	}
	if now.IsZero() {
		return state.Operation{}, errors.New("creation time is required")
	}
	if _, err := t.tx.Task(d.TaskID); err != nil {
		return state.Operation{}, err
	}
	v := state.Operation{Descriptor: d, State: state.OperationPlanned, Revision: 1, CreatedAt: now.UTC(), UpdatedAt: now.UTC()}
	return v, t.tx.InsertOperation(v)
}
func (t *Transaction) markReady(id protocol.OperationID, expectedRevision uint64, now time.Time) (state.Operation, error) {
	op, err := t.tx.Operation(id)
	if err != nil {
		return state.Operation{}, err
	}
	// Executing/verifying -> ready requires a recorded retry-safe verification.
	if op.State != state.OperationPlanned {
		return state.Operation{}, state.ErrInvalidTransition
	}
	return t.transition(op, expectedRevision, state.OperationReady, "", now)
}
func (t *Transaction) startAttempt(id protocol.OperationID, expectedRevision uint64, attemptID protocol.AttemptID, leaseID protocol.LeaseID, owner protocol.ActorID, retryOf protocol.AttemptID, now time.Time) (state.Operation, state.Attempt, error) {
	if leaseID == "" || owner == "" {
		return state.Operation{}, state.Attempt{}, errors.New("lease id and owner are required")
	}
	op, err := t.tx.Operation(id)
	if err != nil {
		return state.Operation{}, state.Attempt{}, err
	}
	if retryOf != "" {
		prior, e := t.tx.Attempt(retryOf)
		if e != nil {
			return state.Operation{}, state.Attempt{}, e
		}
		if prior.OperationID != id {
			return state.Operation{}, state.Attempt{}, ErrStaleOwnership
		}
	}
	next, a, err := state.StartAttempt(op, expectedRevision, attemptID, now.UTC())
	if err != nil {
		return state.Operation{}, state.Attempt{}, err
	}
	a.LeaseID = leaseID
	a.OwnerActorID = owner
	a.RetryOf = retryOf
	if err = t.tx.InsertAttempt(a); err != nil {
		return state.Operation{}, state.Attempt{}, err
	}
	if op.Descriptor.EffectClass == protocol.EffectMutation {
		for _, scope := range op.Descriptor.ConflictScope {
			if err = t.tx.Reserve(p.Reservation{Scope: scope, OperationID: id, AttemptID: a.ID, LeaseID: a.LeaseID, Generation: a.LeaseGeneration, CreatedAt: now.UTC()}); err != nil {
				return state.Operation{}, state.Attempt{}, err
			}
		}
	}
	if err = t.tx.UpdateOperation(next, expectedRevision); err != nil {
		return state.Operation{}, state.Attempt{}, err
	}
	return next, a, nil
}
func (t *Transaction) transition(op state.Operation, rev uint64, to state.OperationState, as state.AttemptState, now time.Time) (state.Operation, error) {
	next, err := state.Transition(op, rev, to, now.UTC())
	if err != nil {
		return state.Operation{}, err
	}
	if op.ActiveAttemptID != "" {
		a, e := t.tx.Attempt(op.ActiveAttemptID)
		if e != nil {
			return state.Operation{}, e
		}
		if as != "" {
			a.State = as
		}
		a.UpdatedAt = now.UTC()
		if e = t.tx.UpdateAttempt(a); e != nil {
			return state.Operation{}, e
		}
	}
	if !next.HoldsConflictReservation() {
		if err = t.tx.Release(op.Descriptor.ID); err != nil {
			return state.Operation{}, err
		}
	}
	if err = t.tx.UpdateOperation(next, rev); err != nil {
		return state.Operation{}, err
	}
	return next, nil
}
func (t *Transaction) beginVerification(id protocol.OperationID, expectedRevision uint64, now time.Time) (state.Operation, error) {
	op, err := t.tx.Operation(id)
	if err != nil {
		return state.Operation{}, err
	}
	var as state.AttemptState
	if op.State == state.OperationExecuting {
		as = state.AttemptReported
	}
	return t.transition(op, expectedRevision, state.OperationVerifying, as, now)
}
func (t *Transaction) markUnknownOutcome(id protocol.OperationID, expectedRevision uint64, now time.Time) (state.Operation, error) {
	op, err := t.tx.Operation(id)
	if err != nil {
		return state.Operation{}, err
	}
	return t.transition(op, expectedRevision, state.OperationUnknownOutcome, state.AttemptUncertain, now)
}
func (t *Transaction) registerEvidence(v evidence.Record) error {
	if err := v.Validate(); err != nil {
		return err
	}
	op, err := t.tx.Operation(v.OperationID)
	if err != nil {
		return err
	}
	if op.Descriptor.TaskID != v.TaskID {
		return ErrEvidenceScope
	}
	if v.AttemptID != "" {
		a, e := t.tx.Attempt(v.AttemptID)
		if e != nil {
			return e
		}
		if a.OperationID != v.OperationID {
			return ErrEvidenceScope
		}
	}
	v.ObservedAt = v.ObservedAt.UTC()
	return t.tx.InsertEvidence(v)
}
func (t *Transaction) validateVerification(op state.Operation, v evidence.Verification) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if v.OperationID != op.Descriptor.ID || v.PolicyRef != op.Descriptor.VerificationPolicyRef || v.AttemptID == "" || v.AttemptID != op.ActiveAttemptID {
		return ErrEvidenceScope
	}
	seen := map[protocol.EvidenceID]bool{}
	for _, id := range v.EvidenceIDs {
		if seen[id] {
			return ErrEvidenceScope
		}
		seen[id] = true
		r, e := t.tx.Evidence(id)
		if e != nil {
			return e
		}
		if r.OperationID != v.OperationID || r.AttemptID != v.AttemptID {
			return ErrEvidenceScope
		}
	}
	return nil
}
func (t *Transaction) markNotStarted(id protocol.OperationID, expectedRevision uint64, v evidence.Verification, now time.Time) (state.Operation, error) {
	if v.Verdict != evidence.VerdictNotSatisfiedRetryable || !v.RetryExecutionSafe {
		return state.Operation{}, errors.New("not-started transition requires retry-safe verification")
	}
	return t.apply(id, expectedRevision, v, now, true)
}
func (t *Transaction) applyVerification(id protocol.OperationID, expectedRevision uint64, v evidence.Verification, now time.Time) (state.Operation, error) {
	return t.apply(id, expectedRevision, v, now, false)
}
func (t *Transaction) apply(id protocol.OperationID, rev uint64, v evidence.Verification, now time.Time, notStarted bool) (state.Operation, error) {
	op, err := t.tx.Operation(id)
	if err != nil {
		return state.Operation{}, err
	}
	if err = t.validateVerification(op, v); err != nil {
		return state.Operation{}, err
	}
	to := map[evidence.Verdict]state.OperationState{evidence.VerdictSatisfied: state.OperationSucceeded, evidence.VerdictNotSatisfiedRetryable: state.OperationReady, evidence.VerdictNotSatisfiedTerminal: state.OperationFailed, evidence.VerdictInconclusive: state.OperationUnknownOutcome}[v.Verdict]
	as := state.AttemptClosed
	if to == state.OperationUnknownOutcome {
		as = state.AttemptUncertain
	}
	if notStarted {
		as = state.AttemptNotStarted
	}
	v.VerifiedAt = v.VerifiedAt.UTC()
	if err = t.tx.InsertVerification(v); err != nil {
		return state.Operation{}, err
	}
	return t.transition(op, rev, to, as, now)
}
func checkActive(r p.Reader, id protocol.OperationID, attemptID protocol.AttemptID, leaseID protocol.LeaseID, generation uint64, owner protocol.ActorID) error {
	op, err := r.Operation(id)
	if err != nil {
		return err
	}
	if op.ActiveAttemptID == "" || op.ActiveAttemptID != attemptID || op.LeaseGeneration != generation {
		return ErrStaleOwnership
	}
	a, err := r.Attempt(attemptID)
	if err != nil {
		return err
	}
	if a.LeaseID != leaseID || a.LeaseGeneration != generation || a.OwnerActorID != owner {
		return ErrStaleOwnership
	}
	if op.State != state.OperationExecuting && op.State != state.OperationVerifying && op.State != state.OperationUnknownOutcome {
		return ErrStaleOwnership
	}
	return nil
}
func (s *Store) CheckActiveAttemptContext(ctx context.Context, id protocol.OperationID, attemptID protocol.AttemptID, leaseID protocol.LeaseID, generation uint64, owner protocol.ActorID) error {
	return s.repository.View(ctx, func(r p.Reader) error { return checkActive(r, id, attemptID, leaseID, generation, owner) })
}
func (t *Transaction) checkActiveAttemptContext(id protocol.OperationID, attemptID protocol.AttemptID, leaseID protocol.LeaseID, generation uint64, owner protocol.ActorID) error {
	return checkActive(t.tx, id, attemptID, leaseID, generation, owner)
}
func (s *Store) RegisterMessage(ctx context.Context, env protocol.Envelope) (bool, error) {
	return writeValue(s, ctx, func(t *Transaction) (bool, error) { return t.RegisterMessage(env) })
}
func (s *Store) CreateTask(ctx context.Context, id protocol.TaskID, now time.Time) (state.Task, error) {
	return writeValue(s, ctx, func(t *Transaction) (state.Task, error) { return t.CreateTask(id, now) })
}
func (s *Store) CreateOperation(ctx context.Context, d protocol.OperationDescriptor, now time.Time) (state.Operation, error) {
	return writeValue(s, ctx, func(t *Transaction) (state.Operation, error) { return t.CreateOperation(d, now) })
}
func (s *Store) MarkReady(ctx context.Context, id protocol.OperationID, rev uint64, now time.Time) (state.Operation, error) {
	return writeValue(s, ctx, func(t *Transaction) (state.Operation, error) { return t.MarkReady(id, rev, now) })
}
func (s *Store) BeginVerification(ctx context.Context, id protocol.OperationID, rev uint64, now time.Time) (state.Operation, error) {
	return writeValue(s, ctx, func(t *Transaction) (state.Operation, error) { return t.BeginVerification(id, rev, now) })
}
func (s *Store) MarkUnknownOutcome(ctx context.Context, id protocol.OperationID, rev uint64, now time.Time) (state.Operation, error) {
	return writeValue(s, ctx, func(t *Transaction) (state.Operation, error) { return t.MarkUnknownOutcome(id, rev, now) })
}
func (s *Store) MarkNotStarted(ctx context.Context, id protocol.OperationID, rev uint64, v evidence.Verification, now time.Time) (state.Operation, error) {
	return writeValue(s, ctx, func(t *Transaction) (state.Operation, error) { return t.MarkNotStarted(id, rev, v, now) })
}
func (s *Store) ApplyVerification(ctx context.Context, id protocol.OperationID, rev uint64, v evidence.Verification, now time.Time) (state.Operation, error) {
	return writeValue(s, ctx, func(t *Transaction) (state.Operation, error) { return t.ApplyVerification(id, rev, v, now) })
}
func (s *Store) RegisterEvidence(ctx context.Context, v evidence.Record) error {
	_, err := writeValue(s, ctx, func(t *Transaction) (struct{}, error) { return struct{}{}, t.RegisterEvidence(v) })
	return err
}
func (s *Store) StartAttempt(ctx context.Context, id protocol.OperationID, rev uint64, attemptID protocol.AttemptID, leaseID protocol.LeaseID, owner protocol.ActorID, retryOf protocol.AttemptID, now time.Time) (state.Operation, state.Attempt, error) {
	type result struct {
		op state.Operation
		a  state.Attempt
	}
	v, err := writeValue(s, ctx, func(t *Transaction) (result, error) {
		op, a, e := t.StartAttempt(id, rev, attemptID, leaseID, owner, retryOf, now)
		return result{op, a}, e
	})
	return v.op, v.a, err
}
func (s *Store) GetTask(ctx context.Context, id protocol.TaskID) (state.Task, error) {
	return readValue(s, ctx, func(r p.Reader) (state.Task, error) { return r.Task(id) })
}
func (s *Store) GetOperation(ctx context.Context, id protocol.OperationID) (state.Operation, error) {
	return readValue(s, ctx, func(r p.Reader) (state.Operation, error) { return r.Operation(id) })
}
func (s *Store) GetAttempt(ctx context.Context, id protocol.AttemptID) (state.Attempt, error) {
	return readValue(s, ctx, func(r p.Reader) (state.Attempt, error) { return r.Attempt(id) })
}
func (s *Store) GetVerification(ctx context.Context, id string) (evidence.Verification, error) {
	return readValue(s, ctx, func(r p.Reader) (evidence.Verification, error) { return r.Verification(id) })
}
func (s *Store) EvidenceForOperation(ctx context.Context, id protocol.OperationID) ([]evidence.Record, error) {
	return readValue(s, ctx, func(r p.Reader) ([]evidence.Record, error) { return r.EvidenceForOperation(id) })
}

func (t *Transaction) RegisterMessage(env protocol.Envelope) (v0 bool, err error) {
	if t.failed != nil {
		err = t.failed
		return
	}
	v0, err = t.registerMessage(env)
	if err != nil {
		t.failed = err
	}
	return
}

func (t *Transaction) CreateTask(id protocol.TaskID, now time.Time) (v0 state.Task, err error) {
	if t.failed != nil {
		err = t.failed
		return
	}
	v0, err = t.createTask(id, now)
	if err != nil {
		t.failed = err
	}
	return
}

func (t *Transaction) CreateOperation(d protocol.OperationDescriptor, now time.Time) (v0 state.Operation, err error) {
	if t.failed != nil {
		err = t.failed
		return
	}
	v0, err = t.createOperation(d, now)
	if err != nil {
		t.failed = err
	}
	return
}

func (t *Transaction) MarkReady(id protocol.OperationID, rev uint64, now time.Time) (v0 state.Operation, err error) {
	if t.failed != nil {
		err = t.failed
		return
	}
	v0, err = t.markReady(id, rev, now)
	if err != nil {
		t.failed = err
	}
	return
}

func (t *Transaction) BeginVerification(id protocol.OperationID, rev uint64, now time.Time) (v0 state.Operation, err error) {
	if t.failed != nil {
		err = t.failed
		return
	}
	v0, err = t.beginVerification(id, rev, now)
	if err != nil {
		t.failed = err
	}
	return
}

func (t *Transaction) MarkUnknownOutcome(id protocol.OperationID, rev uint64, now time.Time) (v0 state.Operation, err error) {
	if t.failed != nil {
		err = t.failed
		return
	}
	v0, err = t.markUnknownOutcome(id, rev, now)
	if err != nil {
		t.failed = err
	}
	return
}

func (t *Transaction) MarkNotStarted(id protocol.OperationID, rev uint64, v evidence.Verification, now time.Time) (v0 state.Operation, err error) {
	if t.failed != nil {
		err = t.failed
		return
	}
	v0, err = t.markNotStarted(id, rev, v, now)
	if err != nil {
		t.failed = err
	}
	return
}

func (t *Transaction) ApplyVerification(id protocol.OperationID, rev uint64, v evidence.Verification, now time.Time) (v0 state.Operation, err error) {
	if t.failed != nil {
		err = t.failed
		return
	}
	v0, err = t.applyVerification(id, rev, v, now)
	if err != nil {
		t.failed = err
	}
	return
}

func (t *Transaction) StartAttempt(id protocol.OperationID, rev uint64, attempt protocol.AttemptID, lease protocol.LeaseID, owner protocol.ActorID, retry protocol.AttemptID, now time.Time) (v0 state.Operation, v1 state.Attempt, err error) {
	if t.failed != nil {
		err = t.failed
		return
	}
	v0, v1, err = t.startAttempt(id, rev, attempt, lease, owner, retry, now)
	if err != nil {
		t.failed = err
	}
	return
}

func (t *Transaction) RegisterEvidence(v evidence.Record) (err error) {
	if t.failed != nil {
		err = t.failed
		return
	}
	err = t.registerEvidence(v)
	if err != nil {
		t.failed = err
	}
	return
}

func (t *Transaction) CheckActiveAttemptContext(id protocol.OperationID, attempt protocol.AttemptID, lease protocol.LeaseID, generation uint64, owner protocol.ActorID) (err error) {
	if t.failed != nil {
		err = t.failed
		return
	}
	err = t.checkActiveAttemptContext(id, attempt, lease, generation, owner)
	if err != nil {
		t.failed = err
	}
	return
}

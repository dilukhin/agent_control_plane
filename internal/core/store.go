package core

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/evidence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

var (
	ErrDuplicateID        = errors.New("duplicate identity")
	ErrNotFound           = errors.New("record not found")
	ErrConflictScope      = errors.New("mutation conflict scope is reserved")
	ErrEvidenceScope      = errors.New("evidence does not match operation")
	ErrMessageIDCollision = errors.New("message id collision")
	ErrStaleOwnership     = errors.New("stale or invalid execution ownership")
)

type Store struct {
	mu                sync.RWMutex
	tasks             map[protocol.TaskID]state.Task
	operations        map[protocol.OperationID]state.Operation
	attempts          map[protocol.AttemptID]state.Attempt
	leases            map[protocol.LeaseID]struct{}
	evidence          map[protocol.EvidenceID]evidence.Record
	operationEvidence map[protocol.OperationID][]protocol.EvidenceID
	messages          map[protocol.MessageID]protocol.Envelope
}

func NewStore() *Store {
	return &Store{
		tasks:             make(map[protocol.TaskID]state.Task),
		operations:        make(map[protocol.OperationID]state.Operation),
		attempts:          make(map[protocol.AttemptID]state.Attempt),
		leases:            make(map[protocol.LeaseID]struct{}),
		evidence:          make(map[protocol.EvidenceID]evidence.Record),
		operationEvidence: make(map[protocol.OperationID][]protocol.EvidenceID),
		messages:          make(map[protocol.MessageID]protocol.Envelope),
	}
}

func (s *Store) RegisterMessage(env protocol.Envelope) (bool, error) {
	if err := env.Validate(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.messages[env.MessageID]; ok {
		if reflect.DeepEqual(existing, env) {
			return true, nil
		}
		return false, fmt.Errorf("%w: %s", ErrMessageIDCollision, env.MessageID)
	}
	s.messages[env.MessageID] = cloneEnvelope(env)
	return false, nil
}

func cloneEnvelope(env protocol.Envelope) protocol.Envelope {
	out := env
	out.Payload = cloneMap(env.Payload)
	out.Extensions = cloneMap(env.Extensions)
	if env.Lease != nil {
		lease := *env.Lease
		out.Lease = &lease
	}
	return out
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return cloneMap(x)
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = cloneValue(x[i])
		}
		return out
	default:
		return v
	}
}

func (s *Store) CreateTask(id protocol.TaskID, now time.Time) (state.Task, error) {
	if id == "" || now.IsZero() {
		return state.Task{}, errors.New("task id and time are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[id]; ok {
		return state.Task{}, fmt.Errorf("%w: task %s", ErrDuplicateID, id)
	}
	task := state.Task{ID: id, State: state.TaskPlanned, Revision: 1, CreatedAt: now, UpdatedAt: now}
	s.tasks[id] = task
	return task, nil
}

func (s *Store) GetTask(id protocol.TaskID) (state.Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, ok := s.tasks[id]
	return task, ok
}

func (s *Store) CreateOperation(d protocol.OperationDescriptor, now time.Time) (state.Operation, error) {
	if err := d.Validate(); err != nil {
		return state.Operation{}, err
	}
	if now.IsZero() {
		return state.Operation{}, errors.New("creation time is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[d.TaskID]; !ok {
		return state.Operation{}, fmt.Errorf("%w: task %s", ErrNotFound, d.TaskID)
	}
	if _, ok := s.operations[d.ID]; ok {
		return state.Operation{}, fmt.Errorf("%w: operation %s", ErrDuplicateID, d.ID)
	}
	op := state.Operation{
		Descriptor: d,
		State:      state.OperationPlanned,
		Revision:   1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	s.operations[d.ID] = op
	return op, nil
}

func (s *Store) GetOperation(id protocol.OperationID) (state.Operation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.operations[id]
	return op, ok
}

func (s *Store) MarkReady(id protocol.OperationID, expectedRevision uint64, now time.Time) (state.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.operations[id]
	if !ok {
		return state.Operation{}, fmt.Errorf("%w: operation %s", ErrNotFound, id)
	}
	next, err := state.Transition(op, expectedRevision, state.OperationReady, now)
	if err != nil {
		return state.Operation{}, err
	}
	s.operations[id] = next
	return next, nil
}

func (s *Store) StartAttempt(
	id protocol.OperationID,
	expectedRevision uint64,
	attemptID protocol.AttemptID,
	leaseID protocol.LeaseID,
	owner protocol.ActorID,
	retryOf protocol.AttemptID,
	now time.Time,
) (state.Operation, state.Attempt, error) {
	if leaseID == "" || owner == "" {
		return state.Operation{}, state.Attempt{}, errors.New("lease id and owner are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	op, ok := s.operations[id]
	if !ok {
		return state.Operation{}, state.Attempt{}, fmt.Errorf("%w: operation %s", ErrNotFound, id)
	}
	if op.Revision != expectedRevision {
		return state.Operation{}, state.Attempt{}, fmt.Errorf("%w: got %d want %d", state.ErrRevisionConflict, expectedRevision, op.Revision)
	}
	if _, exists := s.attempts[attemptID]; exists {
		return state.Operation{}, state.Attempt{}, fmt.Errorf("%w: attempt %s", ErrDuplicateID, attemptID)
	}
	if _, exists := s.leases[leaseID]; exists {
		return state.Operation{}, state.Attempt{}, fmt.Errorf("%w: lease %s", ErrDuplicateID, leaseID)
	}
	if op.Descriptor.EffectClass == protocol.EffectMutation {
		if conflict := s.findConflictLocked(op); conflict != "" {
			return state.Operation{}, state.Attempt{}, fmt.Errorf("%w: operation %s conflicts with %s", ErrConflictScope, id, conflict)
		}
	}

	next, attempt, err := state.StartAttempt(op, expectedRevision, attemptID, now)
	if err != nil {
		return state.Operation{}, state.Attempt{}, err
	}
	attempt.LeaseID = leaseID
	attempt.OwnerActorID = owner
	attempt.RetryOf = retryOf

	s.operations[id] = next
	s.attempts[attemptID] = attempt
	s.leases[leaseID] = struct{}{}
	return next, attempt, nil
}

func (s *Store) BeginVerification(id protocol.OperationID, expectedRevision uint64, now time.Time) (state.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.operations[id]
	if !ok {
		return state.Operation{}, fmt.Errorf("%w: operation %s", ErrNotFound, id)
	}
	activeID := op.ActiveAttemptID
	next, err := state.Transition(op, expectedRevision, state.OperationVerifying, now)
	if err != nil {
		return state.Operation{}, err
	}
	if activeID != "" {
		attempt := s.attempts[activeID]
		if op.State == state.OperationExecuting {
			attempt.State = state.AttemptReported
		}
		attempt.UpdatedAt = now
		s.attempts[activeID] = attempt
	}
	s.operations[id] = next
	return next, nil
}

func (s *Store) MarkUnknownOutcome(id protocol.OperationID, expectedRevision uint64, now time.Time) (state.Operation, error) {
	return s.transitionWithAttemptState(id, expectedRevision, state.OperationUnknownOutcome, state.AttemptUncertain, now)
}

func (s *Store) MarkNotStarted(id protocol.OperationID, expectedRevision uint64, verification evidence.Verification, now time.Time) (state.Operation, error) {
	if err := verification.Validate(); err != nil {
		return state.Operation{}, err
	}
	if verification.Verdict != evidence.VerdictNotSatisfiedRetryable || !verification.RetryExecutionSafe {
		return state.Operation{}, errors.New("not-started transition requires retry-safe verification")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.operations[id]
	if !ok {
		return state.Operation{}, fmt.Errorf("%w: operation %s", ErrNotFound, id)
	}
	if err := s.validateVerificationLocked(op, verification); err != nil {
		return state.Operation{}, err
	}
	activeID := op.ActiveAttemptID
	next, err := state.Transition(op, expectedRevision, state.OperationReady, now)
	if err != nil {
		return state.Operation{}, err
	}
	if activeID != "" {
		attempt := s.attempts[activeID]
		attempt.State = state.AttemptNotStarted
		attempt.UpdatedAt = now
		s.attempts[activeID] = attempt
	}
	s.operations[id] = next
	return next, nil
}

func (s *Store) transitionWithAttemptState(
	id protocol.OperationID,
	expectedRevision uint64,
	to state.OperationState,
	attemptState state.AttemptState,
	now time.Time,
) (state.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.operations[id]
	if !ok {
		return state.Operation{}, fmt.Errorf("%w: operation %s", ErrNotFound, id)
	}
	activeID := op.ActiveAttemptID
	next, err := state.Transition(op, expectedRevision, to, now)
	if err != nil {
		return state.Operation{}, err
	}
	if activeID != "" {
		attempt := s.attempts[activeID]
		attempt.State = attemptState
		attempt.UpdatedAt = now
		s.attempts[activeID] = attempt
	}
	s.operations[id] = next
	return next, nil
}

func (s *Store) RegisterEvidence(record evidence.Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.evidence[record.ID]; exists {
		return fmt.Errorf("%w: evidence %s", ErrDuplicateID, record.ID)
	}
	op, ok := s.operations[record.OperationID]
	if !ok || op.Descriptor.TaskID != record.TaskID {
		return ErrEvidenceScope
	}
	if record.AttemptID != "" {
		attempt, ok := s.attempts[record.AttemptID]
		if !ok || attempt.OperationID != record.OperationID {
			return ErrEvidenceScope
		}
	}
	s.evidence[record.ID] = record
	s.operationEvidence[record.OperationID] = append(s.operationEvidence[record.OperationID], record.ID)
	return nil
}

func (s *Store) ApplyVerification(
	id protocol.OperationID,
	expectedRevision uint64,
	verification evidence.Verification,
	now time.Time,
) (state.Operation, error) {
	if err := verification.Validate(); err != nil {
		return state.Operation{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	op, ok := s.operations[id]
	if !ok {
		return state.Operation{}, fmt.Errorf("%w: operation %s", ErrNotFound, id)
	}
	if err := s.validateVerificationLocked(op, verification); err != nil {
		return state.Operation{}, err
	}

	var to state.OperationState
	switch verification.Verdict {
	case evidence.VerdictSatisfied:
		to = state.OperationSucceeded
	case evidence.VerdictNotSatisfiedRetryable:
		to = state.OperationReady
	case evidence.VerdictNotSatisfiedTerminal:
		to = state.OperationFailed
	case evidence.VerdictInconclusive:
		to = state.OperationUnknownOutcome
	default:
		return state.Operation{}, errors.New("unsupported verification verdict")
	}

	activeID := op.ActiveAttemptID
	next, err := state.Transition(op, expectedRevision, to, now)
	if err != nil {
		return state.Operation{}, err
	}
	if activeID != "" {
		attempt := s.attempts[activeID]
		attempt.State = state.AttemptClosed
		if to == state.OperationUnknownOutcome {
			attempt.State = state.AttemptUncertain
		}
		attempt.UpdatedAt = now
		s.attempts[activeID] = attempt
	}
	s.operations[id] = next
	return next, nil
}

func (s *Store) validateVerificationLocked(op state.Operation, verification evidence.Verification) error {
	if verification.OperationID != op.Descriptor.ID || verification.PolicyRef != op.Descriptor.VerificationPolicyRef {
		return ErrEvidenceScope
	}
	if verification.AttemptID == "" || verification.AttemptID != op.ActiveAttemptID {
		return ErrEvidenceScope
	}
	for _, evidenceID := range verification.EvidenceIDs {
		record, ok := s.evidence[evidenceID]
		if !ok || record.OperationID != op.Descriptor.ID || record.AttemptID != verification.AttemptID {
			return ErrEvidenceScope
		}
	}
	return nil
}

func (s *Store) CheckActiveAttemptContext(
	id protocol.OperationID,
	attemptID protocol.AttemptID,
	leaseID protocol.LeaseID,
	generation uint64,
	owner protocol.ActorID,
) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.operations[id]
	if !ok {
		return fmt.Errorf("%w: operation %s", ErrNotFound, id)
	}
	if op.ActiveAttemptID == "" || op.ActiveAttemptID != attemptID || op.LeaseGeneration != generation {
		return ErrStaleOwnership
	}
	attempt, ok := s.attempts[attemptID]
	if !ok || attempt.LeaseID != leaseID || attempt.LeaseGeneration != generation || attempt.OwnerActorID != owner {
		return ErrStaleOwnership
	}
	if op.State != state.OperationExecuting && op.State != state.OperationVerifying && op.State != state.OperationUnknownOutcome {
		return ErrStaleOwnership
	}
	return nil
}

func (s *Store) EvidenceForOperation(id protocol.OperationID) []evidence.Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.operationEvidence[id]
	out := make([]evidence.Record, 0, len(ids))
	for _, evidenceID := range ids {
		out = append(out, s.evidence[evidenceID])
	}
	return out
}

func (s *Store) findConflictLocked(candidate state.Operation) protocol.OperationID {
	for id, other := range s.operations {
		if id == candidate.Descriptor.ID || !other.HoldsConflictReservation() {
			continue
		}
		if scopesOverlap(candidate.Descriptor.ConflictScope, other.Descriptor.ConflictScope) {
			return id
		}
	}
	return ""
}

func scopesOverlap(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, scope := range a {
		set[scope] = struct{}{}
	}
	for _, scope := range b {
		if _, ok := set[scope]; ok {
			return true
		}
	}
	return false
}

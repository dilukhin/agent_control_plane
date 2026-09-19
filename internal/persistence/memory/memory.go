// Package memory is the transactional reference implementation.
package memory

import (
	"context"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/evidence"
	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

type data struct {
	revocations   map[protocol.AttemptID]p.Revocation
	tasks         map[protocol.TaskID]state.Task
	ops           map[protocol.OperationID]state.Operation
	attempts      map[protocol.AttemptID]state.Attempt
	evidence      map[protocol.EvidenceID]evidence.Record
	verifications map[string]evidence.Verification
	messages      map[protocol.MessageID]p.Message
	reservations  map[string]p.Reservation
}

type Repository struct {
	gate   chan struct{}
	d      data
	closed bool
}

func New() *Repository {
	return &Repository{gate: make(chan struct{}, 1), d: data{
		revocations: map[protocol.AttemptID]p.Revocation{},
		tasks:       map[protocol.TaskID]state.Task{}, ops: map[protocol.OperationID]state.Operation{},
		attempts: map[protocol.AttemptID]state.Attempt{}, evidence: map[protocol.EvidenceID]evidence.Record{},
		verifications: map[string]evidence.Verification{}, messages: map[protocol.MessageID]p.Message{},
		reservations: map[string]p.Reservation{},
	}}
}

func (r *Repository) lock(ctx context.Context) error {
	select {
	case r.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *Repository) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.lock(ctx); err != nil {
		return err
	}
	defer func() { <-r.gate }()
	r.closed = true
	return nil
}
func (r *Repository) View(ctx context.Context, fn func(p.Reader) error) error {
	return r.run(ctx, false, func(t p.Tx) error { return fn(t) })
}
func (r *Repository) Update(ctx context.Context, fn func(p.Tx) error) error {
	return r.run(ctx, true, fn)
}
func (r *Repository) run(ctx context.Context, write bool, fn func(p.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := r.lock(ctx); err != nil {
		return err
	}
	defer func() { <-r.gate }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.closed {
		return p.ErrClosed
	}
	// Copy-on-write makes even a late callback error atomic. Values with slices
	// are cloned on both input and output, so the snapshot has no mutable aliases.
	d := data{maps.Clone(r.d.revocations), maps.Clone(r.d.tasks), maps.Clone(r.d.ops), maps.Clone(r.d.attempts),
		maps.Clone(r.d.evidence), maps.Clone(r.d.verifications), maps.Clone(r.d.messages), maps.Clone(r.d.reservations)}
	t := &transaction{d: &d, write: write}
	if err := fn(t); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if write {
		r.d = d
	}
	return nil
}

type transaction struct {
	d     *data
	write bool
}

func get[K comparable, V any](m map[K]V, k K) (V, error) {
	v, ok := m[k]
	if !ok {
		return v, p.ErrNotFound
	}
	return v, nil
}
func insert[K comparable, V any](t *transaction, m map[K]V, k K, v V) error {
	if !t.write {
		return p.ErrIntegrity
	}
	if _, ok := m[k]; ok {
		return p.ErrDuplicateID
	}
	m[k] = v
	return nil
}
func cloneOp(v state.Operation) state.Operation {
	v.Descriptor.ConflictScope = slices.Clone(v.Descriptor.ConflictScope)
	return v
}
func cloneVerification(v evidence.Verification) evidence.Verification {
	v.EvidenceIDs = slices.Clone(v.EvidenceIDs)
	return v
}
func (t *transaction) Task(id protocol.TaskID) (state.Task, error) { return get(t.d.tasks, id) }
func (t *transaction) Operation(id protocol.OperationID) (state.Operation, error) {
	v, e := get(t.d.ops, id)
	return cloneOp(v), e
}
func (t *transaction) Attempt(id protocol.AttemptID) (state.Attempt, error) {
	return get(t.d.attempts, id)
}
func (t *transaction) Evidence(id protocol.EvidenceID) (evidence.Record, error) {
	return get(t.d.evidence, id)
}
func (t *transaction) Verification(id string) (evidence.Verification, error) {
	v, e := get(t.d.verifications, id)
	return cloneVerification(v), e
}
func (t *transaction) Message(id protocol.MessageID) (p.Message, error) { return get(t.d.messages, id) }
func (t *transaction) Reservation(scope string) (p.Reservation, error) {
	return get(t.d.reservations, scope)
}
func (t *transaction) EvidenceForOperation(id protocol.OperationID) ([]evidence.Record, error) {
	out := []evidence.Record{}
	for _, v := range t.d.evidence {
		if v.OperationID == id {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (t *transaction) InsertTask(v state.Task) error { return insert(t, t.d.tasks, v.ID, v) }
func (t *transaction) InsertOperation(v state.Operation) error {
	if _, e := t.Task(v.Descriptor.TaskID); e != nil {
		return e
	}
	return insert(t, t.d.ops, v.Descriptor.ID, cloneOp(v))
}
func (t *transaction) UpdateOperation(v state.Operation, expected uint64) error {
	if !t.write {
		return p.ErrIntegrity
	}
	old, e := t.Operation(v.Descriptor.ID)
	if e != nil {
		return e
	}
	if old.Revision != expected {
		return state.ErrRevisionConflict
	}
	if v.Revision != expected+1 {
		return p.ErrIntegrity
	}
	// The descriptor is immutable across transitions.
	v.Descriptor = old.Descriptor
	t.d.ops[v.Descriptor.ID] = cloneOp(v)
	return nil
}
func (t *transaction) InsertAttempt(v state.Attempt) error {
	for _, a := range t.d.attempts {
		if a.LeaseID == v.LeaseID {
			return p.ErrDuplicateID
		}
	}
	return insert(t, t.d.attempts, v.ID, v)
}
func (t *transaction) UpdateAttempt(v state.Attempt) error {
	if !t.write {
		return p.ErrIntegrity
	}
	old, e := t.Attempt(v.ID)
	if e != nil {
		return e
	}
	if old.OperationID != v.OperationID || old.LeaseID != v.LeaseID || old.LeaseGeneration != v.LeaseGeneration || old.OwnerActorID != v.OwnerActorID {
		return p.ErrIntegrity
	}
	t.d.attempts[v.ID] = v
	return nil
}
func (t *transaction) InsertEvidence(v evidence.Record) error {
	return insert(t, t.d.evidence, v.ID, v)
}
func (t *transaction) InsertVerification(v evidence.Verification) error {
	return insert(t, t.d.verifications, v.ID, cloneVerification(v))
}
func (t *transaction) InsertMessage(v p.Message) error { return insert(t, t.d.messages, v.ID, v) }
func (t *transaction) Reserve(v p.Reservation) error {
	if _, ok := t.d.reservations[v.Scope]; ok {
		return p.ErrConflictScope
	}
	return insert(t, t.d.reservations, v.Scope, v)
}
func (t *transaction) Release(id protocol.OperationID) error {
	if !t.write {
		return p.ErrIntegrity
	}
	for k, v := range t.d.reservations {
		if v.OperationID == id {
			delete(t.d.reservations, k)
		}
	}
	return nil
}

func (t *transaction) OperationsAfter(after protocol.OperationID, limit int) ([]state.Operation, error) {
	if limit < 1 || limit > p.MaxPageSize {
		return nil, p.ErrIntegrity
	}
	ids := make([]protocol.OperationID, 0, len(t.d.ops))
	for id := range t.d.ops {
		if id > after {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]state.Operation, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneOp(t.d.ops[id]))
	}
	return out, nil
}
func (t *transaction) Revocation(id protocol.AttemptID) (p.Revocation, error) {
	return get(t.d.revocations, id)
}
func (t *transaction) InsertRevocation(v p.Revocation) error {
	return insert(t, t.d.revocations, v.AttemptID, v)
}

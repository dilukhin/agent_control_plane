package core

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/evidence"
	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/persistence/sqlite"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

func finishTask(t *testing.T, s *Store, id protocol.TaskID, now time.Time) {
	t.Helper()
	ctx := context.Background()
	opID := protocol.OperationID(id)
	attemptID := protocol.AttemptID(id)
	op := readyOperation(t, s, id, opID, string(id), now)
	op, _, e := s.StartAttempt(ctx, opID, op.Revision, attemptID, protocol.LeaseID(id), "owner", "", now)
	if e != nil {
		t.Fatal(e)
	}
	op, e = s.BeginVerification(ctx, opID, op.Revision, now)
	if e != nil {
		t.Fatal(e)
	}
	rec := evidence.Record{ID: protocol.EvidenceID(id), TaskID: id, OperationID: opID, AttemptID: attemptID, Kind: evidence.KindTargetObservation, SourceActorID: "verifier", ObservedAt: now, SubjectRef: string(id)}
	if e = s.RegisterEvidence(ctx, rec); e != nil {
		t.Fatal(e)
	}
	if _, e = s.ApplyVerification(ctx, opID, op.Revision, evidence.Verification{ID: string(id), OperationID: opID, AttemptID: attemptID, PolicyRef: "verify:v1", EvidenceIDs: []protocol.EvidenceID{rec.ID}, Verdict: evidence.VerdictSatisfied, VerifiedAt: now, VerifierActorID: "verifier"}, now); e != nil {
		t.Fatal(e)
	}
	if _, e = s.FinalizeTask(ctx, id, 1, now); e != nil {
		t.Fatal(e)
	}
}
func TestRetentionBoundedGraphsAndProtectedWork(t *testing.T) {
	forStores(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now().UTC()
		old := now.Add(-40 * 24 * time.Hour)
		for _, id := range []protocol.TaskID{"a", "b", "c"} {
			finishTask(t, s, id, old)
		}
		finishTask(t, s, "recent", now)
		op := readyOperation(t, s, "active", "active", "active", old)
		op, _, e := s.StartAttempt(ctx, "active", op.Revision, "active", "active", "owner", "", old)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.MarkUnknownOutcome(ctx, "active", op.Revision, old); e != nil {
			t.Fatal(e)
		}
		if _, e = s.FinalizeTask(ctx, "active", 1, old); e == nil {
			t.Fatal("unresolved task finalized")
		}
		if _, e = s.CreateOperation(ctx, desc("a", "new", "new"), now); !errors.Is(e, p.ErrTaskTerminal) {
			t.Fatalf("terminal task mutable: %v", e)
		}
		// Smaller than one operation graph: defer, preserving proof and owner.
		result, e := s.Collect(ctx, p.GCOptions{Now: now, MaxTasks: 100, MaxRows: 5})
		if e != nil || result.Rows != 0 || result.OversizedOperations != 3 {
			t.Fatalf("oversized: %+v %v", result, e)
		}
		if _, e = s.GetVerification(ctx, "a"); e != nil {
			t.Fatal("proof removed early")
		}
		// Exactly one graph per batch. Metadata is removed in subsequent batches.
		cursor := protocol.TaskID("")
		deleted := 0
		for i := 0; i < 20; i++ {
			result, e = s.Collect(ctx, p.GCOptions{Now: now, After: cursor, MaxTasks: 1, MaxRows: 6})
			if e != nil {
				t.Fatal(e)
			}
			if result.Rows > 6 {
				t.Fatalf("budget exceeded: %+v", result)
			}
			deleted += result.Rows
			cursor = result.Next
			if !result.More {
				break
			}
			if i == 19 {
				t.Fatal("GC made no bounded progress")
			}
		}
		if deleted != 21 {
			t.Fatalf("deleted=%d want three complete graphs+tasks", deleted)
		}
		for _, id := range []protocol.TaskID{"a", "b", "c"} {
			if _, e = s.GetTask(ctx, id); !errors.Is(e, ErrNotFound) {
				t.Fatalf("task retained: %s %v", id, e)
			}
		}
		if _, e = s.GetVerification(ctx, "recent"); e != nil {
			t.Fatal("recent proof lost")
		}
		if e = s.repository.View(ctx, func(r p.Reader) error {
			v, e := r.Reservation("active")
			if e != nil {
				return e
			}
			if v.AttemptID != "active" {
				return errors.New("reservation changed")
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
		if _, e = s.Collect(ctx, p.GCOptions{Now: now, MaxTasks: 1, MaxRows: p.MaxGCRows + 1}); e == nil {
			t.Fatal("unbounded budget accepted")
		}
	})
}
func TestRetentionReplayFloorAndClockRollback(t *testing.T) {
	forStores(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		past := time.Now().UTC().Add(-9 * 24 * time.Hour)
		now := past.Add(9 * 24 * time.Hour)
		s.now = func() time.Time { return past }
		env := testEnvelope("orphan")
		env.IssuedAt = past
		if duplicate, e := s.RegisterMessage(ctx, env); e != nil || duplicate {
			t.Fatalf("initial: %v %v", duplicate, e)
		}
		result, e := s.Collect(ctx, p.GCOptions{Now: now, MaxTasks: 10, MaxRows: 1})
		if e != nil || result.Messages != 1 || result.Rows != 1 {
			t.Fatalf("orphan cleanup: %+v %v", result, e)
		}
		// Clock rollback does not reopen an already discarded replay interval.
		if _, e = s.RegisterMessage(ctx, env); !errors.Is(e, p.ErrExpiredMessage) {
			t.Fatalf("replay after deletion: %v", e)
		}
		s.now = func() time.Time { return now }
		env.MessageID = "fresh"
		env.IssuedAt = now
		if _, e = s.RegisterMessage(ctx, env); e != nil {
			t.Fatal(e)
		}
		if duplicate, e := s.RegisterMessage(ctx, env); e != nil || !duplicate {
			t.Fatalf("fresh dedup: %v %v", duplicate, e)
		}
		env.MessageID = "future"
		env.IssuedAt = now.Add(6 * time.Minute)
		if _, e = s.RegisterMessage(ctx, env); !errors.Is(e, p.ErrExpiredMessage) {
			t.Fatalf("future timestamp: %v", e)
		}
	})
}

type failGCRepository struct {
	p.Repository
	failure error
}

func (r failGCRepository) Update(ctx context.Context, fn func(p.Tx) error) error {
	return r.Repository.Update(ctx, func(tx p.Tx) error {
		if e := fn(tx); e != nil {
			return e
		}
		return r.failure
	})
}
func TestRetentionRollbackRestoresProofAndReplayFloor(t *testing.T) {
	forStores(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now().UTC()
		finishTask(t, s, "a", now.Add(-40*24*time.Hour))
		sentinel := errors.New("injected commit-boundary failure")
		original := s.repository
		s.repository = failGCRepository{original, sentinel}
		if _, e := s.Collect(ctx, p.GCOptions{Now: now, MaxTasks: 10, MaxRows: 100}); !errors.Is(e, sentinel) {
			t.Fatal(e)
		}
		s.repository = original
		if _, e := s.GetVerification(ctx, "a"); e != nil {
			t.Fatal("proof lost on rollback")
		}
		if e := original.View(ctx, func(r p.Reader) error {
			floor, e := r.MessageFloor()
			if e != nil {
				return e
			}
			if floor != 0 {
				return errors.New("floor committed on rollback")
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
	})
}
func TestRetentionDefersTaskWithFreshDedup(t *testing.T) {
	forStores(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now().UTC()
		old := now.Add(-40 * 24 * time.Hour)
		// Receive a message now, then finalize with an old fixture time. Its fresh
		// replay record must override the otherwise elapsed task retention period.
		env := testEnvelope("fresh-owner")
		env.TaskID = "a"
		env.IssuedAt = now
		if _, e := s.RegisterMessage(ctx, env); e != nil {
			t.Fatal(e)
		}
		finishTask(t, s, "a", old)
		result, e := s.Collect(ctx, p.GCOptions{Now: now, MaxTasks: 10, MaxRows: 100})
		if e != nil || result.DeferredTasks != 1 || result.Rows != 0 {
			t.Fatalf("fresh owner: %+v %v", result, e)
		}
		result, e = s.Collect(ctx, p.GCOptions{Now: now.Add(8 * 24 * time.Hour), MaxTasks: 10, MaxRows: 100})
		if e != nil || result.Tasks != 1 || result.Messages != 1 {
			t.Fatalf("expired owner: %+v %v", result, e)
		}
	})
}
func TestSQLiteRetentionReopenAndMonotonicFloor(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "retention.db")
	open := func() *Store {
		r, e := sqlite.Open(ctx, path, sqlite.Options{})
		if e != nil {
			t.Fatal(e)
		}
		return NewWithRepository(r)
	}
	s := open()
	now := time.Now().UTC()
	past := now.Add(-9 * 24 * time.Hour)
	s.now = func() time.Time { return past }
	env := testEnvelope("orphan")
	env.IssuedAt = past
	if _, e := s.RegisterMessage(ctx, env); e != nil {
		t.Fatal(e)
	}
	finishTask(t, s, "old", now.Add(-40*24*time.Hour))
	if _, e := s.Collect(ctx, p.GCOptions{Now: now, MaxTasks: 10, MaxRows: 100}); e != nil {
		t.Fatal(e)
	}
	s.Close()
	s = open()
	defer s.Close()
	s.now = func() time.Time { return past }
	if _, e := s.RegisterMessage(ctx, env); !errors.Is(e, p.ErrExpiredMessage) {
		t.Fatalf("reopen replay: %v", e)
	}
	if _, e := s.GetTask(ctx, "old"); !errors.Is(e, ErrNotFound) {
		t.Fatalf("cleanup not durable: %v", e)
	}
}

func TestRetentionRemovesRetryAndRevocationGraphAtomically(t *testing.T) {
	forStores(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now().UTC()
		old := now.Add(-40 * 24 * time.Hour)
		op := readyOperation(t, s, "task", "op", "scope", old)
		op, _, e := s.StartAttempt(ctx, "op", op.Revision, "first", "first", "owner", "", old)
		if e != nil {
			t.Fatal(e)
		}
		op, e = s.InvalidateAttempt(ctx, "op", op.Revision, "first", ControllerRestart, lossRecord("loss", "op", "first"), old)
		if e != nil {
			t.Fatal(e)
		}
		for i := 0; i < 2; i++ {
			id := protocol.AttemptID("first")
			eid := protocol.EvidenceID("proof-first")
			verdict := evidence.VerdictNotSatisfiedRetryable
			if i == 1 {
				id = "second"
				eid = "proof-second"
				verdict = evidence.VerdictSatisfied
			}
			op, e = s.BeginVerification(ctx, "op", op.Revision, old)
			if e != nil {
				t.Fatal(e)
			}
			rec := lossRecord(eid, "op", id)
			rec.Kind = evidence.KindTargetObservation
			if e = s.RegisterEvidence(ctx, rec); e != nil {
				t.Fatal(e)
			}
			op, e = s.ApplyVerification(ctx, "op", op.Revision, evidence.Verification{ID: string(eid), OperationID: "op", AttemptID: id, PolicyRef: "verify:v1", EvidenceIDs: []protocol.EvidenceID{eid}, Verdict: verdict, RetryExecutionSafe: i == 0, VerifiedAt: old, VerifierActorID: "verifier"}, old)
			if e != nil {
				t.Fatal(e)
			}
			if i == 0 {
				op, _, e = s.StartAttempt(ctx, "op", op.Revision, "second", "second", "new-owner", "first", old)
				if e != nil {
					t.Fatal(e)
				}
			}
		}
		if _, e = s.FinalizeTask(ctx, "task", 1, old); e != nil {
			t.Fatal(e)
		}
		result, e := s.Collect(ctx, p.GCOptions{Now: now, MaxTasks: 1, MaxRows: 100})
		if e != nil || result.Tasks != 1 || result.Operations != 1 {
			t.Fatalf("graph cleanup: %+v %v", result, e)
		}
		if e = s.repository.View(ctx, func(r p.Reader) error {
			if _, e := r.Revocation("first"); !errors.Is(e, ErrNotFound) {
				return errors.New("orphan revocation")
			}
			if _, e := r.Attempt("second"); !errors.Is(e, ErrNotFound) {
				return errors.New("orphan retry")
			}
			records, e := r.EvidenceForOperation("op")
			if e != nil {
				return e
			}
			if len(records) != 0 {
				return errors.New("orphan evidence")
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
	})
}

func TestRetentionProtectsLegacyCrossOwnerMessage(t *testing.T) {
	forStores(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now().UTC()
		past := now.Add(-40 * 24 * time.Hour)
		readyOperation(t, s, "live", "live-op", "scope", past)
		env := testEnvelope("mismatch")
		env.TaskID = "missing"
		env.OperationID = "live-op"
		env.AttemptID = ""
		env.IssuedAt = now
		if _, e := s.RegisterMessage(ctx, env); !errors.Is(e, ErrEvidenceScope) {
			t.Fatalf("cross-owner message accepted: %v", e)
		}
		// A malformed legacy correlation is retained conservatively until the real
		// referenced owner is gone, even though the message's task ID is absent.
		if e := s.repository.Update(ctx, func(tx p.Tx) error {
			return tx.InsertMessage(p.Message{ID: "legacy", TaskID: "missing", OperationID: "live-op", Digest: "v1:0000000000000000000000000000000000000000000000000000000000000000", IssuedAt: past, ExpiresAt: past.Add(p.ReplayWindow).Unix()})
		}); e != nil {
			t.Fatal(e)
		}
		result, e := s.Collect(ctx, p.GCOptions{Now: now, MaxTasks: 10, MaxRows: 100})
		if e != nil || result.Rows != 0 {
			t.Fatalf("legacy dependency deleted: %+v %v", result, e)
		}
		if e = s.repository.View(ctx, func(r p.Reader) error { _, e := r.Message("legacy"); return e }); e != nil {
			t.Fatal(e)
		}
	})
}

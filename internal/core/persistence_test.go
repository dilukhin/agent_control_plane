package core

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/evidence"
	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/persistence/sqlite"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

func forStores(t *testing.T, fn func(*testing.T, *Store)) {
	t.Helper()
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			s := NewStore()
			if backend == "sqlite" {
				r, e := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"), sqlite.Options{})
				if e != nil {
					t.Fatal(e)
				}
				s = NewWithRepository(r)
			}
			t.Cleanup(func() {
				if e := s.Close(); e != nil {
					t.Error(e)
				}
			})
			fn(t, s)
		})
	}
}
func TestAtomicMessageStateAndCancellation(t *testing.T) {
	forStores(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		env := testEnvelope("msg_atomic")
		sentinel := errors.New("injected failure")
		for _, mode := range []string{"error", "cancel", "panic"} {
			child, cancel := context.WithCancel(ctx)
			func() {
				defer func() {
					if v := recover(); mode == "panic" && v != sentinel {
						t.Errorf("panic=%v", v)
					}
				}()
				_, err := s.ProcessMessage(child, env, func(tx *Transaction) error {
					if _, err := tx.CreateTask("tsk_1", now); err != nil {
						return err
					}
					if _, err := tx.CreateOperation(desc("tsk_1", "op_1", "target"), now); err != nil {
						return err
					}
					switch mode {
					case "cancel":
						cancel()
						return nil
					case "panic":
						panic(sentinel)
					}
					return sentinel
				})
				if err == nil {
					t.Error("injected transaction committed")
				}
			}()
			cancel()
			if _, err := s.GetTask(ctx, "tsk_1"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("task survived rollback: %v", err)
			}
		}
		calls := 0
		duplicate, err := s.ProcessMessage(ctx, env, func(tx *Transaction) error { calls++; _, err := tx.CreateTask("tsk_1", now); return err })
		if err != nil || duplicate {
			t.Fatalf("commit: %v %v", duplicate, err)
		}
		duplicate, err = s.ProcessMessage(ctx, env, func(*Transaction) error { calls++; return sentinel })
		if err != nil || !duplicate || calls != 1 {
			t.Fatalf("duplicate=%v calls=%d error=%v", duplicate, calls, err)
		}
		// Same instant, different zone and equal JSON numeric values have one digest.
		env.IssuedAt = env.IssuedAt.In(time.FixedZone("offset", 3600))
		if d, e := s.RegisterMessage(ctx, env); e != nil || !d {
			t.Fatalf("timestamp normalization: %v %v", d, e)
		}
	})
}
func TestConflictRollbackAndSafeReady(t *testing.T) {
	forStores(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := readyOperation(t, s, "task", "a", "shared", now)
		bdesc := desc("task", "b", "shared")
		bdesc.ConflictScope = []string{"free", "shared"}
		b, e := s.CreateOperation(ctx, bdesc, now)
		if e != nil {
			t.Fatal(e)
		}
		b, e = s.MarkReady(ctx, "b", b.Revision, now)
		if e != nil {
			t.Fatal(e)
		}
		executing, _, e := s.StartAttempt(ctx, "a", a.Revision, "aa", "la", "owner", "", now)
		if e != nil {
			t.Fatal(e)
		}
		if _, _, e = s.StartAttempt(ctx, "b", b.Revision, "ab", "lb", "owner", "", now); !errors.Is(e, ErrConflictScope) {
			t.Fatalf("conflict: %v", e)
		}
		if _, e = s.GetAttempt(ctx, "ab"); !errors.Is(e, ErrNotFound) {
			t.Fatalf("attempt survived conflict: %v", e)
		}
		if e = s.repository.View(ctx, func(r p.Reader) error {
			_, e := r.Reservation("free")
			if !errors.Is(e, ErrNotFound) {
				return errors.New("partial reservation survived")
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
		if _, e = s.MarkReady(ctx, "a", executing.Revision, now); !errors.Is(e, state.ErrInvalidTransition) {
			t.Fatalf("unsafe ready accepted: %v", e)
		}
		// Duplicate lease on an unrelated operation must roll back as well.
		c := readyOperation(t, s, "task", "c", "free", now)
		if _, _, e = s.StartAttempt(ctx, "c", c.Revision, "ac", "la", "owner", "", now); !errors.Is(e, ErrDuplicateID) {
			t.Fatalf("lease reuse: %v", e)
		}
		if _, _, e = s.StartAttempt(ctx, "c", c.Revision, "ac", "lc", "owner", "", now); e != nil {
			t.Fatal(e)
		}
	})
}
func TestSQLiteReopenPreservesGraphAndOwnership(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "данные with spaces #1.db")
	open := func() *Store {
		r, e := sqlite.Open(ctx, path, sqlite.Options{})
		if e != nil {
			t.Fatal(e)
		}
		return NewWithRepository(r)
	}
	s := open()
	now := time.Date(2026, 9, 19, 1, 2, 3, 123456789, time.FixedZone("offset", 3600))
	a := readyOperation(t, s, "task", "a", "shared", now)
	b := readyOperation(t, s, "task", "b", "shared", now)
	a, attempt, e := s.StartAttempt(ctx, "a", a.Revision, "aa", "la", "owner", "", now)
	if e != nil {
		t.Fatal(e)
	}
	a, e = s.MarkUnknownOutcome(ctx, "a", a.Revision, now)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s = open()
	got, e := s.GetOperation(ctx, "a")
	if e != nil || !reflect.DeepEqual(got, a) {
		t.Fatalf("reopen operation: %+v %v", got, e)
	}
	if e = s.CheckActiveAttemptContext(ctx, "a", attempt.ID, attempt.LeaseID, 1, "owner"); !errors.Is(e, ErrStaleOwnership) {
		t.Fatal(e)
	}
	if _, _, e = s.StartAttempt(ctx, "b", b.Revision, "ab", "lb", "owner", "", now); !errors.Is(e, ErrConflictScope) {
		t.Fatalf("reservation lost: %v", e)
	}
	a, e = s.BeginVerification(ctx, "a", a.Revision, now)
	if e != nil {
		t.Fatal(e)
	}
	rec := evidence.Record{ID: "ev", TaskID: "task", OperationID: "a", AttemptID: "aa", Kind: evidence.KindTargetObservation, SourceActorID: "verifier", ObservedAt: now.UTC(), SubjectRef: "target"}
	v := evidence.Verification{ID: "verification", OperationID: "a", AttemptID: "aa", PolicyRef: "verify:v1", EvidenceIDs: []protocol.EvidenceID{"ev"}, Verdict: evidence.VerdictSatisfied, VerifiedAt: now.UTC(), VerifierActorID: "verifier"}
	env := testEnvelope("message")
	env.TaskID = "task"
	env.OperationID = "a"
	env.AttemptID = "aa"
	// Fail after inserting evidence and verification, to exercise full rollback.
	sentinel := errors.New("after verification")
	apply := func(tx *Transaction) error {
		if err := tx.RegisterEvidence(rec); err != nil {
			return err
		}
		_, err := tx.ApplyVerification("a", a.Revision, v, now)
		return err
	}
	if _, e = s.ProcessMessage(ctx, env, func(tx *Transaction) error {
		if err := apply(tx); err != nil {
			return err
		}
		return sentinel
	}); !errors.Is(e, sentinel) {
		t.Fatal(e)
	}
	if _, e = s.GetVerification(ctx, v.ID); !errors.Is(e, ErrNotFound) {
		t.Fatalf("verification survived: %v", e)
	}
	if records, e := s.EvidenceForOperation(ctx, "a"); e != nil || len(records) != 0 {
		t.Fatalf("evidence survived: %v %v", records, e)
	}
	if _, _, e = s.StartAttempt(ctx, "b", b.Revision, "ab", "lb", "owner", "", now); !errors.Is(e, ErrConflictScope) {
		t.Fatalf("reservation released on rollback: %v", e)
	}
	if _, e = s.ProcessMessage(ctx, env, apply); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s = open()
	defer s.Close()
	stored, e := s.GetVerification(ctx, v.ID)
	if e != nil || !reflect.DeepEqual(stored, v) {
		t.Fatalf("verification: %+v %v", stored, e)
	}
	records, e := s.EvidenceForOperation(ctx, "a")
	if e != nil || len(records) != 1 || !reflect.DeepEqual(records[0], rec) {
		t.Fatalf("evidence: %+v %v", records, e)
	}
	if duplicate, e := s.RegisterMessage(ctx, env); e != nil || !duplicate {
		t.Fatalf("dedup lost: %v %v", duplicate, e)
	}
	env.Payload["intent"] = "changed after reopen"
	if _, e = s.RegisterMessage(ctx, env); !errors.Is(e, ErrMessageIDCollision) {
		t.Fatalf("collision after reopen: %v", e)
	}
	if _, _, e = s.StartAttempt(ctx, "b", b.Revision, "ab", "lb", "owner", "", now); e != nil {
		t.Fatalf("reservation retained after success: %v", e)
	}
	old, e := s.GetAttempt(ctx, "aa")
	if e != nil || old.State != state.AttemptClosed {
		t.Fatalf("attempt not closed: %+v %v", old, e)
	}
}
func TestSQLiteTwoHandlesSerializeCAS(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	r1, e := sqlite.Open(ctx, path, sqlite.Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer r1.Close()
	r2, e := sqlite.Open(ctx, path, sqlite.Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer r2.Close()
	a, b := NewWithRepository(r1), NewWithRepository(r2)
	now := time.Now()
	op := readyOperation(t, a, "task", "op", "scope", now)
	start := make(chan struct{})
	results := make(chan error, 2)
	for i, s := range []*Store{a, b} {
		go func() {
			<-start
			id := protocol.AttemptID("first")
			lease := protocol.LeaseID("first")
			if i == 1 {
				id = "second"
				lease = "second"
			}
			_, _, err := s.StartAttempt(ctx, "op", op.Revision, id, lease, "owner", "", now)
			results <- err
		}()
	}
	close(start)
	success, conflict := 0, 0
	for range 2 {
		err := <-results
		if err == nil {
			success++
		} else if errors.Is(err, state.ErrRevisionConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("success=%d conflicts=%d", success, conflict)
	}
}

func TestIgnoredDecisionErrorStillRollsBack(t *testing.T) {
	forStores(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		op := readyOperation(t, s, "task", "a", "shared", now)
		if _, _, e := s.StartAttempt(ctx, "a", op.Revision, "aa", "la", "owner", "", now); e != nil {
			t.Fatal(e)
		}
		b := readyOperation(t, s, "task", "b", "shared", now)
		env := testEnvelope("ignored_error")
		_, e := s.ProcessMessage(ctx, env, func(tx *Transaction) error {
			_, _, _ = tx.StartAttempt("b", b.Revision, "ab", "lb", "owner", "", now)
			return nil
		})
		if !errors.Is(e, ErrConflictScope) {
			t.Fatalf("ignored error committed: %v", e)
		}
		if _, e = s.GetAttempt(ctx, "ab"); !errors.Is(e, ErrNotFound) {
			t.Fatalf("partial attempt: %v", e)
		}
		if duplicate, e := s.RegisterMessage(ctx, env); e != nil || duplicate {
			t.Fatalf("message committed on failed decision: %v %v", duplicate, e)
		}
	})
}

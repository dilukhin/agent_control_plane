package core

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/evidence"
	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/persistence/sqlite"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

func lossRecord(id protocol.EvidenceID, op protocol.OperationID, attempt protocol.AttemptID) evidence.Record {
	return evidence.Record{ID: id, TaskID: "task", OperationID: op, AttemptID: attempt, Kind: evidence.KindProcessObservation, SourceActorID: "controller", ObservedAt: time.Now().UTC(), SubjectRef: "executor", Summary: "execution certainty lost"}
}
func TestRecoveryRevocationAndReconciliation(t *testing.T) {
	forStores(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := readyOperation(t, s, "task", "a", "shared", now)
		b := readyOperation(t, s, "task", "b", "shared", now)
		a, attempt, e := s.StartAttempt(ctx, "a", a.Revision, "aa", "la", "owner", "", now)
		if e != nil {
			t.Fatal(e)
		}
		a, e = s.InvalidateAttempt(ctx, "a", a.Revision, attempt.ID, LeaseExpired, lossRecord("lost", "a", "aa"), now)
		if e != nil {
			t.Fatal(e)
		}
		if a.State != state.OperationUnknownOutcome || a.ActiveAttemptID != "aa" || a.LeaseGeneration != 1 {
			t.Fatalf("unexpected loss: %+v", a)
		}
		page, e := s.InspectRecovery(ctx, "", 1)
		if e != nil || len(page.Items) != 1 || page.Next != "a" || page.Items[0].Class != RecoveryUnknown || page.Items[0].Revocation == nil {
			t.Fatalf("page=%+v %v", page, e)
		}
		next, e := s.InspectRecovery(ctx, page.Next, 1)
		if e != nil || len(next.Items) != 1 || next.Items[0].Class != RecoveryReady {
			t.Fatalf("next=%+v %v", next, e)
		}
		if _, e = s.InspectRecovery(ctx, "", 0); e == nil {
			t.Fatal("unbounded page accepted")
		}
		if _, _, e = s.StartAttempt(ctx, "b", b.Revision, "ab", "lb", "owner", "", now); !errors.Is(e, ErrConflictScope) {
			t.Fatalf("lost reservation: %v", e)
		}
		if _, _, e = s.StartAttempt(ctx, "a", a.Revision, "retry", "retry", "owner", "aa", now); !errors.Is(e, state.ErrInvalidTransition) {
			t.Fatalf("unsafe retry: %v", e)
		}
		a, e = s.BeginVerification(ctx, "a", a.Revision, now)
		if e != nil {
			t.Fatal(e)
		}
		if e = s.CheckActiveAttemptContext(ctx, "a", "aa", "la", 1, "owner"); !errors.Is(e, ErrStaleOwnership) {
			t.Fatalf("old authority returned: %v", e)
		}
		// A late worker report is retained as evidence, never as canonical authority.
		late := lossRecord("late", "a", "aa")
		late.Kind = evidence.KindWorkerReport
		if e = s.RegisterEvidence(ctx, late); e != nil {
			t.Fatal(e)
		}
		rec := lossRecord("quiesced", "a", "aa")
		rec.Kind = evidence.KindTargetObservation
		rec.Summary = "old executor quiescence and safe retry independently verified"
		if e = s.RegisterEvidence(ctx, rec); e != nil {
			t.Fatal(e)
		}
		a, e = s.ApplyVerification(ctx, "a", a.Revision, evidence.Verification{ID: "verification", OperationID: "a", AttemptID: "aa", PolicyRef: "verify:v1", EvidenceIDs: []protocol.EvidenceID{rec.ID}, Verdict: evidence.VerdictNotSatisfiedRetryable, RetryExecutionSafe: true, VerifiedAt: now, VerifierActorID: "verifier"}, now)
		if e != nil {
			t.Fatal(e)
		}
		a, attempt, e = s.StartAttempt(ctx, "a", a.Revision, "new", "new", "new-owner", "aa", now)
		if e != nil {
			t.Fatal(e)
		}
		if attempt.LeaseGeneration != 2 {
			t.Fatal("generation not advanced")
		}
		if e = s.CheckActiveAttemptContext(ctx, "a", "aa", "la", 1, "owner"); !errors.Is(e, ErrStaleOwnership) {
			t.Fatal("old owner accepted")
		}
		if e = s.CheckActiveAttemptContext(ctx, "a", "new", "new", 2, "new-owner"); e != nil {
			t.Fatal(e)
		}
	})
}
func TestRecoveryClassificationAndAtomicInvalidation(t *testing.T) {
	forStores(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		for _, pair := range []struct {
			id      protocol.OperationID
			verdict evidence.Verdict
		}{{"done", evidence.VerdictSatisfied}, {"failed", evidence.VerdictNotSatisfiedTerminal}} {
			op := readyOperation(t, s, "task", pair.id, string(pair.id), now)
			id := protocol.AttemptID(pair.id)
			op, _, e := s.StartAttempt(ctx, pair.id, op.Revision, id, protocol.LeaseID(pair.id), "owner", "", now)
			if e != nil {
				t.Fatal(e)
			}
			op, e = s.BeginVerification(ctx, pair.id, op.Revision, now)
			if e != nil {
				t.Fatal(e)
			}
			rec := lossRecord(protocol.EvidenceID(pair.id), pair.id, id)
			if e = s.RegisterEvidence(ctx, rec); e != nil {
				t.Fatal(e)
			}
			if _, e = s.ApplyVerification(ctx, pair.id, op.Revision, evidence.Verification{ID: string(pair.id), OperationID: pair.id, AttemptID: id, PolicyRef: "verify:v1", EvidenceIDs: []protocol.EvidenceID{rec.ID}, Verdict: pair.verdict, VerifiedAt: now, VerifierActorID: "verifier"}, now); e != nil {
				t.Fatal(e)
			}
		}
		op := readyOperation(t, s, "task", "running", "running", now)
		op, _, e := s.StartAttempt(ctx, "running", op.Revision, "running", "running", "owner", "", now)
		if e != nil {
			t.Fatal(e)
		}
		page, e := s.InspectRecovery(ctx, "", 100)
		if e != nil {
			t.Fatal(e)
		}
		want := []RecoveryClass{RecoveryVerified, RecoveryFailed, RecoveryEvaluate}
		for i, item := range page.Items {
			if item.Class != want[i] {
				t.Fatalf("class=%s", item.Class)
			}
		}
		sentinel := errors.New("late rollback")
		if _, e = s.ProcessMessage(ctx, testEnvelope("loss"), func(tx *Transaction) error {
			if _, e := tx.InvalidateAttempt("running", op.Revision, "running", ControllerRestart, lossRecord("restart", "running", "running"), now); e != nil {
				return e
			}
			return sentinel
		}); !errors.Is(e, sentinel) {
			t.Fatal(e)
		}
		actual, e := s.GetOperation(ctx, "running")
		if e != nil || actual.Revision != op.Revision || actual.State != state.OperationExecuting {
			t.Fatalf("partial invalidation: %+v %v", actual, e)
		}
		if e = s.repository.View(ctx, func(r p.Reader) error {
			_, e := r.Revocation("running")
			if !errors.Is(e, ErrNotFound) {
				return errors.New("revocation survived rollback")
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
		if _, e = s.InvalidateAttempt(ctx, "running", op.Revision-1, "running", ControllerRestart, lossRecord("restart", "running", "running"), now); !errors.Is(e, state.ErrRevisionConflict) {
			t.Fatalf("CAS=%v", e)
		}
		if _, e = s.InvalidateAttempt(ctx, "running", op.Revision, "old", ControllerRestart, lossRecord("restart", "running", "old"), now); !errors.Is(e, ErrStaleOwnership) {
			t.Fatalf("ownership=%v", e)
		}
	})
}
func TestRecoveryAcrossProcesses(t *testing.T) {
	ctx := context.Background()
	if path := os.Getenv("ACP_RECOVERY_TEST_DB"); path != "" {
		r, e := sqlite.Open(ctx, path, sqlite.Options{})
		if e != nil {
			os.Exit(11)
		}
		s := NewWithRepository(r)
		id := protocol.OperationID(os.Getenv("ACP_RECOVERY_TEST_OPERATION"))
		op, e := s.GetOperation(ctx, id)
		if e != nil {
			os.Exit(12)
		}
		_, _, e = s.StartAttempt(ctx, id, op.Revision, protocol.AttemptID(id), protocol.LeaseID(id), "owner", "", time.Now())
		if errors.Is(e, ErrConflictScope) {
			os.Exit(20)
		}
		if e != nil {
			os.Exit(13)
		}
		// Abrupt exit: no Close and no deferred cleanup.
		os.Exit(0)
	}
	path := filepath.Join(t.TempDir(), "process.db")
	open := func() *Store {
		r, e := sqlite.Open(ctx, path, sqlite.Options{})
		if e != nil {
			t.Fatal(e)
		}
		return NewWithRepository(r)
	}
	s := open()
	now := time.Now()
	readyOperation(t, s, "task", "a", "shared", now)
	readyOperation(t, s, "task", "b", "shared", now)
	s.Close()
	commands := []*exec.Cmd{}
	for _, id := range []string{"a", "b"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestRecoveryAcrossProcesses$")
		cmd.Env = append(os.Environ(), "ACP_RECOVERY_TEST_DB="+path, "ACP_RECOVERY_TEST_OPERATION="+id)
		commands = append(commands, cmd)
		if e := cmd.Start(); e != nil {
			t.Fatal(e)
		}
	}
	success, conflict := 0, 0
	for _, cmd := range commands {
		e := cmd.Wait()
		if e == nil {
			success++
		} else {
			var exit *exec.ExitError
			if errors.As(e, &exit) && exit.ExitCode() == 20 {
				conflict++
			} else {
				t.Fatal(e)
			}
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("process success=%d conflict=%d", success, conflict)
	}
	s = open()
	page, e := s.InspectRecovery(ctx, "", 100)
	if e != nil {
		t.Fatal(e)
	}
	var active state.Operation
	for _, item := range page.Items {
		if item.Class == RecoveryEvaluate {
			active = item.Operation
		}
	}
	if active.ActiveAttemptID == "" {
		t.Fatal("lost committed execution")
	}
	active, e = s.InvalidateAttempt(ctx, active.Descriptor.ID, active.Revision, active.ActiveAttemptID, ControllerRestart, lossRecord("restart", active.Descriptor.ID, active.ActiveAttemptID), now)
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	s = open()
	defer s.Close()
	page, e = s.InspectRecovery(ctx, "", 100)
	if e != nil {
		t.Fatal(e)
	}
	for _, item := range page.Items {
		if item.Operation.Descriptor.ID == active.Descriptor.ID {
			if item.Class != RecoveryUnknown || item.Revocation == nil {
				t.Fatalf("lost recovery: %+v", item)
			}
		}
	}
	if e = s.CheckActiveAttemptContext(ctx, active.Descriptor.ID, active.ActiveAttemptID, protocol.LeaseID(active.Descriptor.ID), 1, "owner"); !errors.Is(e, ErrStaleOwnership) {
		t.Fatalf("authority survived: %v", e)
	}
}

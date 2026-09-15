package core

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/evidence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

func desc(task protocol.TaskID, op protocol.OperationID, target string) protocol.OperationDescriptor {
	return protocol.OperationDescriptor{
		ID:                     op,
		TaskID:                 task,
		Name:                   "target.update",
		TargetRef:              target,
		EffectClass:            protocol.EffectMutation,
		ConflictScope:          []string{target},
		IdempotencyMode:        protocol.IdempotencyUnknown,
		VerificationPolicyRef:  "verify:v1",
		AuthorizationPolicyRef: "auth:v1",
	}
}

func readyOperation(t *testing.T, s *Store, task protocol.TaskID, op protocol.OperationID, target string, now time.Time) state.Operation {
	t.Helper()
	if _, ok := s.GetTask(task); !ok {
		if _, err := s.CreateTask(task, now); err != nil {
			t.Fatal(err)
		}
	}
	created, err := s.CreateOperation(desc(task, op, target), now)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := s.MarkReady(op, created.Revision, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return ready
}

func TestUnknownOutcomeRequiresVerificationBeforeRetry(t *testing.T) {
	s := NewStore()
	now := time.Unix(100, 0)
	op := readyOperation(t, s, "tsk_1", "op_1", "target:1", now)

	executing, _, err := s.StartAttempt(op.Descriptor.ID, op.Revision, "att_1", "lease_1", "worker_1", "", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := s.MarkUnknownOutcome(op.Descriptor.ID, executing.Revision, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := s.StartAttempt(op.Descriptor.ID, unknown.Revision, "att_2", "lease_2", "worker_2", "att_1", now.Add(4*time.Second)); !errors.Is(err, state.ErrInvalidTransition) {
		t.Fatalf("direct retry from unknown outcome should fail, got %v", err)
	}

	verifying, err := s.BeginVerification(op.Descriptor.ID, unknown.Revision, now.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	rec := evidence.Record{
		ID:            "ev_1",
		TaskID:        "tsk_1",
		OperationID:   "op_1",
		AttemptID:     "att_1",
		Kind:          evidence.KindTargetObservation,
		SourceActorID: "verifier_1",
		ObservedAt:    now.Add(6 * time.Second),
		SubjectRef:    "target:1",
	}
	if err := s.RegisterEvidence(rec); err != nil {
		t.Fatal(err)
	}
	retryReady, err := s.ApplyVerification(op.Descriptor.ID, verifying.Revision, evidence.Verification{
		ID:                 "ver_1",
		OperationID:        "op_1",
		AttemptID:          "att_1",
		PolicyRef:          "verify:v1",
		EvidenceIDs:        []protocol.EvidenceID{"ev_1"},
		Verdict:            evidence.VerdictNotSatisfiedRetryable,
		RetryExecutionSafe: true,
		VerifiedAt:         now.Add(7 * time.Second),
		VerifierActorID:    "verifier_1",
	}, now.Add(7*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if retryReady.State != state.OperationReady {
		t.Fatalf("expected ready after safe verification, got %s", retryReady.State)
	}
	second, attempt, err := s.StartAttempt(op.Descriptor.ID, retryReady.Revision, "att_2", "lease_2", "worker_2", "att_1", now.Add(8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if second.LeaseGeneration != 2 || attempt.RetryOf != "att_1" {
		t.Fatalf("unexpected retry metadata: op=%+v attempt=%+v", second, attempt)
	}
}

func TestOverlappingMutationScopesAreMutuallyExclusive(t *testing.T) {
	s := NewStore()
	now := time.Unix(100, 0)
	a := readyOperation(t, s, "tsk_1", "op_a", "target:shared", now)
	b := readyOperation(t, s, "tsk_1", "op_b", "target:shared", now)

	if _, _, err := s.StartAttempt(a.Descriptor.ID, a.Revision, "att_a", "lease_a", "worker_a", "", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.StartAttempt(b.Descriptor.ID, b.Revision, "att_b", "lease_b", "worker_b", "", now.Add(time.Second)); !errors.Is(err, ErrConflictScope) {
		t.Fatalf("expected conflict scope error, got %v", err)
	}
}

func TestConcurrentStartsOnSameScopeAllowOnlyOne(t *testing.T) {
	s := NewStore()
	now := time.Unix(100, 0)
	a := readyOperation(t, s, "tsk_1", "op_a", "target:shared", now)
	b := readyOperation(t, s, "tsk_1", "op_b", "target:shared", now)

	type result struct{ err error }
	results := make(chan result, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _, err := s.StartAttempt(a.Descriptor.ID, a.Revision, "att_a", "lease_a", "worker_a", "", now.Add(time.Second))
		results <- result{err}
	}()
	go func() {
		defer wg.Done()
		_, _, err := s.StartAttempt(b.Descriptor.ID, b.Revision, "att_b", "lease_b", "worker_b", "", now.Add(time.Second))
		results <- result{err}
	}()
	wg.Wait()
	close(results)

	var success, conflicts int
	for r := range results {
		switch {
		case r.err == nil:
			success++
		case errors.Is(r.err, ErrConflictScope):
			conflicts++
		default:
			t.Fatalf("unexpected error: %v", r.err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatalf("success=%d conflicts=%d", success, conflicts)
	}
}

func TestDuplicateAttemptIDRejected(t *testing.T) {
	s := NewStore()
	now := time.Unix(100, 0)
	a := readyOperation(t, s, "tsk_1", "op_a", "target:a", now)
	b := readyOperation(t, s, "tsk_1", "op_b", "target:b", now)

	if _, _, err := s.StartAttempt(a.Descriptor.ID, a.Revision, "att_dup", "lease_a", "worker_a", "", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.StartAttempt(b.Descriptor.ID, b.Revision, "att_dup", "lease_b", "worker_b", "", now.Add(time.Second)); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("expected duplicate attempt id, got %v", err)
	}
}

func TestEvidenceScopeAndVerification(t *testing.T) {
	s := NewStore()
	now := time.Unix(100, 0)
	op := readyOperation(t, s, "tsk_1", "op_1", "target:1", now)
	executing, _, err := s.StartAttempt(op.Descriptor.ID, op.Revision, "att_1", "lease_1", "worker_1", "", now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	verifying, err := s.BeginVerification(op.Descriptor.ID, executing.Revision, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	rec := evidence.Record{
		ID:            "ev_1",
		TaskID:        "tsk_1",
		OperationID:   "op_1",
		AttemptID:     "att_1",
		Kind:          evidence.KindTargetObservation,
		SourceActorID: "verifier_1",
		ObservedAt:    now.Add(3 * time.Second),
		SubjectRef:    "target:1",
	}
	if err := s.RegisterEvidence(rec); err != nil {
		t.Fatal(err)
	}
	done, err := s.ApplyVerification("op_1", verifying.Revision, evidence.Verification{
		ID:              "ver_1",
		OperationID:     "op_1",
		AttemptID:       "att_1",
		PolicyRef:       "verify:v1",
		EvidenceIDs:     []protocol.EvidenceID{"ev_1"},
		Verdict:         evidence.VerdictSatisfied,
		VerifiedAt:      now.Add(4 * time.Second),
		VerifierActorID: "verifier_1",
	}, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if done.State != state.OperationSucceeded {
		t.Fatalf("expected succeeded, got %s", done.State)
	}
	if got := s.EvidenceForOperation("op_1"); len(got) != 1 || got[0].ID != "ev_1" {
		t.Fatalf("unexpected evidence: %+v", got)
	}
}

func TestConfirmedNotStartedReleasesConflictReservation(t *testing.T) {
	s := NewStore()
	now := time.Unix(100, 0)
	a := readyOperation(t, s, "tsk_1", "op_a", "target:shared", now)
	b := readyOperation(t, s, "tsk_1", "op_b", "target:shared", now)

	executing, _, err := s.StartAttempt(a.Descriptor.ID, a.Revision, "att_a", "lease_a", "worker_a", "", now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.StartAttempt(b.Descriptor.ID, b.Revision, "att_b", "lease_b", "worker_b", "", now.Add(2*time.Second)); !errors.Is(err, ErrConflictScope) {
		t.Fatalf("expected conflict before proof, got %v", err)
	}

	proof := evidence.Record{
		ID:            "ev_not_started",
		TaskID:        "tsk_1",
		OperationID:   "op_a",
		AttemptID:     "att_a",
		Kind:          evidence.KindProcessObservation,
		SourceActorID: "verifier_1",
		ObservedAt:    now.Add(3 * time.Second),
		SubjectRef:    "worker_a",
		Summary:       "executor confirmed not started",
	}
	if err := s.RegisterEvidence(proof); err != nil {
		t.Fatal(err)
	}
	readyAgain, err := s.MarkNotStarted("op_a", executing.Revision, evidence.Verification{
		ID:                 "ver_not_started",
		OperationID:        "op_a",
		AttemptID:          "att_a",
		PolicyRef:          "verify:v1",
		EvidenceIDs:        []protocol.EvidenceID{proof.ID},
		Verdict:            evidence.VerdictNotSatisfiedRetryable,
		RetryExecutionSafe: true,
		VerifiedAt:         now.Add(4 * time.Second),
		VerifierActorID:    "verifier_1",
	}, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if readyAgain.State != state.OperationReady {
		t.Fatalf("expected ready, got %s", readyAgain.State)
	}
	if _, _, err := s.StartAttempt(b.Descriptor.ID, b.Revision, "att_b", "lease_b", "worker_b", "", now.Add(5*time.Second)); err != nil {
		t.Fatalf("conflict should be released after proof: %v", err)
	}
}

func testEnvelope(messageID protocol.MessageID) protocol.Envelope {
	return protocol.Envelope{
		ProtocolVersion: protocol.ProtocolVersion,
		MessageID:       messageID,
		MessageType:     protocol.MessageCommand,
		MessageName:     "operation.offer",
		TaskID:          "tsk_1",
		OperationID:     "op_1",
		AttemptID:       "att_1",
		CorrelationID:   "corr_1",
		Actor:           protocol.Actor{ID: "controller_1", Role: "controller"},
		IssuedAt:        time.Unix(100, 0),
		Payload:         map[string]any{"intent": "bounded"},
	}
}

func TestMessageDeduplicationAndCollision(t *testing.T) {
	s := NewStore()
	env := testEnvelope("msg_1")
	duplicate, err := s.RegisterMessage(env)
	if err != nil || duplicate {
		t.Fatalf("first registration duplicate=%v err=%v", duplicate, err)
	}

	env.Payload["intent"] = "mutated-after-registration"
	replay := testEnvelope("msg_1")
	duplicate, err = s.RegisterMessage(replay)
	if err != nil || !duplicate {
		t.Fatalf("same message replay duplicate=%v err=%v", duplicate, err)
	}

	collision := testEnvelope("msg_1")
	collision.Payload["intent"] = "different"
	if _, err := s.RegisterMessage(collision); !errors.Is(err, ErrMessageIDCollision) {
		t.Fatalf("expected message-id collision, got %v", err)
	}
}


func TestMarkNotStartedRejectsUnverifiedEvidence(t *testing.T) {
	s := NewStore()
	now := time.Unix(100, 0)
	op := readyOperation(t, s, "tsk_1", "op_1", "target:1", now)
	executing, _, err := s.StartAttempt(op.Descriptor.ID, op.Revision, "att_1", "lease_1", "worker_1", "", now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	rec := evidence.Record{
		ID:            "ev_1",
		TaskID:        "tsk_1",
		OperationID:   "op_1",
		AttemptID:     "att_1",
		Kind:          evidence.KindWorkerReport,
		SourceActorID: "worker_1",
		ObservedAt:    now.Add(2 * time.Second),
		SubjectRef:    "target:1",
	}
	if err := s.RegisterEvidence(rec); err != nil {
		t.Fatal(err)
	}
	_, err = s.MarkNotStarted("op_1", executing.Revision, evidence.Verification{
		ID:              "ver_1",
		OperationID:     "op_1",
		AttemptID:       "att_1",
		PolicyRef:       "verify:v1",
		EvidenceIDs:     []protocol.EvidenceID{"ev_1"},
		Verdict:         evidence.VerdictInconclusive,
		VerifiedAt:      now.Add(3 * time.Second),
		VerifierActorID: "verifier_1",
	}, now.Add(3*time.Second))
	if err == nil {
		t.Fatal("inconclusive verification must not release execution conflict")
	}
}

func TestActiveAttemptContextRejectsStaleOwnership(t *testing.T) {
	s := NewStore()
	now := time.Unix(100, 0)
	op := readyOperation(t, s, "tsk_1", "op_1", "target:1", now)
	executing, attempt, err := s.StartAttempt(op.Descriptor.ID, op.Revision, "att_1", "lease_1", "worker_1", "", now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CheckActiveAttemptContext("op_1", attempt.ID, attempt.LeaseID, executing.LeaseGeneration, "worker_1"); err != nil {
		t.Fatalf("active context rejected: %v", err)
	}
	if err := s.CheckActiveAttemptContext("op_1", attempt.ID, attempt.LeaseID, executing.LeaseGeneration-1, "worker_1"); !errors.Is(err, ErrStaleOwnership) {
		t.Fatalf("expected stale generation rejection, got %v", err)
	}
	if err := s.CheckActiveAttemptContext("op_1", attempt.ID, "lease_stale", executing.LeaseGeneration, "worker_1"); !errors.Is(err, ErrStaleOwnership) {
		t.Fatalf("expected stale lease rejection, got %v", err)
	}
	if err := s.CheckActiveAttemptContext("op_1", attempt.ID, attempt.LeaseID, executing.LeaseGeneration, "worker_stale"); !errors.Is(err, ErrStaleOwnership) {
		t.Fatalf("expected stale owner rejection, got %v", err)
	}
}

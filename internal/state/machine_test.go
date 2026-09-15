package state

import (
	"errors"
	"testing"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

func testOperation(s OperationState, rev uint64) Operation {
	return Operation{
		Descriptor: protocol.OperationDescriptor{
			ID:                     "op_1",
			TaskID:                 "tsk_1",
			Name:                   "target.update",
			TargetRef:              "target:1",
			EffectClass:            protocol.EffectMutation,
			ConflictScope:          []string{"target:1"},
			IdempotencyMode:        protocol.IdempotencyUnknown,
			VerificationPolicyRef:  "verify:v1",
			AuthorizationPolicyRef: "auth:v1",
		},
		State:     s,
		Revision:  rev,
		CreatedAt: time.Unix(1, 0),
		UpdatedAt: time.Unix(1, 0),
	}
}

func TestUnknownOutcomeCannotRetryDirectly(t *testing.T) {
	op := testOperation(OperationUnknownOutcome, 4)
	_, err := Transition(op, 4, OperationReady, time.Unix(2, 0))
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected invalid transition, got %v", err)
	}
	_, err = Transition(op, 4, OperationExecuting, time.Unix(2, 0))
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected invalid transition, got %v", err)
	}
}

func TestTransitionUsesRevisionCAS(t *testing.T) {
	op := testOperation(OperationPlanned, 3)
	_, err := Transition(op, 2, OperationReady, time.Unix(2, 0))
	if !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("expected revision conflict, got %v", err)
	}
}

func TestStartAttemptIncrementsLeaseGeneration(t *testing.T) {
	op := testOperation(OperationReady, 2)
	op.LeaseGeneration = 7
	next, attempt, err := StartAttempt(op, 2, "att_1", time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	if next.LeaseGeneration != 8 || attempt.LeaseGeneration != 8 {
		t.Fatalf("lease generation not incremented: op=%d attempt=%d", next.LeaseGeneration, attempt.LeaseGeneration)
	}
	if next.ActiveAttemptID != "att_1" || next.State != OperationExecuting {
		t.Fatalf("unexpected operation state: %+v", next)
	}
}

package protocol

import (
	"errors"
	"testing"
	"time"
)

func validDescriptor() OperationDescriptor {
	return OperationDescriptor{
		ID:                     "op_1",
		TaskID:                 "tsk_1",
		Name:                   "target.update",
		TargetRef:              "target:1",
		EffectClass:            EffectMutation,
		ConflictScope:          []string{"target:1"},
		IdempotencyMode:        IdempotencyUnknown,
		VerificationPolicyRef:  "verify:v1",
		AuthorizationPolicyRef: "auth:v1",
	}
}

func TestOperationDescriptorValidation(t *testing.T) {
	if err := validDescriptor().Validate(); err != nil {
		t.Fatalf("valid descriptor rejected: %v", err)
	}
	d := validDescriptor()
	d.ConflictScope = nil
	if err := d.Validate(); !errors.Is(err, ErrInvalidDescriptor) {
		t.Fatalf("mutation without conflict scope: got %v", err)
	}
}

func TestEnvelopeRejectsSensitiveFields(t *testing.T) {
	env := Envelope{
		ProtocolVersion: ProtocolVersion,
		MessageID:       "msg_1",
		MessageType:     MessageCommand,
		MessageName:     "operation.offer",
		TaskID:          "tsk_1",
		OperationID:     "op_1",
		CorrelationID:   "corr_1",
		Actor:            Actor{ID: "actor_1", Role: "controller"},
		IssuedAt:        time.Now(),
		Payload: map[string]any{
			"nested": map[string]any{"api_token": "should-never-be-here"},
		},
	}
	if err := env.Validate(); !errors.Is(err, ErrSensitiveField) {
		t.Fatalf("expected sensitive-field rejection, got %v", err)
	}

	env.Payload = map[string]any{"secret_ref": "vault:item"}
	if err := env.Validate(); err != nil {
		t.Fatalf("opaque secret_ref should be allowed: %v", err)
	}
}

func TestEnvelopeRejectsVersionMismatch(t *testing.T) {
	env := Envelope{
		ProtocolVersion: "2.0",
		MessageID:       "msg_1",
		MessageType:     MessageCommand,
		MessageName:     "operation.offer",
		TaskID:          "tsk_1",
		OperationID:     "op_1",
		CorrelationID:   "corr_1",
		Actor:            Actor{ID: "actor_1", Role: "controller"},
		IssuedAt:        time.Now(),
	}
	if err := env.Validate(); !errors.Is(err, ErrInvalidProtocolVersion) {
		t.Fatalf("expected version error, got %v", err)
	}
}

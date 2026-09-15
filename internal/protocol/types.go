package protocol

import "time"

const ProtocolVersion = "1.0"

type TaskID string
type OperationID string
type AttemptID string
type MessageID string
type LeaseID string
type EvidenceID string
type ActorID string
type CorrelationID string

type EffectClass string

const (
	EffectReadOnly EffectClass = "read_only"
	EffectMutation EffectClass = "mutation"
)

type IdempotencyMode string

const (
	IdempotencyUnknown     IdempotencyMode = "unknown"
	IdempotencyIdempotent  IdempotencyMode = "idempotent"
	IdempotencyConditional IdempotencyMode = "conditional"
)

type MessageType string

const (
	MessageCommand          MessageType = "command"
	MessageEvent            MessageType = "event"
	MessageObservation      MessageType = "observation"
	MessageDecisionRequest  MessageType = "decision_request"
	MessageDecisionResponse MessageType = "decision_response"
)

type Actor struct {
	ID   ActorID `json:"actor_id"`
	Role string  `json:"actor_role"`
}

type LeaseRef struct {
	ID         LeaseID `json:"lease_id"`
	Generation uint64  `json:"generation"`
}

type Envelope struct {
	ProtocolVersion string         `json:"protocol_version"`
	MessageID       MessageID      `json:"message_id"`
	MessageType     MessageType    `json:"message_type"`
	MessageName     string         `json:"message_name"`
	TaskID          TaskID         `json:"task_id"`
	OperationID     OperationID    `json:"operation_id,omitempty"`
	AttemptID       AttemptID      `json:"attempt_id,omitempty"`
	CorrelationID   CorrelationID  `json:"correlation_id"`
	CausationID     MessageID      `json:"causation_id,omitempty"`
	Actor           Actor          `json:"actor"`
	IssuedAt        time.Time      `json:"issued_at"`
	DeadlineAt      *time.Time     `json:"deadline_at,omitempty"`
	Lease           *LeaseRef      `json:"lease,omitempty"`
	Payload         map[string]any `json:"payload,omitempty"`
	Extensions      map[string]any `json:"extensions,omitempty"`
}

type OperationDescriptor struct {
	ID                     OperationID     `json:"operation_id"`
	TaskID                 TaskID          `json:"task_id"`
	Name                   string          `json:"operation_name"`
	TargetRef              string          `json:"target_ref"`
	EffectClass            EffectClass     `json:"effect_class"`
	ConflictScope          []string        `json:"conflict_scope,omitempty"`
	IdempotencyMode        IdempotencyMode `json:"idempotency_mode"`
	VerificationPolicyRef  string          `json:"verification_policy_ref"`
	AuthorizationPolicyRef string          `json:"authorization_policy_ref"`
}

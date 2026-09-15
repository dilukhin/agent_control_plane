package state

import (
	"time"

	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

type TaskState string

const (
	TaskPlanned          TaskState = "planned"
	TaskActive           TaskState = "active"
	TaskBlocked          TaskState = "blocked"
	TaskAwaitingDecision TaskState = "awaiting_decision"
	TaskCompleted        TaskState = "completed"
	TaskFailed           TaskState = "failed"
	TaskCancelled        TaskState = "cancelled"
)

type Task struct {
	ID        protocol.TaskID
	State     TaskState
	Revision  uint64
	CreatedAt time.Time
	UpdatedAt time.Time
}

type OperationState string

const (
	OperationPlanned        OperationState = "planned"
	OperationReady          OperationState = "ready"
	OperationExecuting      OperationState = "executing"
	OperationVerifying      OperationState = "verifying"
	OperationUnknownOutcome OperationState = "unknown_outcome"
	OperationSucceeded      OperationState = "succeeded"
	OperationFailed         OperationState = "failed"
	OperationCancelled      OperationState = "cancelled"
)

type AttemptState string

const (
	AttemptCreated    AttemptState = "created"
	AttemptRunning    AttemptState = "running"
	AttemptReported   AttemptState = "reported"
	AttemptRejected   AttemptState = "rejected"
	AttemptNotStarted AttemptState = "not_started"
	AttemptUncertain  AttemptState = "uncertain"
	AttemptClosed     AttemptState = "closed"
)

type Attempt struct {
	ID              protocol.AttemptID
	OperationID     protocol.OperationID
	LeaseID         protocol.LeaseID
	LeaseGeneration uint64
	OwnerActorID    protocol.ActorID
	RetryOf         protocol.AttemptID
	State           AttemptState
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type Operation struct {
	Descriptor      protocol.OperationDescriptor
	State           OperationState
	Revision        uint64
	LeaseGeneration uint64
	ActiveAttemptID protocol.AttemptID
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func (o Operation) HoldsConflictReservation() bool {
	if o.Descriptor.EffectClass != protocol.EffectMutation {
		return false
	}
	switch o.State {
	case OperationExecuting, OperationVerifying, OperationUnknownOutcome:
		return true
	default:
		return false
	}
}

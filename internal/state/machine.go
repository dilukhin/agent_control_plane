package state

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

var (
	ErrRevisionConflict  = errors.New("state revision conflict")
	ErrInvalidTransition = errors.New("invalid state transition")
)

var allowedTransitions = map[OperationState]map[OperationState]struct{}{
	OperationPlanned: {
		OperationReady:     {},
		OperationCancelled: {},
	},
	OperationReady: {
		OperationExecuting: {},
		OperationCancelled: {},
	},
	OperationExecuting: {
		OperationVerifying:      {},
		OperationReady:          {},
		OperationUnknownOutcome: {},
	},
	OperationVerifying: {
		OperationSucceeded:      {},
		OperationReady:          {},
		OperationFailed:         {},
		OperationUnknownOutcome: {},
	},
	OperationUnknownOutcome: {
		OperationVerifying: {},
	},
}

func Transition(op Operation, expectedRevision uint64, to OperationState, now time.Time) (Operation, error) {
	if op.Revision != expectedRevision {
		return Operation{}, fmt.Errorf("%w: got %d want %d", ErrRevisionConflict, expectedRevision, op.Revision)
	}
	if op.Revision == 0 || op.Revision >= math.MaxInt64 {
		return Operation{}, errors.New("revision exhausted or invalid")
	}
	if now.IsZero() {
		return Operation{}, errors.New("transition time is required")
	}
	if _, ok := allowedTransitions[op.State][to]; !ok {
		return Operation{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, op.State, to)
	}
	op.State = to
	op.Revision++
	op.UpdatedAt = now
	switch to {
	case OperationReady, OperationSucceeded, OperationFailed, OperationCancelled:
		op.ActiveAttemptID = ""
	}
	return op, nil
}

func StartAttempt(op Operation, expectedRevision uint64, attemptID protocol.AttemptID, now time.Time) (Operation, Attempt, error) {
	if attemptID == "" {
		return Operation{}, Attempt{}, errors.New("attempt id is required")
	}
	next, err := Transition(op, expectedRevision, OperationExecuting, now)
	if err != nil {
		return Operation{}, Attempt{}, err
	}
	if next.LeaseGeneration >= math.MaxInt64 {
		return Operation{}, Attempt{}, errors.New("lease generation exhausted")
	}
	next.LeaseGeneration++
	next.ActiveAttemptID = attemptID
	attempt := Attempt{
		ID:              attemptID,
		OperationID:     op.Descriptor.ID,
		LeaseGeneration: next.LeaseGeneration,
		State:           AttemptRunning,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	return next, attempt, nil
}

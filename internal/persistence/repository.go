// Package persistence defines the transaction boundary, not orchestration policy.
package persistence

import (
	"context"
	"errors"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/evidence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

var (
	ErrNotFound       = errors.New("record not found")
	ErrDuplicateID    = errors.New("duplicate identity")
	ErrConflictScope  = errors.New("mutation conflict scope is reserved")
	ErrClosed         = errors.New("repository closed")
	ErrExpiredMessage = errors.New("message outside accepted replay window")
	ErrTaskTerminal   = errors.New("task is terminal")
	ErrIntegrity      = errors.New("persistence integrity error")
)

type Message struct {
	ID          protocol.MessageID
	TaskID      protocol.TaskID
	OperationID protocol.OperationID
	AttemptID   protocol.AttemptID
	Digest      string
	IssuedAt    time.Time
	ExpiresAt   int64
}

type Reservation struct {
	Scope       string
	OperationID protocol.OperationID
	AttemptID   protocol.AttemptID
	LeaseID     protocol.LeaseID
	Generation  uint64
	CreatedAt   time.Time
}

type Revocation struct {
	AttemptID   protocol.AttemptID
	OperationID protocol.OperationID
	EvidenceID  protocol.EvidenceID
	Reason      string
	RevokedAt   time.Time
}

const MaxPageSize = 100

type TaskSummary struct{ Operations, Pending, Failed, Cancelled int }
type Reader interface {
	MessageFloor() (int64, error)
	TaskSummary(protocol.TaskID) (TaskSummary, error)
	TerminalTasksAfter(protocol.TaskID, int64, int) ([]state.Task, error)
	TaskOperations(protocol.TaskID, int) ([]protocol.OperationID, error)
	OperationRows(protocol.OperationID, int) (int, error)
	ExpiredMessages(protocol.TaskID, bool, int64, int64, int) ([]protocol.MessageID, error)
	TaskHasMessages(protocol.TaskID) (bool, error)
	TaskMessagesSafe(protocol.TaskID, int64, int64) (bool, error)
	OperationsAfter(protocol.OperationID, int) ([]state.Operation, error)
	Revocation(protocol.AttemptID) (Revocation, error)
	Task(protocol.TaskID) (state.Task, error)
	Operation(protocol.OperationID) (state.Operation, error)
	Attempt(protocol.AttemptID) (state.Attempt, error)
	Evidence(protocol.EvidenceID) (evidence.Record, error)
	EvidenceForOperation(protocol.OperationID) ([]evidence.Record, error)
	Verification(string) (evidence.Verification, error)
	Message(protocol.MessageID) (Message, error)
	Reservation(string) (Reservation, error)
}

type Tx interface {
	Reader
	SetMessageFloor(int64) error
	UpdateTask(state.Task, uint64) error
	DeleteOperationGraph(protocol.OperationID) error
	DeleteMessages([]protocol.MessageID) error
	DeleteTask(protocol.TaskID) error
	InsertRevocation(Revocation) error
	InsertTask(state.Task) error
	InsertOperation(state.Operation) error
	UpdateOperation(state.Operation, uint64) error
	InsertAttempt(state.Attempt) error
	UpdateAttempt(state.Attempt) error
	InsertEvidence(evidence.Record) error
	InsertVerification(evidence.Verification) error
	InsertMessage(Message) error
	Reserve(Reservation) error
	Release(protocol.OperationID) error
}

// Callbacks are synchronous, must not retain Reader/Tx, and must not perform
// external side effects or nested repository calls. An error rolls back all
// writes. Implementations never replay callbacks. A commit error is not proof
// of rollback: the caller must read actual state before deciding to retry.
type Repository interface {
	View(context.Context, func(Reader) error) error
	Update(context.Context, func(Tx) error) error
	Close() error
}

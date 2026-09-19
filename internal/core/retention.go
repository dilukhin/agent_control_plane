package core

import (
	"context"
	"errors"
	"math"
	"time"

	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

func terminalTask(s state.TaskState) bool {
	return s == state.TaskCompleted || s == state.TaskFailed || s == state.TaskCancelled
}

// FinalizeTask conservatively treats every operation as required. It does not
// infer completion from the last message, nor close a task with unresolved work.
func (s *Store) FinalizeTask(ctx context.Context, id protocol.TaskID, revision uint64, now time.Time) (state.Task, error) {
	return writeValue(s, ctx, func(t *Transaction) (state.Task, error) {
		task, err := t.tx.Task(id)
		if err != nil {
			return state.Task{}, err
		}
		if task.Revision != revision {
			return state.Task{}, state.ErrRevisionConflict
		}
		if terminalTask(task.State) {
			return state.Task{}, p.ErrTaskTerminal
		}
		if now.IsZero() || revision == 0 || revision >= math.MaxInt64 {
			return state.Task{}, errors.New("invalid task time or revision")
		}
		summary, err := t.tx.TaskSummary(id)
		if err != nil {
			return state.Task{}, err
		}
		if summary.Operations == 0 || summary.Pending > 0 {
			return state.Task{}, errors.New("task still has unresolved work")
		}
		switch {
		case summary.Failed > 0:
			task.State = state.TaskFailed
		case summary.Cancelled == summary.Operations:
			task.State = state.TaskCancelled
		case summary.Cancelled > 0:
			return state.Task{}, errors.New("mixed cancellation needs explicit task policy")
		default:
			task.State = state.TaskCompleted
		}
		task.Revision++
		task.UpdatedAt = now.UTC()
		if err = t.tx.UpdateTask(task, revision); err != nil {
			return state.Task{}, err
		}
		return task, nil
	})
}
func (s *Store) Collect(ctx context.Context, o p.GCOptions) (p.GCResult, error) {
	return p.Collect(ctx, s.repository, o)
}

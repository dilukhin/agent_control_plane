package persistence

import (
	"context"
	"errors"
	"time"

	"github.com/dilukhin/agent_control_plane/internal/protocol"
)

const ReplayWindow = 7 * 24 * time.Hour
const TerminalRetention = 30 * 24 * time.Hour
const MaxGCRows = 10000

type GCOptions struct {
	Now               time.Time
	After             protocol.TaskID
	MaxTasks, MaxRows int
}
type GCResult struct {
	Rows, Operations, Tasks, Messages, DeferredTasks, OversizedOperations int
	Next                                                                  protocol.TaskID
	More                                                                  bool
}

// Collect removes complete operation graphs after their task's retention period.
// It never removes a partial evidence graph. All work in this batch is atomic.
func Collect(ctx context.Context, repo Repository, o GCOptions) (GCResult, error) {
	if o.Now.IsZero() || o.MaxTasks < 1 || o.MaxTasks > MaxPageSize || o.MaxRows < 1 || o.MaxRows > MaxGCRows {
		return GCResult{}, errors.New("invalid GC budget")
	}
	out := GCResult{Next: o.After}
	err := repo.Update(ctx, func(tx Tx) error {
		floor, err := tx.MessageFloor()
		if err != nil {
			return err
		}
		if candidate := o.Now.Add(-ReplayWindow).Unix(); candidate > floor {
			floor = candidate
		}
		if err = tx.SetMessageFloor(floor); err != nil {
			return err
		}
		remaining := o.MaxRows
		pruneMessages := func(id protocol.TaskID, orphan bool) error {
			ids, e := tx.ExpiredMessages(id, orphan, o.Now.Unix(), floor, remaining)
			if e != nil {
				return e
			}
			if e = tx.DeleteMessages(ids); e != nil {
				return e
			}
			remaining -= len(ids)
			out.Messages += len(ids)
			out.Rows += len(ids)
			return nil
		}
		if err = pruneMessages("", true); err != nil {
			return err
		}
		if remaining == 0 {
			out.More = true
			return nil
		}
		tasks, err := tx.TerminalTasksAfter(o.After, o.Now.Add(-TerminalRetention).Unix(), o.MaxTasks)
		if err != nil {
			return err
		}
		for _, task := range tasks {
			summary, e := tx.TaskSummary(task.ID)
			if e != nil {
				return e
			}
			safe, e := tx.TaskMessagesSafe(task.ID, o.Now.Unix(), floor)
			if e != nil {
				return e
			}
			if summary.Pending != 0 || !safe {
				out.DeferredTasks++
				out.Next = task.ID
				continue
			}
			ids, e := tx.TaskOperations(task.ID, o.MaxRows)
			if e != nil {
				return e
			}
			deferred := false
			for _, id := range ids {
				n, e := tx.OperationRows(id, o.MaxRows+1)
				if e != nil {
					return e
				}
				if n > o.MaxRows {
					out.OversizedOperations++
					deferred = true
					continue
				}
				if n > remaining {
					out.More = true
					return nil
				}
				if e = tx.DeleteOperationGraph(id); e != nil {
					return e
				}
				remaining -= n
				out.Rows += n
				out.Operations++
			}
			left, e := tx.TaskSummary(task.ID)
			if e != nil {
				return e
			}
			if left.Operations > 0 {
				if deferred {
					out.DeferredTasks++
					out.Next = task.ID
					continue
				}
				out.More = true
				return nil
			}
			if remaining == 0 {
				out.More = true
				return nil
			}
			if e = pruneMessages(task.ID, false); e != nil {
				return e
			}
			has, e := tx.TaskHasMessages(task.ID)
			if e != nil {
				return e
			}
			if has || remaining == 0 {
				out.More = true
				return nil
			}
			if e = tx.DeleteTask(task.ID); e != nil {
				return e
			}
			remaining--
			out.Rows++
			out.Tasks++
			out.Next = task.ID
			if remaining == 0 {
				out.More = true
				return nil
			}
		}
		out.More = len(tasks) == o.MaxTasks
		if !out.More {
			out.Next = ""
		}
		return nil
	})
	if err != nil {
		return GCResult{}, err
	}
	return out, nil
}

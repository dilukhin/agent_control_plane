package memory

import (
	"slices"

	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

func (t *transaction) MessageFloor() (int64, error) { return t.d.messageFloor, nil }
func (t *transaction) SetMessageFloor(v int64) error {
	if !t.write {
		return p.ErrIntegrity
	}
	if v > t.d.messageFloor {
		t.d.messageFloor = v
	}
	return nil
}
func terminal(s state.TaskState) bool {
	return s == state.TaskCompleted || s == state.TaskFailed || s == state.TaskCancelled
}
func (t *transaction) UpdateTask(v state.Task, revision uint64) error {
	if !t.write {
		return p.ErrIntegrity
	}
	old, e := t.Task(v.ID)
	if e != nil {
		return e
	}
	if old.Revision != revision {
		return state.ErrRevisionConflict
	}
	if v.Revision != revision+1 {
		return p.ErrIntegrity
	}
	t.d.tasks[v.ID] = v
	return nil
}
func (t *transaction) TaskSummary(id protocol.TaskID) (p.TaskSummary, error) {
	var out p.TaskSummary
	for _, op := range t.d.ops {
		if op.Descriptor.TaskID != id {
			continue
		}
		out.Operations++
		switch op.State {
		case state.OperationSucceeded:
		case state.OperationFailed:
			out.Failed++
		case state.OperationCancelled:
			out.Cancelled++
		default:
			out.Pending++
		}
		for _, v := range t.d.reservations {
			if v.OperationID == op.Descriptor.ID {
				out.Pending++
				break
			}
		}
	}
	return out, nil
}
func (t *transaction) TerminalTasksAfter(after protocol.TaskID, cutoff int64, limit int) ([]state.Task, error) {
	if limit < 1 || limit > p.MaxPageSize {
		return nil, p.ErrIntegrity
	}
	ids := []protocol.TaskID{}
	for id, v := range t.d.tasks {
		if id > after && terminal(v.State) && v.UpdatedAt.Unix() < cutoff {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]state.Task, 0, len(ids))
	for _, id := range ids {
		out = append(out, t.d.tasks[id])
	}
	return out, nil
}
func (t *transaction) TaskOperations(id protocol.TaskID, limit int) ([]protocol.OperationID, error) {
	if limit < 1 || limit > p.MaxGCRows {
		return nil, p.ErrIntegrity
	}
	out := []protocol.OperationID{}
	for key, op := range t.d.ops {
		if op.Descriptor.TaskID == id {
			out = append(out, key)
		}
	}
	slices.Sort(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (t *transaction) OperationRows(id protocol.OperationID, limit int) (int, error) {
	op, e := t.Operation(id)
	if e != nil {
		return 0, e
	}
	n := 1 + len(op.Descriptor.ConflictScope)
	for _, v := range t.d.attempts {
		if v.OperationID == id {
			n++
		}
	}
	for _, v := range t.d.evidence {
		if v.OperationID == id {
			n++
		}
	}
	for _, v := range t.d.verifications {
		if v.OperationID == id {
			n += 1 + len(v.EvidenceIDs)
		}
	}
	for _, v := range t.d.revocations {
		if v.OperationID == id {
			n++
		}
	}
	return min(n, limit), nil
}
func (t *transaction) ExpiredMessages(id protocol.TaskID, orphan bool, now, floor int64, limit int) ([]protocol.MessageID, error) {
	if limit < 1 || limit > p.MaxGCRows {
		return nil, p.ErrIntegrity
	}
	out := []protocol.MessageID{}
	for key, v := range t.d.messages {
		_, exists := t.d.tasks[v.TaskID]
		matches := v.TaskID == id
		if orphan {
			_, hasOp := t.d.ops[v.OperationID]
			_, hasAttempt := t.d.attempts[v.AttemptID]
			matches = !exists && !hasOp && !hasAttempt
		}
		if matches && v.ExpiresAt < now && v.IssuedAt.Unix() < floor {
			out = append(out, key)
		}
	}
	slices.Sort(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (t *transaction) TaskHasMessages(id protocol.TaskID) (bool, error) {
	for _, v := range t.d.messages {
		if v.TaskID == id {
			return true, nil
		}
	}
	return false, nil
}
func (t *transaction) TaskMessagesSafe(id protocol.TaskID, now, floor int64) (bool, error) {
	for _, v := range t.d.messages {
		op, hasOp := t.d.ops[v.OperationID]
		attempt, hasAttempt := t.d.attempts[v.AttemptID]
		attemptOp := t.d.ops[attempt.OperationID]
		if v.TaskID == id && (v.ExpiresAt >= now || v.IssuedAt.Unix() >= floor || (hasOp && op.Descriptor.TaskID != id) || (hasAttempt && attemptOp.Descriptor.TaskID != id)) {
			return false, nil
		}
	}
	return true, nil
}
func (t *transaction) DeleteOperationGraph(id protocol.OperationID) error {
	if !t.write {
		return p.ErrIntegrity
	}
	for _, v := range t.d.reservations {
		if v.OperationID == id {
			return p.ErrIntegrity
		}
	}
	for k, v := range t.d.revocations {
		if v.OperationID == id {
			delete(t.d.revocations, k)
		}
	}
	for k, v := range t.d.verifications {
		if v.OperationID == id {
			delete(t.d.verifications, k)
		}
	}
	for k, v := range t.d.evidence {
		if v.OperationID == id {
			delete(t.d.evidence, k)
		}
	}
	for k, v := range t.d.attempts {
		if v.OperationID == id {
			delete(t.d.attempts, k)
		}
	}
	delete(t.d.ops, id)
	return nil
}
func (t *transaction) DeleteMessages(ids []protocol.MessageID) error {
	if !t.write {
		return p.ErrIntegrity
	}
	for _, id := range ids {
		delete(t.d.messages, id)
	}
	return nil
}
func (t *transaction) DeleteTask(id protocol.TaskID) error {
	if !t.write {
		return p.ErrIntegrity
	}
	summary, e := t.TaskSummary(id)
	if e != nil {
		return e
	}
	has, e := t.TaskHasMessages(id)
	if e != nil {
		return e
	}
	if summary.Operations != 0 || has {
		return p.ErrIntegrity
	}
	delete(t.d.tasks, id)
	return nil
}

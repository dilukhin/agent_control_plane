package sqlite

import (
	"context"
	"fmt"

	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

func (t *transaction) MessageFloor() (int64, error) {
	var v int64
	e := t.conn.QueryRowContext(t.ctx, "SELECT message_floor FROM retention_state WHERE singleton=1").Scan(&v)
	return v, e
}
func (t *transaction) SetMessageFloor(v int64) error {
	_, e := t.exec("UPDATE retention_state SET message_floor=max(message_floor,?) WHERE singleton=1", v)
	return e
}
func (t *transaction) UpdateTask(v state.Task, revision uint64) error {
	if v.Revision != revision+1 {
		return p.ErrIntegrity
	}
	result, e := t.exec("UPDATE tasks SET state=?,revision=?,updated_at=? WHERE id=? AND revision=?", v.State, v.Revision, timestamp(v.UpdatedAt), v.ID, revision)
	if e != nil {
		return e
	}
	n, e := result.RowsAffected()
	if e != nil {
		return e
	}
	if n != 1 {
		return state.ErrRevisionConflict
	}
	return nil
}
func (t *transaction) TaskSummary(id protocol.TaskID) (p.TaskSummary, error) {
	var v p.TaskSummary
	e := t.conn.QueryRowContext(t.ctx, `SELECT count(*),coalesce(sum(state NOT IN ('succeeded','failed','cancelled') OR EXISTS(SELECT 1 FROM reservations r WHERE r.operation_id=o.id)),0),coalesce(sum(state='failed'),0),coalesce(sum(state='cancelled'),0) FROM operations o WHERE task_id=?`, id).Scan(&v.Operations, &v.Pending, &v.Failed, &v.Cancelled)
	return v, e
}
func (t *transaction) TerminalTasksAfter(after protocol.TaskID, cutoff int64, limit int) ([]state.Task, error) {
	if limit < 1 || limit > p.MaxPageSize {
		return nil, p.ErrIntegrity
	}
	rows, e := t.conn.QueryContext(t.ctx, "SELECT "+taskColumns+" FROM tasks WHERE id>? AND state IN ('completed','failed','cancelled') AND CAST(strftime('%s',updated_at) AS INTEGER)<? ORDER BY id LIMIT ?", after, cutoff, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []state.Task{}
	for rows.Next() {
		v, e := scanTask(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (t *transaction) TaskOperations(id protocol.TaskID, limit int) ([]protocol.OperationID, error) {
	if limit < 1 || limit > p.MaxGCRows {
		return nil, p.ErrIntegrity
	}
	rows, e := t.conn.QueryContext(t.ctx, "SELECT id FROM operations WHERE task_id=? ORDER BY id LIMIT ?", id, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []protocol.OperationID{}
	for rows.Next() {
		var v protocol.OperationID
		if e = rows.Scan(&v); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (t *transaction) OperationRows(id protocol.OperationID, limit int) (int, error) {
	if limit < 1 || limit > p.MaxGCRows+1 {
		return 0, p.ErrIntegrity
	}
	var n int
	e := t.conn.QueryRowContext(t.ctx, `SELECT count(*) FROM (
 SELECT 1 FROM operations WHERE id=? UNION ALL SELECT 1 FROM operation_scopes WHERE operation_id=?
 UNION ALL SELECT 1 FROM attempts WHERE operation_id=? UNION ALL SELECT 1 FROM evidence WHERE operation_id=?
 UNION ALL SELECT 1 FROM verifications WHERE operation_id=? UNION ALL SELECT 1 FROM verification_evidence WHERE operation_id=?
 UNION ALL SELECT 1 FROM revocations WHERE operation_id=? LIMIT ?)`, id, id, id, id, id, id, id, limit).Scan(&n)
	return n, e
}
func (t *transaction) ExpiredMessages(id protocol.TaskID, orphan bool, now, floor int64, limit int) ([]protocol.MessageID, error) {
	if limit < 1 || limit > p.MaxGCRows {
		return nil, p.ErrIntegrity
	}
	filter := "task_id=?"
	args := []any{id, now, floor, limit}
	if orphan {
		filter = "NOT EXISTS(SELECT 1 FROM tasks t WHERE t.id=messages.task_id) AND NOT EXISTS(SELECT 1 FROM operations o WHERE o.id=messages.operation_id) AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.id=messages.attempt_id)"
		args = []any{now, floor, limit}
	}
	rows, e := t.conn.QueryContext(t.ctx, "SELECT id FROM messages WHERE "+filter+" AND expires_at<? AND CAST(strftime('%s',issued_at) AS INTEGER)<? ORDER BY id LIMIT ?", args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []protocol.MessageID{}
	for rows.Next() {
		var v protocol.MessageID
		if e = rows.Scan(&v); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (t *transaction) TaskHasMessages(id protocol.TaskID) (bool, error) {
	var v bool
	e := t.conn.QueryRowContext(t.ctx, "SELECT EXISTS(SELECT 1 FROM messages WHERE task_id=?)", id).Scan(&v)
	return v, e
}
func (t *transaction) TaskMessagesSafe(id protocol.TaskID, now, floor int64) (bool, error) {
	var v bool
	e := t.conn.QueryRowContext(t.ctx, "SELECT NOT EXISTS(SELECT 1 FROM messages WHERE task_id=? AND (expires_at>=? OR CAST(strftime('%s',issued_at) AS INTEGER)>=? OR EXISTS(SELECT 1 FROM operations o WHERE o.id=messages.operation_id AND o.task_id<>messages.task_id) OR EXISTS(SELECT 1 FROM attempts a JOIN operations o ON o.id=a.operation_id WHERE a.id=messages.attempt_id AND o.task_id<>messages.task_id)))", id, now, floor).Scan(&v)
	return v, e
}
func (t *transaction) DeleteOperationGraph(id protocol.OperationID) error {
	// Called only after eligibility and row-budget checks in the same write tx.
	for _, table := range []string{"revocations", "verification_evidence", "verifications", "evidence", "operation_scopes", "attempts"} {
		if _, e := t.exec("DELETE FROM "+table+" WHERE operation_id=?", id); e != nil {
			return e
		}
	}
	_, e := t.exec("DELETE FROM operations WHERE id=?", id)
	return e
}
func (t *transaction) DeleteMessages(ids []protocol.MessageID) error {
	for _, id := range ids {
		if _, e := t.exec("DELETE FROM messages WHERE id=?", id); e != nil {
			return e
		}
	}
	return nil
}
func (t *transaction) DeleteTask(id protocol.TaskID) error {
	_, e := t.exec("DELETE FROM tasks WHERE id=?", id)
	return e
}

// Maintain is explicit, outside the execution path. Free at most pages pages;
// WAL checkpointing remains SQLite's normal mechanism, not a full VACUUM.
func (r *Repository) Maintain(ctx context.Context, pages int) error {
	if pages < 1 || pages > 1024 {
		return p.ErrIntegrity
	}
	return r.run(ctx, true, func(t *transaction) error {
		rows, e := t.conn.QueryContext(t.ctx, fmt.Sprintf("PRAGMA incremental_vacuum(%d)", pages))
		if e != nil {
			return e
		}
		defer rows.Close()
		for rows.Next() {
		}
		return rows.Err()
	})
}

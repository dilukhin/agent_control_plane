package sqlite

import (
	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
	"time"
)

func (t *transaction) OperationsAfter(after protocol.OperationID, limit int) ([]state.Operation, error) {
	if limit < 1 || limit > p.MaxPageSize {
		return nil, p.ErrIntegrity
	}
	rows, err := t.conn.QueryContext(t.ctx, "SELECT id FROM operations WHERE id>? ORDER BY id LIMIT ?", after, limit)
	if err != nil {
		return nil, err
	}
	ids := []protocol.OperationID{}
	for rows.Next() {
		var id protocol.OperationID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := make([]state.Operation, 0, len(ids))
	for _, id := range ids {
		v, e := t.Operation(id)
		if e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, nil
}
func (t *transaction) Revocation(id protocol.AttemptID) (p.Revocation, error) {
	var v p.Revocation
	var at string
	err := t.conn.QueryRowContext(t.ctx, "SELECT attempt_id,operation_id,evidence_id,reason,revoked_at FROM revocations WHERE attempt_id=?", id).Scan(&v.AttemptID, &v.OperationID, &v.EvidenceID, &v.Reason, &at)
	if err != nil {
		return v, dbError(err)
	}
	v.RevokedAt, err = time.Parse(time.RFC3339Nano, at)
	return v, err
}
func (t *transaction) InsertRevocation(v p.Revocation) error {
	_, err := t.exec("INSERT INTO revocations VALUES(?,?,?,?,?)", v.AttemptID, v.OperationID, v.EvidenceID, v.Reason, timestamp(v.RevokedAt))
	return err
}

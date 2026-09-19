// Package sqlite implements local durable storage. Use only a local filesystem,
// never a network share; provision a private parent directory on both platforms.
package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	modernc "modernc.org/sqlite"
)

type Options struct {
	BusyTimeout        time.Duration
	TransactionTimeout time.Duration
}
type Repository struct {
	db      *sql.DB
	timeout time.Duration
	busy    time.Duration
	closed  atomic.Bool
}

func Open(ctx context.Context, path string, opts Options) (*Repository, error) {
	if path == "" || path == ":memory:" {
		return nil, errors.New("a local database file is required")
	}
	if opts.BusyTimeout == 0 {
		opts.BusyTimeout = 5 * time.Second
	}
	if opts.TransactionTimeout == 0 {
		opts.TransactionTimeout = 10 * time.Second
	}
	if opts.BusyTimeout < time.Millisecond || opts.BusyTimeout > 30*time.Second || opts.TransactionTimeout < time.Millisecond || opts.TransactionTimeout > 30*time.Second {
		return nil, errors.New("timeouts must be between 1ms and 30s")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(abs, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err == nil {
		err = f.Close()
	} else if errors.Is(err, os.ErrExist) {
		info, e := os.Lstat(abs)
		err = e
		if e == nil && !info.Mode().IsRegular() {
			err = errors.New("database must be a regular file")
		}
	}
	if err != nil {
		return nil, err
	}
	uriPath := filepath.ToSlash(abs)
	if !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	u := url.URL{Scheme: "file", Path: uriPath}
	q := url.Values{}
	for _, v := range []string{"foreign_keys(1)", "synchronous(FULL)", fmt.Sprintf("busy_timeout(%d)", opts.BusyTimeout.Milliseconds())} {
		q.Add("_pragma", v)
	}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	r := &Repository{db: db, timeout: opts.TransactionTimeout, busy: opts.BusyTimeout}
	if err = r.initialize(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return r, nil
}
func (r *Repository) Close() error { r.closed.Store(true); return r.db.Close() }
func (r *Repository) View(ctx context.Context, fn func(p.Reader) error) error {
	return r.run(ctx, false, func(t *transaction) error { return fn(t) })
}
func (r *Repository) Update(ctx context.Context, fn func(p.Tx) error) error {
	return r.run(ctx, true, func(t *transaction) error { return fn(t) })
}
func (r *Repository) run(ctx context.Context, write bool, fn func(*transaction) error) error {
	if r.closed.Load() {
		return p.ErrClosed
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err = r.configure(ctx, conn); err != nil {
		return err
	}
	begin := "BEGIN"
	if write {
		begin = "BEGIN IMMEDIATE"
	}
	if _, err = conn.ExecContext(ctx, begin); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return dbError(err)
	}
	committed := false
	defer func() {
		if !committed {
			rollback(conn)
		}
	}()
	version, err := validateSchema(ctx, conn)
	if err != nil {
		return err
	}
	if version != len(migrations()) {
		return ErrSchema
	}
	t := &transaction{conn: conn, ctx: ctx, write: write}
	if err = fn(t); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	committed = err == nil
	return dbError(err)
}
func rollback(conn *sql.Conn) {
	// Independent context: cancellation must not return an open transaction to
	// the pool. Discard the connection if rollback cannot be confirmed.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		// A successful COMMIT also makes ROLLBACK fail; discarding is safe.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
}
func connectionSettings(ctx context.Context, c *sql.Conn) error {
	for _, v := range []struct {
		name string
		want int
	}{{"foreign_keys", 1}, {"synchronous", 2}} {
		var n int
		if err := c.QueryRowContext(ctx, "PRAGMA "+v.name).Scan(&n); err != nil {
			return err
		}
		if n != v.want {
			return fmt.Errorf("unsafe SQLite %s setting", v.name)
		}
	}
	return nil
}
func dbError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return p.ErrNotFound
	}
	var se *modernc.Error
	if errors.As(err, &se) {
		switch se.Code() {
		case 1555, 2067:
			return p.ErrDuplicateID
		case 787, 275, 1299, 1811, 3091:
			return fmt.Errorf("%w: SQLite code %d", p.ErrIntegrity, se.Code())
		}
	}
	return err
}

type transaction struct {
	conn  *sql.Conn
	ctx   context.Context
	write bool
}

func (t *transaction) exec(query string, args ...any) (sql.Result, error) {
	if !t.write {
		return nil, p.ErrIntegrity
	}
	v, e := t.conn.ExecContext(t.ctx, query, args...)
	return v, dbError(e)
}
func timestamp(v time.Time) string { return v.UTC().Format(time.RFC3339Nano) }
func nullable[T ~string](v T) any {
	if v == "" {
		return nil
	}
	return string(v)
}

// Limit SQLite lock waiting to the remaining context budget as well as the
// configured maximum. The driver may otherwise finish its busy wait before
// observing cancellation.
func (r *Repository) configure(ctx context.Context, c *sql.Conn) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	busy := r.busy
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < busy {
			busy = remaining
		}
	}
	ms := busy.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	if _, err := c.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", ms)); err != nil {
		return err
	}
	return connectionSettings(ctx, c)
}

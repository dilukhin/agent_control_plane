package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	p "github.com/dilukhin/agent_control_plane/internal/persistence"
	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/state"
)

func openTest(t *testing.T) (*Repository, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	r, e := Open(context.Background(), path, Options{})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	return r, path
}
func raw(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, e := sql.Open("sqlite", path)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
func task(id string) state.Task {
	return state.Task{ID: protocol.TaskID(id), State: state.TaskPlanned, Revision: 1, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
}
func TestSettingsEveryConnection(t *testing.T) {
	r, _ := openTest(t)
	ctx := context.Background()
	var conns []*sql.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for range 4 {
		c, e := r.db.Conn(ctx)
		if e != nil {
			t.Fatal(e)
		}
		conns = append(conns, c)
		for _, check := range []struct {
			name string
			want any
		}{{"foreign_keys", int64(1)}, {"synchronous", int64(2)}, {"busy_timeout", int64(5000)}, {"auto_vacuum", int64(2)}, {"journal_mode", "wal"}, {"user_version", int64(len(migrations()))}} {
			var got any
			if e = c.QueryRowContext(ctx, "PRAGMA "+check.name).Scan(&got); e != nil {
				t.Fatal(e)
			}
			if got != check.want {
				t.Errorf("%s=%v want %v", check.name, got, check.want)
			}
		}
	}
}
func TestSchemaRejectsFutureTamperedAndUnrelated(t *testing.T) {
	for _, mode := range []string{"future", "checksum", "missing_history", "unrelated"} {
		t.Run(mode, func(t *testing.T) {
			r, path := openTest(t)
			db := raw(t, path)
			query := map[string]string{"future": "PRAGMA user_version=99", "checksum": "UPDATE schema_migrations SET checksum='changed'", "missing_history": "DELETE FROM schema_migrations", "unrelated": "PRAGMA application_id=42"}[mode]
			if _, e := db.Exec(query); e != nil {
				t.Fatal(e)
			}
			if e := r.Update(context.Background(), func(tx p.Tx) error { return tx.InsertTask(task("blocked")) }); !errors.Is(e, ErrSchema) {
				t.Fatalf("existing handle accepted schema: %v", e)
			}
			if got, e := Open(context.Background(), path, Options{}); !errors.Is(e, ErrSchema) {
				if got != nil {
					got.Close()
				}
				t.Fatalf("open accepted schema: %v", e)
			}
		})
	}
	t.Run("unversioned", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "other.db")
		db := raw(t, path)
		if _, e := db.Exec("CREATE TABLE unrelated(id INTEGER)"); e != nil {
			t.Fatal(e)
		}
		if r, e := Open(context.Background(), path, Options{}); !errors.Is(e, ErrSchema) {
			if r != nil {
				r.Close()
			}
			t.Fatalf("adopted unrelated db: %v", e)
		}
	})
}
func TestMigrationRollbackPreservesVersionAndData(t *testing.T) {
	r, _ := openTest(t)
	ctx := context.Background()
	if e := r.Update(ctx, func(tx p.Tx) error { return tx.InsertTask(task("existing")) }); e != nil {
		t.Fatal(e)
	}
	c, e := r.db.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	ms := append(migrations(), migration{len(migrations()) + 1, "CREATE TABLE partial(id INTEGER) STRICT; DELETE FROM tasks; INVALID SQL;"})
	if e = migrate(ctx, c, ms); e == nil {
		t.Fatal("bad migration succeeded")
	}
	version, e := validateSchema(ctx, c)
	if e != nil || version != len(migrations()) {
		t.Fatalf("version=%d %v", version, e)
	}
	var count int
	if e = c.QueryRowContext(ctx, "SELECT count(*) FROM tasks WHERE id='existing'").Scan(&count); e != nil || count != 1 {
		t.Fatalf("lost data: %d %v", count, e)
	}
	if e = c.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE name='partial'").Scan(&count); e != nil || count != 0 {
		t.Fatalf("partial schema: %d %v", count, e)
	}
}
func TestInitialMigrationRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.db")
	db := raw(t, path)
	ctx := context.Background()
	c, e := db.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if e = migrate(ctx, c, []migration{{1, "CREATE TABLE partial(id INTEGER) STRICT; INVALID SQL;"}}); e == nil {
		t.Fatal("bad migration succeeded")
	}
	v, e := validateSchema(ctx, c)
	if e != nil || v != 0 {
		t.Fatalf("initial rollback: %d %v", v, e)
	}
	if e = migrate(ctx, c, migrations()); e != nil {
		t.Fatalf("retry initial migration: %v", e)
	}
}
func TestPhysicalCASAndConstraints(t *testing.T) {
	r, _ := openTest(t)
	ctx := context.Background()
	now := time.Now()
	err := r.Update(ctx, func(tx p.Tx) error {
		if e := tx.InsertTask(task("task")); e != nil {
			return e
		}
		op := state.Operation{Descriptor: protocol.OperationDescriptor{ID: "op", TaskID: "task", Name: "name", TargetRef: "target", EffectClass: protocol.EffectReadOnly, IdempotencyMode: protocol.IdempotencyUnknown, VerificationPolicyRef: "v", AuthorizationPolicyRef: "a"}, State: state.OperationPlanned, Revision: 1, CreatedAt: now, UpdatedAt: now}
		return tx.InsertOperation(op)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Update(ctx, func(tx p.Tx) error {
		op, e := tx.Operation("op")
		if e != nil {
			return e
		}
		op.Revision = 3
		op.State = state.OperationReady
		return tx.UpdateOperation(op, 2)
	}); !errors.Is(err, state.ErrRevisionConflict) {
		t.Fatalf("SQL CAS: %v", err)
	}
	if err = r.Update(ctx, func(tx p.Tx) error {
		return tx.InsertAttempt(state.Attempt{ID: "orphan", OperationID: "missing", LeaseID: "lease", LeaseGeneration: 1, OwnerActorID: "owner", State: state.AttemptRunning, CreatedAt: now, UpdatedAt: now})
	}); !errors.Is(err, p.ErrIntegrity) {
		t.Fatalf("foreign key: %v", err)
	}
	if err = r.View(ctx, func(reader p.Reader) error { return reader.(p.Tx).InsertTask(task("bad")) }); !errors.Is(err, p.ErrIntegrity) {
		t.Fatalf("read callback wrote: %v", err)
	}
}
func TestBusyWaitBoundedAndCancelledWriteRolledBack(t *testing.T) {
	r, _ := openTest(t)
	ctx := context.Background()
	c, e := r.db.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if _, e = c.ExecContext(ctx, "BEGIN IMMEDIATE"); e != nil {
		t.Fatal(e)
	}
	defer rollback(c)
	child, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	called := false
	e = r.Update(child, func(tx p.Tx) error { called = true; return tx.InsertTask(task("blocked")) })
	if e == nil || called {
		t.Fatalf("blocked writer ran: %v %v", called, e)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("busy wait exceeded finite bound: %v", elapsed)
	}
}
func TestAbruptProcessExit(t *testing.T) {
	if path := os.Getenv("ACP_SQLITE_CRASH_TEST_PATH"); path != "" {
		r, e := Open(context.Background(), path, Options{})
		if e != nil {
			os.Exit(11)
		}
		if e = r.Update(context.Background(), func(tx p.Tx) error { return tx.InsertTask(task("committed")) }); e != nil {
			os.Exit(12)
		}
		_ = r.Update(context.Background(), func(tx p.Tx) error {
			if e := tx.InsertTask(task("uncommitted")); e != nil {
				os.Exit(13)
			}
			os.Exit(0)
			return nil
		})
		os.Exit(14)
	}
	path := filepath.Join(t.TempDir(), "crash.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestAbruptProcessExit$")
	cmd.Env = append(os.Environ(), "ACP_SQLITE_CRASH_TEST_PATH="+path)
	if output, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("child: %v %s", e, output)
	}
	r, e := Open(context.Background(), path, Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if e = r.View(context.Background(), func(tx p.Reader) error {
		if _, e := tx.Task("committed"); e != nil {
			return e
		}
		if _, e := tx.Task("uncommitted"); !errors.Is(e, p.ErrNotFound) {
			return errors.New("uncommitted task survived")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}

func TestUpgradePreviousSchemaPreservesData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	db := raw(t, path)
	ctx := context.Background()
	c, e := db.Conn(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = migrate(ctx, c, migrations()[:1]); e != nil {
		t.Fatal(e)
	}
	tx := &transaction{conn: c, ctx: ctx, write: true}
	if e = tx.InsertTask(task("from-v1")); e != nil {
		t.Fatal(e)
	}
	c.Close()
	db.Close()
	r, e := Open(ctx, path, Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if e = r.View(ctx, func(reader p.Reader) error { _, e := reader.Task("from-v1"); return e }); e != nil {
		t.Fatal(e)
	}
}

package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"errors"
	"fmt"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const applicationID = 0x41435031

var ErrSchema = errors.New("unsupported or inconsistent database schema")

type migration struct {
	version int
	sql     string
}

func migrations() []migration {
	out := []migration{}
	for i, name := range []string{"001_initial.sql", "002_recovery.sql", "003_retention.sql"} {
		b, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			panic(err)
		}
		out = append(out, migration{i + 1, string(b)})
	}
	return out
}
func checksum(s string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(s))) }
func validateSchema(ctx context.Context, c *sql.Conn) (int, error) {
	return validateHistory(ctx, c, migrations())
}
func validateHistory(ctx context.Context, c *sql.Conn, ms []migration) (int, error) {
	var app, version int
	if err := c.QueryRowContext(ctx, "PRAGMA application_id").Scan(&app); err != nil {
		return 0, err
	}
	if err := c.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return 0, err
	}
	if version == 0 && app == 0 {
		var count int
		if err := c.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").Scan(&count); err != nil {
			return 0, err
		}
		if count != 0 {
			return 0, ErrSchema
		}
		return 0, nil
	}
	if app != applicationID || version < 1 || version > len(ms) {
		return 0, ErrSchema
	}
	rows, err := c.QueryContext(ctx, "SELECT version,checksum FROM schema_migrations ORDER BY version")
	if err != nil {
		return 0, fmt.Errorf("%w: missing migration history", ErrSchema)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var v int
		var digest string
		if err = rows.Scan(&v, &digest); err != nil {
			return 0, err
		}
		n++
		if n > version || v != n || ms[n-1].version != v || digest != checksum(ms[n-1].sql) {
			return 0, ErrSchema
		}
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	if n != version {
		return 0, ErrSchema
	}
	return version, nil
}
func (r *Repository) initialize(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	c, err := r.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	if err = r.configure(ctx, c); err != nil {
		return err
	}
	if err = migrate(ctx, c, migrations()); err != nil {
		return err
	}
	var journal string
	if err = c.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journal); err != nil {
		return err
	}
	if journal != "wal" {
		return errors.New("WAL unavailable")
	}
	var result string
	if err = c.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return errors.New("database integrity check failed")
	}
	rows, err := c.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return errors.New("database foreign key check failed")
	}
	return rows.Err()
}
func migrate(ctx context.Context, c *sql.Conn, ms []migration) error {
	// auto_vacuum must be selected before opening the first schema transaction.
	// Validate first so an unrelated or newer database is never adopted.
	prior, err := validateHistory(ctx, c, ms)
	if err != nil {
		return err
	}
	if prior == 0 {
		if _, err = c.ExecContext(ctx, "PRAGMA auto_vacuum=INCREMENTAL"); err != nil {
			return err
		}
	}
	if _, err := c.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			rollback(c)
		}
	}()
	version, err := validateHistory(ctx, c, ms)
	if err != nil {
		return err
	}
	if version == 0 {
		if _, err = c.ExecContext(ctx, "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY CHECK(version>0), checksum TEXT NOT NULL) STRICT"); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id=%d", applicationID)); err != nil {
			return err
		}
	}
	for _, m := range ms {
		if m.version <= version {
			continue
		}
		if m.version != version+1 {
			return ErrSchema
		}
		if _, err = c.ExecContext(ctx, m.sql); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, "INSERT INTO schema_migrations VALUES(?,?)", m.version, checksum(m.sql)); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version=%d", m.version)); err != nil {
			return err
		}
		version = m.version
	}
	_, err = c.ExecContext(ctx, "COMMIT")
	committed = err == nil
	return err
}

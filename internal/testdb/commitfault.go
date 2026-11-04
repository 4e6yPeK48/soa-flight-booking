// Package testdb provides controlled database faults for PostgreSQL integration tests.
package testdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// CommitFault loses one client acknowledgment after PostgreSQL successfully commits.
// This is a driver fault, not a network interruption.
type CommitFault struct {
	armed, triggered atomic.Bool
}

func (f *CommitFault) Arm()            { f.armed.Store(true) }
func (f *CommitFault) Triggered() bool { return f.triggered.Load() }

func OpenCommitFault(t *testing.T, dsn string) (*sql.DB, *CommitFault) {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	fault := &CommitFault{}
	db := sql.OpenDB(&faultConnector{Connector: stdlib.GetConnector(*cfg), fault: fault})
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db, fault
}

type faultConnector struct {
	driver.Connector
	fault *CommitFault
}

func (c *faultConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	// Embed pgx's connection to retain its context, ping, and session reset behavior.
	return &faultConn{Conn: conn.(*stdlib.Conn), fault: c.fault}, nil
}

type faultConn struct {
	*stdlib.Conn
	fault *CommitFault
}

func (c *faultConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *faultConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &faultTx{Tx: tx, fault: c.fault}, nil
}

type faultTx struct {
	driver.Tx
	fault *CommitFault
}

func (tx *faultTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.fault.armed.CompareAndSwap(true, false) {
		tx.fault.triggered.Store(true)
		return driver.ErrBadConn
	}
	return nil
}

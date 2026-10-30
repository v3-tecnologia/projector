package projection

import (
	"context"
	"database/sql"
)

// Handler owns one read model. EnsureSchema is called by Projector.Initialize.
// Implement it with idempotent DDL (typically CREATE TABLE IF NOT EXISTS).
type Handler interface {
	Name() string
	Handles(category string) bool
	EnsureSchema(ctx context.Context, db *sql.DB) error
	Apply(ctx context.Context, tx *sql.Tx, event Event) error
}

// Package mariadb implements projection persistence using MariaDB/InnoDB.
package mariadb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/v3-tecnologia/projector/projection"
)

const processedEventsDDL = `
CREATE TABLE IF NOT EXISTS projection_processed_events (
    projection_name VARCHAR(128) NOT NULL,
    event_id        VARCHAR(255) NOT NULL,
    processed_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (projection_name, event_id)
) ENGINE=InnoDB`

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("mariadb database is required")
	}
	return &Store{db: db}, nil
}

func (s *Store) Initialize(ctx context.Context, handlers []projection.Handler) error {
	if _, err := s.db.ExecContext(ctx, processedEventsDDL); err != nil {
		return fmt.Errorf("create projection_processed_events: %w", err)
	}
	for _, handler := range handlers {
		if err := handler.EnsureSchema(ctx, s.db); err != nil {
			return fmt.Errorf("ensure schema for handler %q: %w", handler.Name(), err)
		}
	}
	return nil
}

func (s *Store) Process(
	ctx context.Context,
	handler projection.Handler,
	event projection.Event,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO projection_processed_events (projection_name, event_id)
		VALUES (?, ?)
	`, handler.Name(), event.ID)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return projection.ErrAlreadyProcessed
		}
		return fmt.Errorf("insert idempotency record: %w", err)
	}

	if err := handler.Apply(ctx, tx, event); err != nil {
		return fmt.Errorf("apply handler: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

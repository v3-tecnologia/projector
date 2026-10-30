package stock

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/v3-tecnologia/projector/projection"
)

// Projection is an example read model. The application may register it alongside
// any number of other handlers.
type Projection struct{}

func (Projection) Name() string { return "stock" }

func (Projection) Handles(category string) bool {
	return category == "stock.received" || category == "stock.sold"
}

func (Projection) EnsureSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS stock_read_model (
			product_id VARCHAR(128) NOT NULL,
			quantity BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (product_id),
			CHECK (quantity >= 0)
		) ENGINE=InnoDB
	`)
	return err
}

type payload struct {
	ProductID string `json:"product_id"`
	Quantity  int64  `json:"quantity"`
}

func (Projection) Apply(
	ctx context.Context,
	tx *sql.Tx,
	event projection.Event,
) error {
	var data payload
	if err := json.Unmarshal(event.Payload, &data); err != nil {
		return fmt.Errorf("decode stock payload: %w", err)
	}
	if data.ProductID == "" || data.Quantity <= 0 {
		return fmt.Errorf("invalid stock payload")
	}

	switch event.Category {
	case "stock.received":
		_, err := tx.ExecContext(ctx, `
			INSERT INTO stock_read_model (product_id, quantity)
			VALUES (?, ?)
			ON DUPLICATE KEY UPDATE quantity = quantity + VALUES(quantity)
		`, data.ProductID, data.Quantity)
		return err

	case "stock.sold":
		result, err := tx.ExecContext(ctx, `
			UPDATE stock_read_model
			SET quantity = quantity - ?
			WHERE product_id = ? AND quantity >= ?
		`, data.Quantity, data.ProductID, data.Quantity)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return fmt.Errorf("stock missing or insufficient for product %q", data.ProductID)
		}
	}
	return nil
}

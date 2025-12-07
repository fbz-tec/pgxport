package db

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Store defines the interface for database operations.
// Implementations should handle connection management and query execution.
type Store interface {
	Connect() error
	Close() error
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

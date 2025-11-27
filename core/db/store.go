package db

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type Store interface {
	Connect() error
	Close() error
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

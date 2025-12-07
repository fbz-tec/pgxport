package db

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/jackc/pgx/v5"
)

// PgStore represents a PostgreSQL database store connection.
type PgStore struct {
	dsn  string
	conn *pgx.Conn
}

// NewPgStore creates a new PostgreSQL store instance with the given DSN.
func NewPgStore(dsn string) *PgStore {
	return &PgStore{dsn: dsn}
}

// Connect establishes a connection to the PostgreSQL database.
// Returns an error if the connection fails or if ping fails.
func (s *PgStore) Connect() error {
	if s.conn != nil {
		return nil // already connected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger.Debug("Connection timeout: 10s")
	logger.Debug("Attempting to connect to database host: %s", sanitizeDSN(s.dsn))

	conn, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}

	logger.Debug("Connection established, verifying connectivity (ping)...")

	// Ping the database
	if err := conn.Ping(ctx); err != nil {
		conn.Close(ctx)
		return fmt.Errorf("unable to ping database: %w", err)
	}

	logger.Debug("Database ping successful")
	s.conn = conn
	return nil
}

// Close closes the database connection.
// Returns an error if the close operation fails.
func (s *PgStore) Close() error {
	logger.Debug("Closing database connection...")

	if s.conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := s.conn.Close(ctx)
		if err != nil {
			logger.Debug("Error closing database connection: %v", err)
		} else {
			logger.Debug("Database connection closed successfully")
		}
		return err
	}
	return nil
}

// Query executes a SQL query with the given arguments and returns the result rows.
// Returns an error if the query execution fails or if the store is not connected.
func (s *PgStore) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if s.conn == nil {
		logger.Debug("No active database connection; query cannot be executed")
		return nil, fmt.Errorf("database not connected")
	}

	logger.Debug("Executing SQL query...")
	logger.Debug("Query: %s", sql)

	startTime := time.Now()
	rows, err := s.conn.Query(ctx, sql, args...)
	duration := time.Since(startTime)

	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	logger.Debug("Query executed successfully in %v", duration)
	return rows, nil
}

// Conn returns the underlying PostgreSQL connection.
// This is useful for advanced operations like COPY that require direct connection access.
func (s *PgStore) Conn() *pgx.Conn {
	return s.conn
}

// sanitizeDSN masks the password inside a PostgreSQL DSN before logging.
func sanitizeDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "<invalid-dsn>"
	}

	var userInfo string
	if u.User != nil {
		username := u.User.Username()
		if _, hasPwd := u.User.Password(); hasPwd {
			userInfo = fmt.Sprintf("%s:***@", username)
		} else {
			userInfo = fmt.Sprintf("%s@", username)
		}
	}

	path := u.Path
	if path == "" {
		path = "/"
	}

	return fmt.Sprintf("%s://%s%s%s", u.Scheme, userInfo, u.Host, path)
}

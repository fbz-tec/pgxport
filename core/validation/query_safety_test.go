package validation

import (
	"strings"
	"testing"
)

func TestValidateQuery(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
		errMsg  string // Optional: check for specific error message content
	}{
		// ========== Valid Queries ==========
		{
			name:    "valid SELECT",
			query:   "SELECT * FROM users",
			wantErr: false,
		},
		{
			name:    "valid SELECT with WHERE",
			query:   "SELECT id, name FROM users WHERE active = true",
			wantErr: false,
		},
		{
			name:    "valid SELECT with JOIN",
			query:   "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id",
			wantErr: false,
		},
		{
			name:    "valid WITH (CTE)",
			query:   "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users",
			wantErr: false,
		},
		{
			name:    "valid multiple CTEs",
			query:   "WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1",
			wantErr: false,
		},
		{
			name:    "valid SELECT with subquery",
			query:   "SELECT * FROM (SELECT id FROM users) AS sub",
			wantErr: false,
		},
		{
			name:    "valid SELECT with string containing 'delete'",
			query:   "SELECT 'delete me' AS action FROM users",
			wantErr: false,
		},
		{
			name:    "valid SELECT with column name containing 'update'",
			query:   "SELECT last_update FROM users",
			wantErr: false,
		},
		{
			name:    "valid SELECT with semicolon",
			query:   "SELECT * FROM users;",
			wantErr: false,
		},

		// ========== Forbidden Commands ==========
		{
			name:    "forbidden DELETE",
			query:   "DELETE FROM users",
			wantErr: true,
			errMsg:  "DELETE",
		},
		{
			name:    "forbidden DROP",
			query:   "DROP TABLE users",
			wantErr: true,
			errMsg:  "DROP",
		},
		{
			name:    "forbidden TRUNCATE",
			query:   "TRUNCATE TABLE users",
			wantErr: true,
			errMsg:  "TRUNCATE",
		},
		{
			name:    "forbidden INSERT",
			query:   "INSERT INTO users (name) VALUES ('test')",
			wantErr: true,
			errMsg:  "INSERT",
		},
		{
			name:    "forbidden UPDATE",
			query:   "UPDATE users SET name = 'test'",
			wantErr: true,
			errMsg:  "UPDATE",
		},
		{
			name:    "forbidden ALTER",
			query:   "ALTER TABLE users ADD COLUMN test INT",
			wantErr: true,
			errMsg:  "ALTER",
		},
		{
			name:    "forbidden CREATE",
			query:   "CREATE TABLE test (id INT)",
			wantErr: true,
			errMsg:  "CREATE",
		},
		{
			name:    "forbidden GRANT",
			query:   "GRANT SELECT ON users TO user1",
			wantErr: true,
			errMsg:  "GRANT",
		},
		{
			name:    "forbidden REVOKE",
			query:   "REVOKE SELECT ON users FROM user1",
			wantErr: true,
			errMsg:  "REVOKE",
		},
		{
			name:    "forbidden EXECUTE",
			query:   "EXECUTE procedure_name()",
			wantErr: true,
			errMsg:  "EXECUTE",
		},
		{
			name:    "forbidden EXEC",
			query:   "EXEC procedure_name",
			wantErr: true,
			errMsg:  "EXEC",
		},
		{
			name:    "forbidden CALL",
			query:   "CALL procedure_name()",
			wantErr: true,
			errMsg:  "CALL",
		},
		{
			name:    "forbidden MERGE",
			query:   "MERGE INTO users USING source ON condition",
			wantErr: true,
			errMsg:  "MERGE",
		},

		// ========== Case Insensitivity ==========
		{
			name:    "lowercase delete",
			query:   "delete from users",
			wantErr: true,
			errMsg:  "DELETE",
		},
		{
			name:    "mixed case delete",
			query:   "DeLeTe FrOm users",
			wantErr: true,
			errMsg:  "DELETE",
		},

		// ========== Multiple Statements ==========
		{
			name:    "chained DELETE",
			query:   "SELECT 1; DELETE FROM users",
			wantErr: true,
			errMsg:  "only a single SQL statement",
		},
		{
			name:    "multiple forbidden commands",
			query:   "SELECT 1; DROP TABLE users; SELECT 2",
			wantErr: true,
			errMsg:  "only a single SQL statement",
		},
		{
			name:    "valid multiple SELECTs",
			query:   "SELECT 1; SELECT 2; SELECT 3",
			wantErr: true,
			errMsg:  "only a single SQL statement",
		},

		// ========== Comment Evasion Attempts ==========
		{
			name:    "DELETE in single-line comment",
			query:   "SELECT * FROM users -- DELETE FROM users",
			wantErr: false, // Comments are removed, so this should pass
		},
		{
			name:    "DELETE in multi-line comment",
			query:   "SELECT * FROM users /* DELETE FROM users */",
			wantErr: false, // Comments are removed, so this should pass
		},
		{
			name:    "DELETE before comment",
			query:   "DELETE FROM users -- this is a comment",
			wantErr: true,
			errMsg:  "DELETE",
		},
		{
			name:    "nested comments",
			query:   "SELECT * FROM users /* outer /* inner */ comment */",
			wantErr: false,
		},

		// ========== String Literal Handling ==========
		{
			name:    "DELETE in string literal",
			query:   "SELECT 'DELETE FROM users' AS query FROM users",
			wantErr: false,
		},
		{
			name:    "semicolon in string literal",
			query:   "SELECT 'test; DELETE' AS cmd FROM users",
			wantErr: false,
		},
		{
			name:    "escaped quotes in string",
			query:   "SELECT 'O''Brien' AS name FROM users",
			wantErr: false,
		},
		{
			name:    "double quotes string",
			query:   `SELECT "DELETE FROM users" AS cmd FROM users`,
			wantErr: false,
		},

		// ========== CTE and Subquery Attacks ==========
		{
			name:    "forbidden command in CTE",
			query:   "WITH bad AS (DELETE FROM users RETURNING *) SELECT * FROM bad",
			wantErr: true,
			errMsg:  "DELETE",
		},
		{
			name:    "forbidden command in subquery",
			query:   "SELECT * FROM (DELETE FROM users RETURNING *) AS sub",
			wantErr: true,
			errMsg:  "DELETE",
		},
		{
			name:    "valid CTE with SELECT",
			query:   "WITH users_cte AS (SELECT * FROM users) SELECT * FROM users_cte",
			wantErr: false,
		},

		// ========== Edge Cases ==========
		{
			name:    "empty query",
			query:   "",
			wantErr: true,
			errMsg:  "empty",
		},
		{
			name:    "whitespace only",
			query:   "   \n\t  ",
			wantErr: true,
			errMsg:  "empty",
		},
		{
			name:    "SELECT with lots of whitespace",
			query:   "   SELECT     *     FROM     users   ",
			wantErr: false,
		},
		{
			name:    "unknown command",
			query:   "UNKNOWN_COMMAND users",
			wantErr: true,
			errMsg:  "unsupported",
		},
		{
			name:    "SELECT with UNION",
			query:   "SELECT 1 UNION SELECT 2",
			wantErr: false,
		},
		{
			name:    "SELECT with INTERSECT",
			query:   "SELECT 1 INTERSECT SELECT 2",
			wantErr: false,
		},
		{
			name:    "SELECT with EXCEPT",
			query:   "SELECT 1 EXCEPT SELECT 2",
			wantErr: false,
		},

		// ========== Real-world Attack Scenarios ==========
		{
			name:    "attack: comment hiding DELETE",
			query:   "SELECT * FROM users; -- DELETE FROM users;",
			wantErr: true, // Multiple statements detected (semicolon creates second statement even if comment is removed)
			errMsg:  "only a single SQL statement",
		},
		{
			name:    "attack: multi-line comment hiding DELETE",
			query:   "SELECT * FROM users; /* DELETE FROM users; */",
			wantErr: true, // Multiple statements detected (semicolon creates second statement even if comment is removed)
			errMsg:  "only a single SQL statement",
		},
		{
			name:    "attack: string containing command",
			query:   "SELECT 'DELETE FROM users' AS malicious",
			wantErr: false, // String literal, not actual command
		},
		{
			name:    "attack: column name containing command",
			query:   "SELECT delete_flag, update_time FROM users",
			wantErr: false, // Column names, not commands
		},
		{
			name:    "attack: actual DELETE after valid SELECT",
			query:   "SELECT 1; DELETE FROM users",
			wantErr: true,
			errMsg:  "only a single SQL statement",
		},
		{
			name:    "attack: WITH containing DELETE",
			query:   "WITH malicious AS (DELETE FROM users RETURNING id) SELECT * FROM malicious",
			wantErr: true,
			errMsg:  "DELETE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateQuery(tt.query)
			hasErr := err != nil

			if hasErr != tt.wantErr {
				t.Errorf("ValidateQuery() error = %v, wantErr %v", err, tt.wantErr)
				if err != nil {
					t.Logf("Error message: %v", err)
				}
				return
			}

			// If we expect an error and specified an error message pattern, check it
			if tt.wantErr && tt.errMsg != "" && err != nil {
				errStr := err.Error()
				if !strings.Contains(strings.ToUpper(errStr), strings.ToUpper(tt.errMsg)) {
					t.Errorf("ValidateQuery() error message = %v, expected to contain %v", errStr, tt.errMsg)
				}
			}
		})
	}
}

// TestValidateQuery_ComplexQueries tests complex real-world queries
func TestValidateQuery_ComplexQueries(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name: "complex SELECT with multiple JOINs",
			query: `
				SELECT 
					u.id,
					u.name,
					o.total,
					p.name AS product_name
				FROM users u
				LEFT JOIN orders o ON u.id = o.user_id
				LEFT JOIN order_items oi ON o.id = oi.order_id
				LEFT JOIN products p ON oi.product_id = p.id
				WHERE u.active = true
				ORDER BY o.total DESC
			`,
			wantErr: false,
		},
		{
			name: "complex CTE with multiple levels",
			query: `
				WITH 
					active_users AS (
						SELECT * FROM users WHERE active = true
					),
					user_orders AS (
						SELECT u.id, COUNT(o.id) AS order_count
						FROM active_users u
						LEFT JOIN orders o ON u.id = o.user_id
						GROUP BY u.id
					)
				SELECT * FROM user_orders WHERE order_count > 0
			`,
			wantErr: false,
		},
		{
			name: "SELECT with window functions",
			query: `
				SELECT 
					id,
					name,
					ROW_NUMBER() OVER (PARTITION BY category ORDER BY price) AS rn
				FROM products
			`,
			wantErr: false,
		},
		{
			name: "SELECT with array operations",
			query: `
				SELECT 
					id,
					tags,
					array_length(tags, 1) AS tag_count
				FROM products
				WHERE 'electronics' = ANY(tags)
			`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateQuery(tt.query)
			hasErr := err != nil
			if hasErr != tt.wantErr {
				t.Errorf("ValidateQuery() error = %v, wantErr %v", err, tt.wantErr)
				if err != nil {
					t.Logf("Error message: %v", err)
				}
			}
		})
	}
}

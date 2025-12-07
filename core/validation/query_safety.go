package validation

import (
	"fmt"
	"regexp"
	"strings"
)

// Allowed SQL commands for read-only operations
var allowedCommands = map[string]bool{
	"SELECT": true,
	"WITH":   true, // CTE (Common Table Expression) - read-only
}

// Forbidden SQL commands that modify data or schema
var forbiddenCommands = []string{
	"DELETE",
	"DROP",
	"TRUNCATE",
	"INSERT",
	"UPDATE",
	"ALTER",
	"CREATE",
	"GRANT",
	"REVOKE",
	"EXECUTE",
	"EXEC",
	"CALL",
	"MERGE",
	"COPY", // COPY TO is allowed in COPY mode, but COPY FROM is not
}

// ValidateQuery checks if the query is safe for export (read-only)
func ValidateQuery(query string) error {
	if strings.TrimSpace(query) == "" {
		return fmt.Errorf("query cannot be empty")
	}

	// Step 1: Remove SQL comments to prevent comment-based evasion
	cleanedQuery := removeSQLComments(query)

	// Step 2: Split into individual statements (handles multiple queries separated by ;)
	statements := splitStatements(cleanedQuery)

	if len(statements) > 1 {
		return fmt.Errorf("only a single SQL statement is allowed")
	}

	// Step 3: Validate each statement
	for i, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}

		// Normalize statement for analysis
		normalized := normalizeSQL(stmt)

		// Step 4: Extract the first command from the statement
		firstCommand := extractFirstCommand(normalized)

		if firstCommand == "" {
			// If we can't identify a command, reject for safety
			return fmt.Errorf("unable to identify SQL command in statement %d (security: unknown command)", i+1)
		}

		// Step 5: Check if command is in whitelist (strict whitelist approach)
		if !allowedCommands[firstCommand] {
			// Check if it's a forbidden command
			for _, forbidden := range forbiddenCommands {
				if firstCommand == forbidden {
					return fmt.Errorf("forbidden SQL command detected: %s (read-only mode)", forbidden)
				}
			}
			// Unknown command - reject for safety
			return fmt.Errorf("unsupported SQL command: %s (only SELECT and WITH are allowed)", firstCommand)
		}

		// Step 6: Additional security check - scan for forbidden commands even in allowed queries
		// This catches attempts to hide commands in CTEs, subqueries, or comments that weren't removed
		if err := scanForForbiddenCommands(normalized); err != nil {
			return err
		}
	}

	return nil
}

// removeSQLComments removes SQL comments from the query
// Handles both -- (single line) and /* */ (multi-line) comments
func removeSQLComments(query string) string {
	var result strings.Builder
	inSingleLineComment := false
	inMultiLineComment := false
	inString := false
	stringChar := byte(0)

	queryBytes := []byte(query)
	i := 0

	for i < len(queryBytes) {
		char := queryBytes[i]

		// Handle string literals (skip comment detection inside strings)
		if !inSingleLineComment && !inMultiLineComment {
			if char == '\'' || char == '"' {
				if !inString {
					inString = true
					stringChar = char
				} else if char == stringChar {
					// Check for escaped quote ('' or "")
					if i+1 < len(queryBytes) && queryBytes[i+1] == stringChar {
						result.WriteByte(char)
						result.WriteByte(char)
						i += 2
						continue
					}
					inString = false
					stringChar = 0
				}
				result.WriteByte(char)
				i++
				continue
			}
		}

		// Handle comments only outside of strings
		if !inString {
			// Check for single-line comment (--)
			if !inMultiLineComment && i+1 < len(queryBytes) && char == '-' && queryBytes[i+1] == '-' {
				inSingleLineComment = true
				i += 2
				continue
			}

			// Check for end of single-line comment (newline)
			if inSingleLineComment {
				if char == '\n' {
					inSingleLineComment = false
					result.WriteByte(char) // Keep newline for statement splitting
				}
				i++
				continue
			}

			// Check for multi-line comment start (/*)
			if !inSingleLineComment && i+1 < len(queryBytes) && char == '/' && queryBytes[i+1] == '*' {
				inMultiLineComment = true
				i += 2
				continue
			}

			// Check for multi-line comment end (*/)
			if inMultiLineComment {
				if i+1 < len(queryBytes) && char == '*' && queryBytes[i+1] == '/' {
					inMultiLineComment = false
					i += 2
					continue
				}
				i++
				continue
			}
		}

		// Write character if not in comment
		if !inSingleLineComment && !inMultiLineComment {
			result.WriteByte(char)
		}
		i++
	}

	return result.String()
}

// splitStatements splits a query into individual statements separated by semicolons
// Handles semicolons inside string literals and function calls
func splitStatements(query string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)

	queryBytes := []byte(query)
	i := 0

	for i < len(queryBytes) {
		char := queryBytes[i]

		// Handle string literals
		if char == '\'' || char == '"' {
			if !inString {
				inString = true
				stringChar = char
			} else if char == stringChar {
				// Check for escaped quote
				if i+1 < len(queryBytes) && queryBytes[i+1] == stringChar {
					current.WriteByte(char)
					current.WriteByte(char)
					i += 2
					continue
				}
				inString = false
				stringChar = 0
			}
			current.WriteByte(char)
			i++
			continue
		}

		// Handle semicolon (statement separator)
		if !inString && char == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			i++
			continue
		}

		current.WriteByte(char)
		i++
	}

	// Add final statement if exists
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

// normalizeSQL normalizes SQL for analysis
// - Converts to uppercase
// - Normalizes whitespace
// - Removes leading/trailing whitespace
func normalizeSQL(query string) string {
	// Convert to uppercase
	normalized := strings.ToUpper(strings.TrimSpace(query))

	// Normalize whitespace (multiple spaces to single space)
	space := regexp.MustCompile(`\s+`)
	normalized = space.ReplaceAllString(normalized, " ")

	return normalized
}

// extractFirstCommand extracts the first SQL command from a normalized query
// Handles CTEs (WITH ... SELECT) and direct SELECT statements
func extractFirstCommand(normalized string) string {
	normalized = strings.TrimSpace(normalized)
	if normalized == "" {
		return ""
	}

	// Check for WITH (CTE) - allowed command
	if strings.HasPrefix(normalized, "WITH ") {
		// Find the SELECT after the WITH clause
		// WITH can be complex, so we look for SELECT after the first closing parenthesis or comma
		selectIdx := findSelectAfterWith(normalized)
		if selectIdx != -1 {
			return "WITH"
		}
		// If no SELECT found, still return WITH (will be validated further)
		return "WITH"
	}

	// Extract first word (command)
	parts := strings.Fields(normalized)
	if len(parts) == 0 {
		return ""
	}

	firstWord := parts[0]
	// Remove any trailing punctuation
	firstWord = strings.TrimRight(firstWord, ";,()")

	return firstWord
}

// findSelectAfterWith finds the position of SELECT after a WITH clause
func findSelectAfterWith(query string) int {
	// Simple approach: look for SELECT after WITH
	// More sophisticated parsing would be needed for complex CTEs
	queryLower := strings.ToUpper(query)
	selectIdx := strings.Index(queryLower, " SELECT ")
	if selectIdx == -1 {
		selectIdx = strings.Index(queryLower, "SELECT ")
	}
	return selectIdx
}

// scanForForbiddenCommands scans the normalized query for forbidden commands
func scanForForbiddenCommands(normalized string) error {
	// Remove string literals to avoid false positives
	// This allows commands in strings like: SELECT 'DELETE FROM users' AS cmd
	queryWithoutStrings := removeStringLiterals(normalized)

	// Create a pattern that matches forbidden commands as whole words
	// This prevents false positives (e.g., "SELECT" containing "ELECT")
	for _, forbidden := range forbiddenCommands {
		// Match forbidden command as a whole word (not part of another word)
		// Pattern: word boundary, command, followed by space or semicolon or end
		pattern := fmt.Sprintf(`\b%s\b`, regexp.QuoteMeta(forbidden))
		matched, err := regexp.MatchString(pattern, queryWithoutStrings)
		if err != nil {
			continue // Skip if regex fails
		}
		if matched {
			return fmt.Errorf("forbidden SQL command detected: %s (security: command found in query)", forbidden)
		}
	}
	return nil
}

// removeStringLiterals removes SQL string literals (single and double quotes)
// and replaces them with spaces to preserve word boundaries
func removeStringLiterals(query string) string {
	var result strings.Builder
	inSingleQuote := false
	inDoubleQuote := false

	queryBytes := []byte(query)
	i := 0

	for i < len(queryBytes) {
		char := queryBytes[i]

		// Handle single quotes (SQL string literals)
		if char == '\'' && !inDoubleQuote {
			if inSingleQuote {
				// Check for escaped quote ('') - in SQL, '' inside a string is an escaped quote
				if i+1 < len(queryBytes) && queryBytes[i+1] == '\'' {
					// It's an escaped quote inside the string, skip both quotes
					// Don't write anything, just skip
					i += 2
					continue
				}
				// End of string literal
				inSingleQuote = false
				// Write a space instead of the closing quote
				result.WriteByte(' ')
			} else {
				// Start of string literal
				inSingleQuote = true
				// Write a space instead of the opening quote
				result.WriteByte(' ')
			}
			i++
			continue
		}

		// Handle double quotes (PostgreSQL identifier quotes)
		if char == '"' && !inSingleQuote {
			if inDoubleQuote {
				// Check for escaped quote ("") - in PostgreSQL, "" inside an identifier is an escaped quote
				if i+1 < len(queryBytes) && queryBytes[i+1] == '"' {
					// It's an escaped quote inside the identifier, skip both quotes
					i += 2
					continue
				}
				// End of quoted identifier
				inDoubleQuote = false
				// Write a space instead of the closing quote
				result.WriteByte(' ')
			} else {
				// Start of quoted identifier
				inDoubleQuote = true
				// Write a space instead of the opening quote
				result.WriteByte(' ')
			}
			i++
			continue
		}

		// Write character if not inside a string literal
		if !inSingleQuote && !inDoubleQuote {
			result.WriteByte(char)
		} else {
			// Replace string/identifier content with space
			result.WriteByte(' ')
		}
		i++
	}

	return result.String()
}

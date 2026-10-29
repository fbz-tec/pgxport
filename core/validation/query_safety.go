package validation

import (
	"fmt"
	"strings"
)

// ValidateQuery gives early, clear errors for queries pgxport cannot export:
// an empty query, several statements, or a statement that is not SELECT/WITH.
// It is not the read-only guarantee: the query runs in a READ ONLY transaction
// (see db.PgStore.Connect), so PostgreSQL itself refuses any write, including
// writes hidden in functions, CTEs or setval().
func ValidateQuery(query string) error {
	if strings.TrimSpace(query) == "" {
		return fmt.Errorf("query cannot be empty")
	}

	// Checked before comment removal, so a comment cannot hide a second statement
	if hasMultipleStatements(query) {
		return fmt.Errorf("only a single SQL statement is allowed")
	}

	words := strings.Fields(removeSQLComments(query))
	if len(words) == 0 {
		return fmt.Errorf("query cannot be empty")
	}
	command := strings.ToUpper(strings.TrimLeft(words[0], "("))
	if command != "SELECT" && command != "WITH" {
		return fmt.Errorf("unsupported SQL command: %s (only SELECT and WITH queries can be exported)", command)
	}

	return nil
}

// hasMultipleStatements checks if the query contains multiple statements
// by detecting semicolons outside of string literals and comments
// This is done before comment removal to catch attempts to hide statements
func hasMultipleStatements(query string) bool {
	semicolonCount := 0
	semicolonPos := -1
	inSingleQuote := false
	inDoubleQuote := false
	inSingleLineComment := false
	inMultiLineComment := false

	queryBytes := []byte(query)
	i := 0

	for i < len(queryBytes) {
		char := queryBytes[i]

		// Handle string literals (skip semicolon detection inside strings)
		if !inSingleLineComment && !inMultiLineComment {
			if char == '\'' && !inDoubleQuote {
				if inSingleQuote {
					// Check for escaped quote ('')
					if i+1 < len(queryBytes) && queryBytes[i+1] == '\'' {
						i += 2
						continue
					}
					inSingleQuote = false
				} else {
					inSingleQuote = true
				}
				i++
				continue
			}

			if char == '"' && !inSingleQuote {
				if inDoubleQuote {
					// Check for escaped quote ("")
					if i+1 < len(queryBytes) && queryBytes[i+1] == '"' {
						i += 2
						continue
					}
					inDoubleQuote = false
				} else {
					inDoubleQuote = true
				}
				i++
				continue
			}
		}

		// Handle comments (skip semicolon detection inside comments)
		if !inSingleQuote && !inDoubleQuote {
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

		// Count semicolons outside of strings and comments
		if !inSingleQuote && !inDoubleQuote && !inSingleLineComment && !inMultiLineComment {
			if char == ';' {
				semicolonCount++
				// Store position of first semicolon
				if semicolonPos == -1 {
					semicolonPos = i
				}
				// If we find more than one semicolon, we have multiple statements
				if semicolonCount > 1 {
					return true
				}
			}
		}

		i++
	}

	// If we have exactly one semicolon, check if it's at the end (allowed) or in the middle (multiple statements)
	if semicolonCount == 1 && semicolonPos != -1 {
		// Check what comes after the semicolon
		afterSemicolon := strings.TrimSpace(query[semicolonPos+1:])
		// If there's anything after the semicolon (even just a comment), it's multiple statements
		if len(afterSemicolon) > 0 {
			return true
		}
	}

	return false
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

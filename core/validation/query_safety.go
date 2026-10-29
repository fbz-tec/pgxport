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
	if _, multiple := statementEnd(query); multiple {
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

// TrimTerminator returns the query without its final ';' and any comment after it,
// so it can be embedded in another statement such as COPY (...) TO STDOUT.
// Call it after ValidateQuery, which guarantees a single statement.
func TrimTerminator(query string) string {
	end, _ := statementEnd(query)
	return query[:end]
}

// statementEnd returns where the first statement ends: the position of its ';'
// outside of string literals and comments, or len(query) if there is none.
// multiple is true when another statement follows; comments after the ';' do not count.
func statementEnd(query string) (end int, multiple bool) {
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
					return semicolonPos, true
				}
			}
		}

		i++
	}

	if semicolonPos == -1 {
		return len(query), false
	}
	// Only comments may follow the terminating semicolon
	return semicolonPos, strings.TrimSpace(removeSQLComments(query[semicolonPos+1:])) != ""
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

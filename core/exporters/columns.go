package exporters

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// uniqueColumnNames returns the column names to use as keys in formats that need unique
// keys (JSON, YAML, templates). A repeated name, common with SELECT * over a JOIN, gets a
// suffix: id, id_2, id_3. Without it, the second "id" would overwrite the first one.
// A suffix never reuses another column's name (id, id, id_2 gives id, id_3, id_2).
func uniqueColumnNames(fields []pgconn.FieldDescription) []string {
	original := make(map[string]bool, len(fields))
	for _, fd := range fields {
		original[fd.Name] = true
	}

	used := make(map[string]bool, len(fields))
	names := make([]string, len(fields))
	for i, fd := range fields {
		name := fd.Name
		if used[name] {
			for n := 2; ; n++ {
				candidate := fmt.Sprintf("%s_%d", fd.Name, n)
				if !original[candidate] && !used[candidate] {
					name = candidate
					break
				}
			}
		}
		used[name] = true
		names[i] = name
	}
	return names
}

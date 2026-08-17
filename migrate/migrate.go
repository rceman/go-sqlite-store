package migrate

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"

	"github.com/rceman/go-sqlite-store/store"
)

const DefaultTable = "schema_migrations"

var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Migration struct {
	Version    int64
	Name       string
	Statements []store.Statement
}

type Options struct {
	Table string
}

// Apply applies each missing migration exactly once. Each migration's statements
// and its version marker are submitted as one Store.Batch request, so they share
// the store's per-request atomicity boundary.
func Apply(ctx context.Context, s *store.Store, migrations []Migration, opts Options) error {
	if s == nil {
		return errors.New("migrate: store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	table := opts.Table
	if table == "" {
		table = DefaultTable
	}
	if !identifierRE.MatchString(table) {
		return fmt.Errorf("migrate: invalid table name %q", table)
	}

	ordered := append([]Migration(nil), migrations...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Version < ordered[j].Version })
	for i, m := range ordered {
		if m.Version <= 0 {
			return fmt.Errorf("migrate: version must be positive: %d", m.Version)
		}
		if m.Name == "" {
			return fmt.Errorf("migrate: version %d has empty name", m.Version)
		}
		if len(m.Statements) == 0 {
			return fmt.Errorf("migrate: version %d has no statements", m.Version)
		}
		if i > 0 && ordered[i-1].Version == m.Version {
			return fmt.Errorf("migrate: duplicate version %d", m.Version)
		}
	}

	if _, err := s.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		version INTEGER PRIMARY KEY,
		name TEXT NOT NULL
	)`, table)); err != nil {
		return fmt.Errorf("migrate: create table: %w", err)
	}

	rows, err := s.Query(ctx, fmt.Sprintf(`SELECT version, name FROM %s ORDER BY version`, table))
	if err != nil {
		return fmt.Errorf("migrate: read applied versions: %w", err)
	}
	applied := make(map[int64]string, len(rows.Rows))
	for _, row := range rows.Rows {
		if len(row) != 2 {
			return errors.New("migrate: invalid migration table row")
		}
		version, ok := row[0].(int64)
		if !ok {
			return fmt.Errorf("migrate: invalid version type %T", row[0])
		}
		name, ok := row[1].(string)
		if !ok {
			return fmt.Errorf("migrate: invalid name type %T", row[1])
		}
		applied[version] = name
	}

	for _, m := range ordered {
		if name, ok := applied[m.Version]; ok {
			if name != m.Name {
				return fmt.Errorf("migrate: version %d already applied as %q, requested %q", m.Version, name, m.Name)
			}
			continue
		}
		stmts := make([]store.Statement, 0, len(m.Statements)+1)
		stmts = append(stmts, m.Statements...)
		stmts = append(stmts, store.Statement{
			SQL:  fmt.Sprintf(`INSERT INTO %s(version, name) VALUES(?, ?)`, table),
			Args: []any{m.Version, m.Name},
		})
		if _, err := s.Batch(ctx, stmts); err != nil {
			return fmt.Errorf("migrate: apply version %d (%s): %w", m.Version, m.Name, err)
		}
	}
	return nil
}

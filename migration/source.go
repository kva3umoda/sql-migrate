package migrate

import (
	"bytes"
	"embed"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/rubenv/sql-migrate/sqlparse"
)

type byId []*Migration

func (b byId) Len() int           { return len(b) }
func (b byId) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byId) Less(i, j int) bool { return b[i].Less(b[j]) }

type Source interface {
	// FindMigrations Finds the migrations.
	// The resulting slice of migrations should be sorted by Id.
	FindMigrations() ([]*Migration, error)
}

// A hardcoded set of migrations, in-memory.
type MemorySource struct {
	Migrations []*Migration
}

var _ Source = (*MemorySource)(nil)

func (m MemorySource) FindMigrations() ([]*Migration, error) {
	// Make sure migrations are sorted. In order to make the MemorySource safe for
	// concurrent use we should not mutate it in place. So `FindMigrations` would sort a copy
	// of the m.Migrations.
	migrations := make([]*Migration, len(m.Migrations))
	copy(migrations, m.Migrations)
	sort.Sort(byId(migrations))
	return migrations, nil
}

// A set of migrations loaded from an http.FileServer

type HttpFileSystemSource struct {
	FileSystem http.FileSystem
}

var _ Source = (*HttpFileSystemSource)(nil)

func (f HttpFileSystemSource) FindMigrations() ([]*Migration, error) {
	return findMigrations(f.FileSystem, "/")
}

// A set of migrations loaded from a directory.
type FileSource struct {
	Dir string
}

var _ Source = (*FileSource)(nil)

func (f FileSource) FindMigrations() ([]*Migration, error) {
	filesystem := http.Dir(f.Dir)
	return findMigrations(filesystem, "/")
}

func findMigrations(dir http.FileSystem, root string) ([]*Migration, error) {
	migrations := make([]*Migration, 0)

	file, err := dir.Open(root)
	if err != nil {
		return nil, err
	}

	files, err := file.Readdir(0)
	if err != nil {
		return nil, err
	}

	for _, info := range files {
		if strings.HasSuffix(info.Name(), ".sql") {
			migration, err := migrationFromFile(dir, root, info)
			if err != nil {
				return nil, err
			}

			migrations = append(migrations, migration)
		}
	}

	// Make sure migrations are sorted
	sort.Sort(byId(migrations))

	return migrations, nil
}

func migrationFromFile(dir http.FileSystem, root string, info os.FileInfo) (*Migration, error) {
	path := path.Join(root, info.Name())
	file, err := dir.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Error while opening %s: %w", info.Name(), err)
	}
	defer func() { _ = file.Close() }()

	migration, err := parseMigration(info.Name(), file)
	if err != nil {
		return nil, fmt.Errorf("Error while parsing %s: %w", info.Name(), err)
	}
	return migration, nil
}

// Migrations from a bindata asset set.
type AssetSource struct {
	// Asset should return content of file in path if exists
	Asset func(path string) ([]byte, error)

	// AssetDir should return list of files in the path
	AssetDir func(path string) ([]string, error)

	// Path in the bindata to use.
	Dir string
}

var _ Source = (*AssetSource)(nil)

func (a AssetSource) FindMigrations() ([]*Migration, error) {
	migrations := make([]*Migration, 0)

	files, err := a.AssetDir(a.Dir)
	if err != nil {
		return nil, err
	}

	for _, name := range files {
		if strings.HasSuffix(name, ".sql") {
			file, err := a.Asset(path.Join(a.Dir, name))
			if err != nil {
				return nil, err
			}

			migration, err := parseMigration(name, bytes.NewReader(file))
			if err != nil {
				return nil, err
			}

			migrations = append(migrations, migration)
		}
	}

	// Make sure migrations are sorted
	sort.Sort(byId(migrations))

	return migrations, nil
}

// A set of migrations loaded from an go1.16 embed.FS
type EmbedFileSystemSource struct {
	FileSystem embed.FS

	Root string
}

var _ Source = (*EmbedFileSystemSource)(nil)

func (f EmbedFileSystemSource) FindMigrations() ([]*Migration, error) {
	return findMigrations(http.FS(f.FileSystem), f.Root)
}

// Migrations from a packr box.
type PackrSource struct {
	Box PackrBox

	// Path in the box to use.
	Dir string
}

// Avoids pulling in the packr library for everyone, mimicks the bits of
// packr.Box that we need.
type PackrBox interface {
	List() []string
	Find(name string) ([]byte, error)
}

var _ Source = (*PackrSource)(nil)

func (p PackrSource) FindMigrations() ([]*Migration, error) {
	migrations := make([]*Migration, 0)
	items := p.Box.List()

	prefix := ""
	dir := path.Clean(p.Dir)
	if dir != "." {
		prefix = fmt.Sprintf("%s/", dir)
	}

	for _, item := range items {
		if !strings.HasPrefix(item, prefix) {
			continue
		}
		name := strings.TrimPrefix(item, prefix)
		if strings.Contains(name, "/") {
			continue
		}

		if strings.HasSuffix(name, ".sql") {
			file, err := p.Box.Find(item)
			if err != nil {
				return nil, err
			}

			migration, err := parseMigration(name, bytes.NewReader(file))
			if err != nil {
				return nil, err
			}

			migrations = append(migrations, migration)
		}
	}

	// Make sure migrations are sorted
	sort.Sort(byId(migrations))

	return migrations, nil
}

// parseMigration Migration parsing
func parseMigration(id string, r io.ReadSeeker) (*Migration, error) {
	m := &Migration{
		Id: id,
	}

	parsed, err := sqlparse.ParseMigration(r)
	if err != nil {
		return nil, fmt.Errorf("error parsing migration (%s): %w", id, err)
	}

	m.Up = parsed.UpStatements
	m.Down = parsed.DownStatements

	m.DisableTransactionUp = parsed.DisableTransactionUp
	m.DisableTransactionDown = parsed.DisableTransactionDown

	return m, nil
}

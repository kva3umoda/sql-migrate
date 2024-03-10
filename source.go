package migrate

import (
	`bytes`
	`embed`
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"

	`github.com/kva3umoda/sql-migrate/sqlparse`
)

type byId []*Migration

func (b byId) Len() int           { return len(b) }
func (b byId) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byId) Less(i, j int) bool { return b[i].Less(b[j]) }

type MigrationSource interface {
	// FindMigrations Finds the migrations.
	// The resulting slice of migrations should be sorted by Id.
	FindMigrations() ([]*Migration, error)
}

var _ MigrationSource = (*FileSystemMigrationSource)(nil)

type FileSystemMigrationSource struct {
	fs   http.FileSystem
	root string
}

// NewHttpFileSystemMigrationSource A set of migrations loaded from an http.FileServer
func NewHttpFileSystemMigrationSource(fs http.FileSystem) *FileSystemMigrationSource {
	return &FileSystemMigrationSource{
		fs:   fs,
		root: "/",
	}
}

// NewEmbedFileSystemMigrationSource A set of migrations loaded from an go1.16 embed.FS
func NewEmbedFileSystemMigrationSource(fs embed.FS, root string) *FileSystemMigrationSource {
	return &FileSystemMigrationSource{
		fs:   http.FS(fs),
		root: root,
	}
}

// NewFileSource A set of migrations loaded from a directory.
func NewFileMigrationSource(dir string) *FileSystemMigrationSource {
	return &FileSystemMigrationSource{
		fs:   http.Dir(dir),
		root: "/",
	}
}

func (fs *FileSystemMigrationSource) FindMigrations() ([]*Migration, error) {
	return fs.findMigrations(fs.fs, fs.root)
}

func (fs *FileSystemMigrationSource) findMigrations(dir http.FileSystem, root string) ([]*Migration, error) {
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
			migration, err := fs.migrationFromFile(dir, root, info)
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

func (fs *FileSystemMigrationSource) migrationFromFile(dir http.FileSystem, root string, info os.FileInfo) (*Migration, error) {
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

var _ MigrationSource = (*MemoryMigrationSource)(nil)

// MemoryMigrationSource A hardcoded set of migrations, in-memory.
type MemoryMigrationSource struct {
	Migrations []*Migration
}

// NewMemoryMigrationSource A hardcoded set of migrations, in-memory.
func NewMemoryMigrationSource(migrations []*Migration) *MemoryMigrationSource {
	return &MemoryMigrationSource{
		Migrations: migrations,
	}
}

func (m *MemoryMigrationSource) FindMigrations() ([]*Migration, error) {
	// Make sure migrations are sorted. In order to make the MemoryMigrationSource safe for
	// concurrent use we should not mutate it in place. So `FindMigrations` would sort a copy
	// of the m.Migrations.
	migrations := make([]*Migration, len(m.Migrations))
	copy(migrations, m.Migrations)
	sort.Sort(byId(migrations))

	return migrations, nil
}

var _ MigrationSource = (*AssetMigrationSource)(nil)

type AssetFunc func(path string) ([]byte, error)
type AssetDirFunc func(path string) ([]string, error)

// AssetMigrationSource Migrations from a bindata asset set.
type AssetMigrationSource struct {
	// Asset should return content of file in path if exists
	Asset AssetFunc
	// AssetDir should return list of files in the path
	AssetDir AssetDirFunc
	// Dir Path in the bindata to use.
	Dir string
}

func NewAssetMigrationSource(asset AssetFunc, assetDir AssetDirFunc, dir string) *AssetMigrationSource {
	return &AssetMigrationSource{
		Asset:    asset,
		AssetDir: assetDir,
		Dir:      dir,
	}
}
func (a *AssetMigrationSource) FindMigrations() ([]*Migration, error) {
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

package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var keyPattern = regexp.MustCompile(`^[a-f0-9]{32}-[0-9]{1,3}$`)

// faultHook is a same-package test seam for injecting filesystem failures
// during the staged commit protocol. Production uses real calls.
var faultHook func(op string) error

// Store is a content vault: content-addressed immutable objects plus
// workspaces that are promoted into named, opaque-keyed sets. It holds no
// domain knowledge; keys and their meaning are decided by the caller.
type Store struct{ Root string }

func Open(root string) (*Store, error) {
	for _, dir := range []string{"objects", "staging", "sets"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			return nil, err
		}
	}
	// Conservatively adopt sets committed by earlier builds under the legacy
	// physical directory name. Entries are moved, never deleted; a name that
	// already exists in sets is left in place as an orphan rather than
	// overwritten.
	legacy := filepath.Join(root, "renders")
	if entries, err := os.ReadDir(legacy); err == nil {
		for _, e := range entries {
			from := filepath.Join(legacy, e.Name())
			to := filepath.Join(root, "sets", e.Name())
			if err := os.Rename(from, to); err != nil && !os.IsExist(err) {
				return nil, err
			}
		}
	}
	return &Store{Root: root}, nil
}

// Object resolves a content digest to its immutable object path.
func (s *Store) Object(digest string) (string, error) {
	if !digestPattern.MatchString(digest) {
		return "", fmt.Errorf("invalid content digest")
	}
	return filepath.Join(s.Root, "objects", digest), nil
}

// Receive durably stores one upload under its content digest. Re-uploading
// identical bytes converges to the same object.
func (s *Store) Receive(r io.Reader, limit int64) (string, int64, error) {
	f, err := os.CreateTemp(filepath.Join(s.Root, "staging"), "upload-*")
	if err != nil {
		return "", 0, err
	}
	defer os.Remove(f.Name())
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(r, limit+1))
	if err != nil {
		f.Close()
		return "", n, err
	}
	if n == 0 || n > limit {
		f.Close()
		return "", n, fmt.Errorf("upload must contain 1..%d bytes", limit)
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return "", n, err
	}
	if err = f.Close(); err != nil {
		return "", n, err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	dest, _ := s.Object(digest)
	if err = os.Link(f.Name(), dest); err != nil && !os.IsExist(err) {
		return "", n, err
	}
	if err = syncDir(filepath.Dir(dest)); err != nil {
		return "", n, err
	}
	return digest, n, nil
}

// Workspace is a private staging directory for producing a set of files.
// Its path stays inside the vault boundary; callers only write files into it.
type Workspace struct {
	Path string
}

// NewWorkspace creates an empty private workspace.
func (s *Store) NewWorkspace() (Workspace, error) {
	if err := fail("new-workspace"); err != nil {
		return Workspace{}, err
	}
	dir, err := os.MkdirTemp(filepath.Join(s.Root, "staging"), "work-")
	if err != nil {
		return Workspace{}, err
	}
	return Workspace{Path: dir}, nil
}

// Discard removes a workspace that will not be committed. Orphaned
// workspaces are left in place on failure rather than auto-repaired.
func (s *Store) Discard(w Workspace) error {
	return os.RemoveAll(w.Path)
}

// SetLocation resolves an opaque set key to its committed directory.
func (s *Store) SetLocation(key string) (string, error) {
	if !keyPattern.MatchString(key) {
		return "", fmt.Errorf("invalid set key")
	}
	return filepath.Join(s.Root, "sets", key), nil
}

// CommitWorkspace fsyncs every workspace file, atomically promotes the
// workspace directory to the opaque key and syncs the parent directory.
// A workspace whose commit fails stays in staging as a conservative orphan.
func (s *Store) CommitWorkspace(w Workspace, key string) error {
	if !keyPattern.MatchString(key) {
		return fmt.Errorf("invalid set key")
	}
	entries, err := os.ReadDir(w.Path)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			return fmt.Errorf("unexpected nested workspace directory")
		}
		f, err := os.OpenFile(filepath.Join(w.Path, e.Name()), os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		err = f.Sync()
		f.Close()
		if err != nil {
			return err
		}
	}
	if err = syncDir(w.Path); err != nil {
		return err
	}
	dest, err := s.SetLocation(key)
	if err != nil {
		return err
	}
	if err = fail("promote"); err != nil {
		return err
	}
	if err = os.Rename(w.Path, dest); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dest))
}

func fail(op string) error {
	if faultHook != nil {
		return faultHook(op)
	}
	return nil
}

func syncDir(path string) error {
	if err := fail("syncdir"); err != nil {
		return err
	}
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

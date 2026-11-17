package blob

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openVault(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open vault: %v", err)
	}
	return s
}

func TestReceiveConvergesByDigest(t *testing.T) {
	s := openVault(t)
	payload := bytes.Repeat([]byte("cadence-"), 8)
	d1, n1, err := s.Receive(bytes.NewReader(payload), 1024)
	if err != nil {
		t.Fatal(err)
	}
	d2, n2, err := s.Receive(bytes.NewReader(payload), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if d1 != d2 || n1 != n2 || n1 != int64(len(payload)) {
		t.Fatalf("identical bytes did not converge: %s/%d vs %s/%d", d1, n1, d2, n2)
	}
	path, err := s.Object(d1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("object content mismatch: %v", err)
	}
}

func TestReceiveRejectsEmptyAndOversized(t *testing.T) {
	s := openVault(t)
	if _, _, err := s.Receive(bytes.NewReader(nil), 10); err == nil {
		t.Fatal("empty upload accepted")
	}
	if _, _, err := s.Receive(bytes.NewReader(bytes.Repeat([]byte("x"), 11)), 10); err == nil {
		t.Fatal("oversized upload accepted")
	}
}

func TestObjectRejectsNonDigest(t *testing.T) {
	s := openVault(t)
	if _, err := s.Object("../etc/passwd"); err == nil {
		t.Fatal("non-digest key accepted")
	}
}

func TestWorkspaceCommitAndLocate(t *testing.T) {
	s := openVault(t)
	w, err := s.NewWorkspace()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(w.Path, "composition.mp4"), []byte("media"), 0600); err != nil {
		t.Fatal(err)
	}
	key := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1"
	if err = s.CommitWorkspace(w, key); err != nil {
		t.Fatalf("commit workspace: %v", err)
	}
	dir, err := s.SetLocation(key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "composition.mp4"))
	if err != nil || string(got) != "media" {
		t.Fatalf("committed set content mismatch: %v", err)
	}
	// The staging directory was promoted away.
	if _, err = os.Stat(w.Path); !os.IsNotExist(err) {
		t.Fatal("workspace still present after commit")
	}
}

func TestSetKeyValidation(t *testing.T) {
	s := openVault(t)
	for _, bad := range []string{"", "x-1", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "aaaa-0", "../escape-1"} {
		if _, err := s.SetLocation(bad); err == nil {
			t.Fatalf("bad set key accepted: %q", bad)
		}
		w, err := s.NewWorkspace()
		if err != nil {
			t.Fatal(err)
		}
		if err = s.CommitWorkspace(w, bad); err == nil {
			t.Fatalf("bad commit key accepted: %q", bad)
		}
		_ = s.Discard(w)
	}
}

func TestCommitFailureLeavesConservativeOrphan(t *testing.T) {
	s := openVault(t)
	w, err := s.NewWorkspace()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(w.Path, "f.bin"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	faultHook = func(op string) error {
		if op == "promote" {
			return errors.New("simulated promote failure")
		}
		return nil
	}
	t.Cleanup(func() { faultHook = nil })

	key := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-2"
	if err = s.CommitWorkspace(w, key); err == nil {
		t.Fatal("promote failure not reported")
	}
	// The set must not appear committed at its destination.
	dir, _ := s.SetLocation(key)
	if _, err = os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("failed commit exposed a set as committed")
	}
	// The workspace is conservatively retained, not auto-repaired.
	if _, err = os.Stat(w.Path); err != nil {
		t.Fatalf("orphan workspace should be retained on failure: %v", err)
	}
}

func TestDiscardWorkspace(t *testing.T) {
	s := openVault(t)
	w, err := s.NewWorkspace()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Discard(w); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(w.Path); !os.IsNotExist(err) {
		t.Fatal("discard did not remove workspace")
	}
}

func TestCommitRejectsNestedDirectories(t *testing.T) {
	s := openVault(t)
	w, _ := s.NewWorkspace()
	if err := os.Mkdir(filepath.Join(w.Path, "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitWorkspace(w, "cccccccccccccccccccccccccccccccc-1"); err == nil {
		t.Fatal("nested directory in workspace accepted")
	}
}

func TestOpenAdoptsLegacyRenderDirectory(t *testing.T) {
	root := t.TempDir()
	legacy := filepath.Join(root, "renders")
	if err := os.MkdirAll(filepath.Join(legacy, "dddddddddddddddddddddddddddddddd-1"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "dddddddddddddddddddddddddddddddd-1", "composition.mp4"), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(root)
	if err != nil {
		t.Fatalf("open with legacy dir: %v", err)
	}
	dir, err := s.SetLocation("dddddddddddddddddddddddddddddddd-1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "composition.mp4"))
	if err != nil || string(got) != "old" {
		t.Fatalf("legacy set not adopted: %v", err)
	}
	if entries, _ := os.ReadDir(legacy); len(entries) != 0 {
		t.Fatal("legacy directory entries not moved")
	}
}

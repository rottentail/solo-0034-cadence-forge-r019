package persist

import (
	"bytes"
	"cadenceforge/internal/domain"
	"cadenceforge/internal/journal"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func openGateway(t *testing.T) *Gateway {
	t.Helper()
	g, _, err := OpenRoot(t.TempDir())
	if err != nil {
		t.Fatalf("open gateway: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

func TestCommitPassesDomainErrorsThrough(t *testing.T) {
	g := openGateway(t)
	want := domain.Conflict("business rule")
	_, err := g.Commit(func(*domain.State) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("domain error not passed through: %v", err)
	}
	if f, ok := AsFault(err); ok {
		t.Fatalf("domain error wrapped as persistence fault: %+v", f)
	}
}

func TestCommitAndSnapshotRoundTrip(t *testing.T) {
	g := openGateway(t)
	changed, err := g.Commit(func(st *domain.State) error {
		st.Emit("x", "k", "d")
		return nil
	})
	if err != nil || !changed {
		t.Fatalf("commit: changed=%v err=%v", changed, err)
	}
	snap, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Sequence != 1 {
		t.Fatalf("snapshot sequence=%d", snap.Sequence)
	}
}

// TestDurabilityFaultTranslation proves the gateway maps the adapter's two
// durability outcomes onto distinct application-facing faults instead of a
// raw OS error or a generic boolean.
func TestDurabilityFaultTranslation(t *testing.T) {
	uncertain := journal.NewDurabilityFault(journal.UncertainCommit, "sync state directory", errors.New("disk"))
	translated := translate("commit", uncertain)
	if !IsUncertain(translated) {
		t.Fatalf("uncertain fault not translated: %v", translated)
	}
	if f, ok := AsFault(translated); !ok || !errors.Is(f, UncertainCommit) {
		t.Fatalf("translation lost type: %v", translated)
	}

	unavail := journal.NewDurabilityFault(journal.StoreUnavailable, "write", errors.New("poison"))
	if IsUncertain(translate("commit", unavail)) {
		t.Fatal("store-unavailable mistaken for uncertain commit")
	}
	if f, ok := AsFault(translate("commit", unavail)); !ok || !errors.Is(f, StoreUnavailable) {
		t.Fatalf("unavailable mapping wrong: %v", unavail)
	}
}

// TestGatewayOpensWithCleanReconcileReport proves a fresh root reports no
// unconfirmed commit. The recovered/rolled-back branches themselves are
// proven against the private fault copy in the journal package's own tests.
func TestGatewayOpensWithCleanReconcileReport(t *testing.T) {
	g, report, err := OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if report.Recovered || report.RolledBack {
		t.Fatalf("fresh root reported a reconciliation: %+v", report)
	}
}

func TestReceiveAndLeaseRoundTrip(t *testing.T) {
	g := openGateway(t)
	payload := bytes.Repeat([]byte("object-"), 4)
	digest, n, err := g.Receive(bytes.NewReader(payload), 4096)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("size=%d", n)
	}
	path, err := g.Lease(digest)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("lease content mismatch: %v", err)
	}
}

// TestWorkspaceTwoPhaseCommit proves the boundary sequence: workspace files
// become durable under an opaque key, and reads locate exactly that set.
func TestWorkspaceTwoPhaseCommit(t *testing.T) {
	g := openGateway(t)
	w, err := g.BeginWorkspace()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(w.Dir, "composition.mp4"), []byte("bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	key, err := SetKey("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.CommitWorkspace(w, key); err != nil {
		t.Fatalf("phase 1 commit: %v", err)
	}
	dir, err := g.SetLocation(key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "composition.mp4"))
	if err != nil || string(got) != "bytes" {
		t.Fatalf("committed set mismatch: %v", err)
	}
}

func TestAbandonWorkspaceRemovesIt(t *testing.T) {
	g := openGateway(t)
	w, err := g.BeginWorkspace()
	if err != nil {
		t.Fatal(err)
	}
	if err = g.AbandonWorkspace(w); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(w.Dir); !os.IsNotExist(err) {
		t.Fatal("abandoned workspace still present")
	}
}

func TestSetKeyDerivation(t *testing.T) {
	id := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if got, err := SetKey(id, 3); err != nil || got != id+"-3" {
		t.Fatalf("key derivation: %s %v", got, err)
	}
	if _, err := SetKey("short", 1); err == nil {
		t.Fatal("short identity accepted")
	}
	if _, err := SetKey(id, 0); err == nil {
		t.Fatal("zero revision accepted")
	}
}

// fakeVault lets gateway tests observe Vault errors without touching disk.
type fakeVault struct{ receiveErr error }

func (f *fakeVault) Receive(io.Reader, int64) (string, int64, error) {
	return "", 0, f.receiveErr
}
func (f *fakeVault) ObjectPath(string) (string, error)       { return "", nil }
func (f *fakeVault) NewWorkspace() (Workspace, error)        { return Workspace{}, nil }
func (f *fakeVault) Discard(Workspace) error                 { return nil }
func (f *fakeVault) CommitWorkspace(Workspace, string) error { return nil }
func (f *fakeVault) SetPath(string) (string, error)          { return "", nil }

func TestVaultErrorIsTranslatedNotLeaked(t *testing.T) {
	g := &Gateway{vault: &fakeVault{receiveErr: errors.New("raw io detail")}}
	_, _, err := g.Receive(bytes.NewReader([]byte("x")), 10)
	if err == nil {
		t.Fatal("expected error")
	}
	if f, ok := AsFault(err); !ok {
		t.Fatalf("vault error not presented as persistence fault: %v", err)
	} else if errors.Is(f, UncertainCommit) {
		t.Fatal("ordinary vault error mislabeled uncertain")
	}
}

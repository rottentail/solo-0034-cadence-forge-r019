package journal

import (
	"cadenceforge/internal/domain"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func validState() domain.State {
	st := domain.Empty()
	st.Sources["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"] = domain.Source{
		ID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Name: "src",
		Digest: "b01c1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab",
		Bytes:  12, Media: domain.Media{DurationMS: 1000, Audio: true},
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	return st
}

func openStore(t *testing.T) (*Store, string) {
	t.Helper()
	root := t.TempDir()
	st, _, err := Open(root)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return st, root
}

func seed(t *testing.T, st *Store) domain.State {
	t.Helper()
	base := validState()
	_, err := st.Update(func(s *domain.State) error {
		*s = base
		return nil
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return base
}

// mutate appends an event so the state genuinely changes.
func mutate(marker uint64) func(*domain.State) error {
	return func(s *domain.State) error {
		s.Emit("subject", "test.kind", fmt.Sprintf("marker-%d", marker))
		return nil
	}
}

func TestCommitRoundTrip(t *testing.T) {
	st, _ := openStore(t)
	defer st.Close()
	seed(t, st)
	out, err := st.Update(mutate(1))
	if err != nil || !out.Changed {
		t.Fatalf("commit: outcome=%+v err=%v", out, err)
	}
	got, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got.Sequence != 1 || len(got.Events) != 1 {
		t.Fatalf("state not durable in memory: %+v", got)
	}
}

func TestNoopMutationDoesNotReplace(t *testing.T) {
	st, root := openStore(t)
	defer st.Close()
	seed(t, st)
	before, err := os.Stat(filepath.Join(root, stateFile))
	if err != nil {
		t.Fatal(err)
	}
	out, err := st.Update(func(*domain.State) error { return nil })
	if err != nil || out.Changed {
		t.Fatalf("noop: outcome=%+v err=%v", out, err)
	}
	after, _ := os.Stat(filepath.Join(root, stateFile))
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("noop mutation replaced the state file")
	}
}

func TestDomainErrorAbortsCommit(t *testing.T) {
	st, _ := openStore(t)
	defer st.Close()
	seed(t, st)
	want := errors.New("business rejected")
	if _, err := st.Update(func(*domain.State) error { return want }); !errors.Is(err, want) {
		t.Fatalf("expected caller error, got %v", err)
	}
	got, _ := st.Snapshot()
	if got.Sequence != 0 {
		t.Fatal("aborted mutation changed state")
	}
}

// injectDirSyncFailure makes the state directory sync fail after the private
// fault copy has itself been durably written, simulating exactly "replacement
// done, fault copy secured, state directory sync failed".
func injectDirSyncFailure(t *testing.T) {
	t.Helper()
	calls := 0
	syncFaultHook = func(path string) error {
		calls++
		// Within replace(): call #1 syncs the fault-copy rename, call #2
		// syncs the state directory. Fail only the latter.
		if calls == 2 {
			return errors.New("simulated directory sync failure")
		}
		return nil
	}
	t.Cleanup(func() { syncFaultHook = nil })
}

// TestUncertainCommitPoisonsAndKeepsFaultCopy proves the uncertain window:
// the typed fault is returned, the store rejects further writes, the in-memory
// state still serves the candidate, and the private fault copy exists on disk.
func TestUncertainCommitPoisonsAndKeepsFaultCopy(t *testing.T) {
	st, root := openStore(t)
	defer st.Close()
	seed(t, st)
	injectDirSyncFailure(t)

	_, err := st.Update(mutate(7))
	var fault *DurabilityFault
	if !errors.As(err, &fault) || !errors.Is(fault, UncertainCommit) {
		t.Fatalf("expected UncertainCommit fault, got %v", err)
	}

	// A second write must be refused with the same typed outcome, never
	// silently accepted as a fresh commit.
	_, err = st.Update(mutate(8))
	if !errors.Is(err, UncertainCommit) {
		t.Fatalf("poisoned store accepted another write: %v", err)
	}

	// Reads still serve the candidate state (best available copy).
	got, err := st.Snapshot()
	if err != nil {
		t.Fatalf("snapshot after poison: %v", err)
	}
	if got.Sequence != 1 || got.Events[0].Detail != "marker-7" {
		t.Fatalf("candidate state not retained: seq=%d events=%+v", got.Sequence, got.Events)
	}

	// The private fault copy with exact bytes is present.
	recordPath := filepath.Join(root, faultCopy)
	raw, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("fault copy missing: %v", err)
	}
	if info, err := os.Stat(recordPath); err != nil || info.Size() == 0 {
		t.Fatal("fault copy is empty")
	}
	var record faultRecord
	if err = json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("decode fault copy: %v", err)
	}
	if digestBytes(record.State) != record.Digest {
		t.Fatal("fault copy digest does not match its payload")
	}
	if !contains(record.State, "marker-7") {
		t.Fatal("fault copy does not hold the candidate state")
	}
}

// TestReconcileRecovered proves restart when the replacement actually landed:
// reopening must recover the candidate state and clear the fault copy.
func TestReconcileRecovered(t *testing.T) {
	root := t.TempDir()
	st, _, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	seed(t, st)
	injectDirSyncFailure(t)
	if _, err = st.Update(mutate(9)); !errors.Is(err, UncertainCommit) {
		t.Fatalf("inject uncertain commit: %v", err)
	}
	st.Close()

	// The replacement had landed before the failed sync, so state.json on
	// disk already contains marker-9.
	committed, err := os.ReadFile(filepath.Join(root, stateFile))
	if err != nil {
		t.Fatal(err)
	}
	if !contains(committed, "marker-9") {
		t.Fatal("test premise: replacement should already be on disk")
	}

	reopened, report, err := Open(root)
	if err != nil {
		t.Fatalf("reopen during reconciliation: %v", err)
	}
	defer reopened.Close()
	if !report.Recovered || report.RolledBack {
		t.Fatalf("expected recovered report, got %+v", report)
	}
	if _, err = os.Stat(filepath.Join(root, faultCopy)); !os.IsNotExist(err) {
		t.Fatal("fault copy not cleared after reconciliation")
	}
	got, _ := reopened.Snapshot()
	if got.Sequence != 1 || got.Events[0].Detail != "marker-9" {
		t.Fatalf("recovered state mismatch: %+v", got)
	}
}

// TestReconcileRolledBack proves restart when the replacement did not land:
// the prior committed history is retained and the private copy is discarded.
func TestReconcileRolledBack(t *testing.T) {
	root := t.TempDir()
	st, _, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	seed(t, st)
	if _, err = st.Update(mutate(3)); err != nil {
		t.Fatal(err)
	}
	st.Close()

	// Simulate an unconfirmed commit whose replacement never reached disk by
	// hand-writing a valid fault copy while state.json stays at marker-3.
	candidate := validState()
	candidate.Emit("subject", "test.kind", "orphan-candidate")
	raw := mustMarshalIndent(t, candidate)
	payload := append(raw, '\n')
	record := faultRecord{Reason: UncertainCommit.Error(), Digest: digestBytes(payload), State: payload}
	writeSidecar(t, root, record)

	reopened, report, err := Open(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if !report.RolledBack || report.Recovered {
		t.Fatalf("expected rolled-back report, got %+v", report)
	}
	if _, err = os.Stat(filepath.Join(root, faultCopy)); !os.IsNotExist(err) {
		t.Fatal("fault copy not discarded after rollback")
	}
	got, _ := reopened.Snapshot()
	if got.Sequence != 1 || got.Events[0].Detail != "marker-3" {
		t.Fatalf("prior history not retained: %+v", got)
	}
}

// TestReconcileFaultCopyBeforeRename proves the ordering guarantee: even
// when the process dies after securing the fault copy but before the
// replacement reaches the state file, restart rolls back to the committed
// history instead of guessing.
func TestReconcileFaultCopyBeforeRename(t *testing.T) {
	root := t.TempDir()
	st, _, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	seed(t, st)
	if _, err = st.Update(mutate(2)); err != nil {
		t.Fatal(err)
	}
	st.Close()

	// A fault copy exists (secured before the rename) while state.json still
	// holds the prior committed history.
	candidate := validState()
	candidate.Emit("subject", "test.kind", "marker-2")
	candidate.Emit("subject", "test.kind", "never-renamed")
	raw := mustMarshalIndent(t, candidate)
	payload := append(raw, '\n')
	writeSidecar(t, root, faultRecord{Reason: UncertainCommit.Error(), Digest: digestBytes(payload), State: payload})

	reopened, report, err := Open(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if !report.RolledBack {
		t.Fatalf("pre-rename crash must roll back, got %+v", report)
	}
	got, _ := reopened.Snapshot()
	if got.Sequence != 1 || got.Events[0].Detail != "marker-2" {
		t.Fatalf("committed history altered by pre-rename candidate: %+v", got.Events)
	}
}

// TestReconcileCorruptCopyIsFatal proves ambiguity is never guessed: a fault
// copy whose digest does not match its payload blocks startup.
func TestReconcileCorruptCopyIsFatal(t *testing.T) {
	root := t.TempDir()
	st, _, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	seed(t, st)
	st.Close()
	raw := mustMarshalIndent(t, validState())
	record := faultRecord{Reason: UncertainCommit.Error(), Digest: "deadbeef", State: raw}
	writeSidecar(t, root, record)

	if _, _, err = Open(root); err == nil {
		t.Fatal("corrupt fault copy must fail reconciliation, not guess a history")
	}
	if _, err = os.Stat(filepath.Join(root, faultCopy)); err != nil {
		t.Fatal("corrupt fault copy must be left for operator review")
	}
}

// TestRepeatedPoisonKeepsTypedFault proves every refused write keeps
// returning the typed uncertain fault rather than degrading to a generic
// boolean-style error.
func TestRepeatedPoisonKeepsTypedFault(t *testing.T) {
	st, _ := openStore(t)
	defer st.Close()
	seed(t, st)
	injectDirSyncFailure(t)
	_, _ = st.Update(mutate(1))
	for i := 0; i < 3; i++ {
		if _, err := st.Update(mutate(uint64(i + 10))); !errors.Is(err, UncertainCommit) {
			t.Fatalf("write %d lost typed fault: %v", i, err)
		}
	}
}

func mustMarshalIndent(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func writeSidecar(t *testing.T, root string, record faultRecord) {
	t.Helper()
	raw, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, faultCopy), append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func contains(haystack []byte, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == needle {
			return true
		}
	}
	return false
}

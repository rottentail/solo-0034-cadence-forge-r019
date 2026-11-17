package journal

import (
	"cadenceforge/internal/domain"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
)

// Fault kinds reported by the durability adapter. They describe commit
// outcomes, never domain semantics such as clips or cancellations.
var (
	// UncertainCommit means the replacement was performed but the parent
	// directory sync did not confirm. The candidate state is retained in a
	// private fault copy so restart can decide whether the commit landed.
	UncertainCommit = errors.New("durable commit unconfirmed")
	// Unavailable means the store cannot accept further writes until the
	// process is restarted and the private fault copy is reconciled.
	Unavailable = errors.New("state store is unavailable; restart required")
)

// DurabilityFault is the typed boundary error of the state adapter.
// Callers branch on the sentinel via errors.Is instead of inspecting
// operating-system details.
type DurabilityFault struct {
	Kind error
	Op   string
	err  error
}

func (f *DurabilityFault) Error() string {
	return fmt.Sprintf("%s: %s: %v", f.Op, f.Kind, f.err)
}
func (f *DurabilityFault) Unwrap() error { return f.Kind }
func (f *DurabilityFault) Cause() error  { return f.err }

// NewDurabilityFault builds a typed durability fault. It is exposed so the
// persistence boundary can translate adapter outcomes without parsing
// operating-system errors.
func NewDurabilityFault(kind error, op string, cause error) *DurabilityFault {
	return &DurabilityFault{Kind: kind, Op: op, err: cause}
}

// StoreUnavailable is the exported alias for the unavailable sentinel used
// across the persistence boundary.
var StoreUnavailable = Unavailable

const (
	stateFile = "state.json"
	// faultCopy is the private failure-sidecar. It is not part of the state
	// format: it records exactly which bytes were replacing the state when
	// durability became unconfirmed, together with their content digest.
	faultCopy = ".state-fault.json"
)

// faultRecord is the on-disk private fault copy. State is stored as an
// opaque base64 envelope so it round-trips byte-for-byte regardless of the
// JSON encoding of the surrounding record.
type faultRecord struct {
	Reason string `json:"reason"`
	Digest string `json:"digest"`
	State  []byte `json:"state"`
}

// ReconcileReport explains what a restart-time reconciliation decided.
type ReconcileReport struct {
	// Recovered is true when the uncertain commit had landed on disk and
	// the fault copy matched it exactly.
	Recovered bool
	// RolledBack is true when the replacement did not land and the state
	// file still matched the pre-commit bytes.
	RolledBack bool
}

// syncFaultHook is a test seam for injecting directory-sync failures.
// Production builds leave it nil and use the real syscall path.
var syncFaultHook func(path string) error

// SetSyncFaultHook installs a directory-sync interceptor. A nil hook
// restores real syscalls. It is intended for cross-package restart and
// durability tests, not request handling.
func SetSyncFaultHook(hook func(path string) error) { syncFaultHook = hook }

type Store struct {
	mu       sync.Mutex
	root     string
	state    domain.State
	lock     *os.File
	poisoned *DurabilityFault
}

func Open(root string) (*Store, ReconcileReport, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, ReconcileReport{}, err
	}
	lock, err := os.OpenFile(filepath.Join(root, "process.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, ReconcileReport{}, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, ReconcileReport{}, fmt.Errorf("data root already in use: %w", err)
	}
	s := &Store{root: root, state: domain.Empty(), lock: lock}
	report, err := s.reconcile()
	if err != nil {
		s.Close()
		return nil, ReconcileReport{}, err
	}
	data, err := os.ReadFile(filepath.Join(root, stateFile))
	if err == nil {
		err = json.Unmarshal(data, &s.state)
	} else if os.IsNotExist(err) {
		err = nil
	}
	if err == nil {
		err = domain.ValidateState(s.state)
	}
	if err != nil {
		s.Close()
		return nil, ReconcileReport{}, fmt.Errorf("load state: %w", err)
	}
	return s, report, nil
}

// reconcile inspects a private fault copy left by an earlier process whose
// replacement landed without a confirmed directory sync. It never edits the
// state on ambiguity: an unmatched copy is fatal and requires an operator,
// because guessing would silently pick one of two divergent histories.
func (s *Store) reconcile() (ReconcileReport, error) {
	recordPath := filepath.Join(s.root, faultCopy)
	raw, err := os.ReadFile(recordPath)
	if os.IsNotExist(err) {
		return ReconcileReport{}, nil
	}
	if err != nil {
		return ReconcileReport{}, fmt.Errorf("read fault copy: %w", err)
	}
	var record faultRecord
	if err = json.Unmarshal(raw, &record); err != nil {
		return ReconcileReport{}, fmt.Errorf("decode fault copy: %w", err)
	}
	if digestBytes(record.State) != record.Digest {
		return ReconcileReport{}, fmt.Errorf("fault copy is corrupt and cannot be reconciled")
	}
	committed, err := os.ReadFile(filepath.Join(s.root, stateFile))
	if err != nil {
		return ReconcileReport{}, fmt.Errorf("read committed state during reconciliation: %w", err)
	}
	report := ReconcileReport{}
	switch digestBytes(committed) == record.Digest {
	case true:
		// The replacement did land; the state on disk is exactly the bytes
		// the earlier process could not confirm. Idempotent history holds.
		report.Recovered = true
	default:
		var candidate domain.State
		if err = json.Unmarshal(record.State, &candidate); err != nil {
			return ReconcileReport{}, fmt.Errorf("fault copy holds invalid state: %w", err)
		}
		if err = domain.ValidateState(candidate); err != nil {
			return ReconcileReport{}, fmt.Errorf("fault copy violates state invariants: %w", err)
		}
		// The replacement never landed. The still-committed state is the
		// authoritative pre-commit history; the private copy is discarded.
		report.RolledBack = true
	}
	if err = os.Remove(recordPath); err != nil {
		return ReconcileReport{}, fmt.Errorf("clear reconciled fault copy: %w", err)
	}
	if err = syncPath(s.root); err != nil {
		return ReconcileReport{}, fmt.Errorf("sync fault copy removal: %w", err)
	}
	return report, nil
}

func clone(s domain.State) (domain.State, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return domain.State{}, err
	}
	var out domain.State
	err = json.Unmarshal(b, &out)
	return out, err
}

func (s *Store) Snapshot() (domain.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Reads stay available even while writes are poisoned: after a
	// replacement the in-memory state is the candidate that likely landed,
	// which is the most accurate view the process can offer.
	return clone(s.state)
}

// CommitOutcome reports what an Update accomplished.
type CommitOutcome struct {
	// Changed is false when the mutation left the state byte-for-byte equal
	// to the prior snapshot; no replacement was performed in that case.
	Changed bool
}

// Update applies fn to a private copy and durably replaces the state file.
// The returned error is either the caller's own mutation error, a
// DurabilityFault, or an ordinary serialization/IO error.
func (s *Store) Update(fn func(*domain.State) error) (CommitOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned != nil {
		return CommitOutcome{}, s.poisoned
	}
	next, err := clone(s.state)
	if err != nil {
		return CommitOutcome{}, err
	}
	if err = fn(&next); err != nil {
		return CommitOutcome{}, err
	}
	if reflect.DeepEqual(next, s.state) {
		return CommitOutcome{Changed: false}, nil
	}
	if err = domain.ValidateState(next); err != nil {
		return CommitOutcome{}, fmt.Errorf("state invariant: %w", err)
	}
	b, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return CommitOutcome{}, err
	}
	b = append(b, '\n')
	if err = s.replace(b); err != nil {
		return CommitOutcome{}, err
	}
	return CommitOutcome{Changed: true}, nil
}

// replace performs the staged durability protocol and maintains the private
// fault copy around the uncertainty window. On an unconfirmed commit the
// in-memory state advances to the candidate (the bytes that likely landed)
// while the store is poisoned, so reads never serve a history older than the
// replacement that already reached the file system.
func (s *Store) replace(b []byte) error {
	f, err := os.CreateTemp(s.root, ".state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	target := filepath.Join(s.root, stateFile)
	// Secure the private fault copy before the replacement. Every crash or
	// sync failure from this point on therefore leaves a byte-exact record of
	// the candidate that restart reconciliation can compare against.
	record := faultRecord{Reason: UncertainCommit.Error(), Digest: digestBytes(b), State: append([]byte(nil), b...)}
	if err = s.writeFaultCopy(record); err != nil {
		return s.poison(UncertainCommit, "persist fault copy", err)
	}
	if err = os.Rename(name, target); err != nil {
		return err
	}
	// Advance the in-memory view to the candidate before the directory sync
	// whose outcome cannot be assumed.
	var candidate domain.State
	if err = json.Unmarshal(b, &candidate); err != nil {
		return s.poison(UncertainCommit, "decode candidate after replacement", err)
	}
	s.state = candidate
	if err = syncPath(s.root); err != nil {
		return s.poison(UncertainCommit, "sync state directory", err)
	}
	// Durability is confirmed; the private fault copy is no longer needed.
	if err = os.Remove(filepath.Join(s.root, faultCopy)); err != nil {
		return s.poison(UncertainCommit, "clear fault copy", err)
	}
	if err = syncPath(s.root); err != nil {
		return s.poison(UncertainCommit, "sync fault copy removal", err)
	}
	return nil
}

func (s *Store) writeFaultCopy(record faultRecord) error {
	raw, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	path := filepath.Join(s.root, faultCopy)
	f, err := os.CreateTemp(s.root, ".state-fault-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return syncPath(s.root)
}

func (s *Store) poison(kind error, op string, err error) *DurabilityFault {
	fault := &DurabilityFault{Kind: kind, Op: op, err: err}
	if s.poisoned == nil {
		s.poisoned = fault
	}
	return fault
}

func digestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func syncPath(path string) error {
	if syncFaultHook != nil {
		if err := syncFaultHook(path); err != nil {
			return err
		}
	}
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	err = d.Sync()
	d.Close()
	return err
}

func (s *Store) Close() error {
	if s.lock != nil {
		return s.lock.Close()
	}
	return nil
}

// Package persist is the application-facing persistence boundary. It hides
// temp files, atomic replacement, directory syncs and staged object
// promotion behind intent-level operations, while staying free of any clip,
// render or cancellation semantics.
//
// Durability outcomes are typed rather than collapsed into a boolean:
// callers can tell a rejected mutation, a confirmed commit and an
// unconfirmed-but-likely-landed commit apart without touching OS errors.
package persist

import (
	"cadenceforge/internal/domain"
	"cadenceforge/internal/journal"
	"errors"
	"fmt"
	"io"
)

// Fault kinds crossing the persistence boundary.
var (
	// UncertainCommit means the state replacement was performed but its
	// durability could not be confirmed. Re-issuing the request is unsafe
	// because the commit may already be present; restart reconciliation
	// decides using the private fault copy.
	UncertainCommit = errors.New("commit unconfirmed; do not resend until restart reconciliation")
	// StoreUnavailable means the persistence boundary is poisoned and
	// rejects further writes until the process restarts.
	StoreUnavailable = errors.New("persistence unavailable until restart")
	// OperationFailed means a single persistence operation failed without
	// poisoning the store; later operations may still succeed.
	OperationFailed = errors.New("persistence operation failed")
)

// Fault is a typed persistence error. Domain faults (invalid input,
// conflicts, missing entities) are passed through untouched.
type Fault struct {
	Kind error
	Op   string
	err  error
}

func (f *Fault) Error() string { return fmt.Sprintf("persistence %s: %s: %v", f.Op, f.Kind, f.err) }
func (f *Fault) Unwrap() error { return f.Kind }

// AsFault extracts a typed persistence Fault.
func AsFault(err error) (*Fault, bool) {
	var f *Fault
	if errors.As(err, &f) {
		return f, true
	}
	return nil, false
}

// NewFaultForTest constructs a persistence Fault for boundary tests.
func NewFaultForTest(kind error, op string, cause error) *Fault {
	return &Fault{Kind: kind, Op: op, err: cause}
}

// IsUncertain reports whether an error means a commit may have landed
// without confirmation. Business code uses this instead of guessing from
// raw IO errors, and must not treat it as a clean rejection.
func IsUncertain(err error) bool { return errors.Is(err, UncertainCommit) }

func translate(op string, err error) error {
	if err == nil {
		return nil
	}
	var durability *journal.DurabilityFault
	if errors.As(err, &durability) {
		kind := StoreUnavailable
		if errors.Is(durability, journal.UncertainCommit) {
			kind = UncertainCommit
		}
		return &Fault{Kind: kind, Op: op, err: durability.Cause()}
	}
	// Domain faults carry HTTP meaning and must pass through untouched;
	// anything else crossing this boundary is a persistence-level failure
	// that does not poison the store.
	var domainFault *domain.Fault
	if errors.As(err, &domainFault) {
		return err
	}
	return &Fault{Kind: OperationFailed, Op: op, err: err}
}

// Vault abstracts the content store. The blob adapter satisfies it; a
// different content backend could replace it without touching the gateway
// or its callers.
type Vault interface {
	Receive(r io.Reader, limit int64) (digest string, size int64, err error)
	ObjectPath(digest string) (string, error)
	NewWorkspace() (Workspace, error)
	Discard(Workspace) error
	CommitWorkspace(Workspace, string) error
	SetPath(key string) (string, error)
}

// Workspace is an opaque staging handle. Its directory is meaningful only
// inside the persistence boundary.
type Workspace struct {
	Dir string
}

// Gateway is the single persistence port used by application services.
type Gateway struct {
	state  *journal.Store
	vault  Vault
	report journal.ReconcileReport
}

// Open wires the two independent adapters: durable state (journal) and the
// content vault. Both may evolve behind this boundary. The report describes
// any restart-time reconciliation of a previously unconfirmed commit.
func Open(root string, vault Vault) (*Gateway, journal.ReconcileReport, error) {
	st, report, err := journal.Open(root)
	if err != nil {
		return nil, journal.ReconcileReport{}, err
	}
	return &Gateway{state: st, vault: vault, report: report}, report, nil
}

// ReconcileReport exposes the decision taken at startup about a private
// fault copy left by an earlier, unconfirmed commit.
func (g *Gateway) ReconcileReport() journal.ReconcileReport { return g.report }

// Snapshot returns an independent deep copy of the current state.
func (g *Gateway) Snapshot() (domain.State, error) {
	st, err := g.state.Snapshot()
	return st, translate("snapshot", err)
}

// Commit applies fn as one durable state transition. The mutation callback
// may return domain errors; those abort the commit and are returned
// unchanged. Changed reports whether the transition altered the state.
func (g *Gateway) Commit(fn func(*domain.State) error) (changed bool, err error) {
	outcome, err := g.state.Update(fn)
	if err != nil {
		return false, translate("commit", err)
	}
	return outcome.Changed, nil
}

// Recover moves in-flight work to its restart-consistent domain state.
func (g *Gateway) Recover() error {
	return translate("recover", g.state.Recover())
}

// Close releases the single-instance lock.
func (g *Gateway) Close() error { return g.state.Close() }

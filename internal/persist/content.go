package persist

import (
	"errors"
	"fmt"
	"io"
)

// Content and workspace operations of the persistence boundary. These
// methods speak digests, leases and opaque set keys; they never reference
// renders, attempts or cancellation, which remain application decisions.

// Receive stores one bounded upload and returns its content identity.
// Identical bytes converge to the same digest.
func (g *Gateway) Receive(r io.Reader, limit int64) (string, int64, error) {
	digest, size, err := g.vault.Receive(r, limit)
	return digest, size, translate("receive", err)
}

// Lease opens read access to an immutable object for the duration of a
// local media operation. The returned path is a lease, not a durable
// location the caller may store or reason about across requests.
func (g *Gateway) Lease(digest string) (string, error) {
	path, err := g.vault.ObjectPath(digest)
	if err != nil {
		return "", translate("lease", err)
	}
	return path, nil
}

// BeginWorkspace creates a private workspace for producing files.
func (g *Gateway) BeginWorkspace() (Workspace, error) {
	w, err := g.vault.NewWorkspace()
	if err != nil {
		return Workspace{}, translate("begin-workspace", err)
	}
	return Workspace{Dir: w.Dir}, nil
}

// AbandonWorkspace removes a workspace without committing it.
func (g *Gateway) AbandonWorkspace(w Workspace) error {
	return translate("abandon-workspace", g.vault.Discard(w))
}

// SetKey derives the opaque key for a committed set. The derivation rule is
// owned by the persistence boundary so callers never learn the on-disk
// layout.
func SetKey(identity string, revision int) (string, error) {
	if len(identity) != 32 {
		return "", errors.New("invalid content identity")
	}
	if revision < 1 || revision > 999 {
		return "", errors.New("invalid set revision")
	}
	return fmt.Sprintf("%s-%d", identity, revision), nil
}

// CommitWorkspace durably promotes a workspace to the given opaque key.
// Failure leaves the workspace in staging as a conservative orphan and
// never presents the set as committed.
func (g *Gateway) CommitWorkspace(w Workspace, key string) error {
	return translate("commit-workspace", g.vault.CommitWorkspace(w, key))
}

// SetLocation resolves a committed set key to a local directory for
// read-only delivery.
func (g *Gateway) SetLocation(key string) (string, error) {
	dir, err := g.vault.SetPath(key)
	if err != nil {
		return "", translate("set-location", err)
	}
	return dir, nil
}

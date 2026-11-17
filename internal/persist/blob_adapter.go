package persist

import (
	"cadenceforge/internal/blob"
	"io"
)

// blobVault adapts the content-address vault to the persistence port. It is
// the only place that knows blob's concrete workspace and path types.
type blobVault struct{ store *blob.Store }

// NewVault wraps a blob store as a persistence Vault.
func NewVault(store *blob.Store) Vault { return &blobVault{store: store} }

func (v *blobVault) Receive(r io.Reader, limit int64) (string, int64, error) {
	return v.store.Receive(r, limit)
}

func (v *blobVault) ObjectPath(digest string) (string, error) {
	return v.store.Object(digest)
}

func (v *blobVault) NewWorkspace() (Workspace, error) {
	w, err := v.store.NewWorkspace()
	if err != nil {
		return Workspace{}, err
	}
	return Workspace{Dir: w.Path}, nil
}

func (v *blobVault) Discard(w Workspace) error {
	return v.store.Discard(blob.Workspace{Path: w.Dir})
}

func (v *blobVault) CommitWorkspace(w Workspace, key string) error {
	return v.store.CommitWorkspace(blob.Workspace{Path: w.Dir}, key)
}

func (v *blobVault) SetPath(key string) (string, error) {
	return v.store.SetLocation(key)
}

func blobWorkspace(w Workspace) blob.Workspace { return blob.Workspace{Path: w.Dir} }

package persist

import (
	"cadenceforge/internal/blob"
	"cadenceforge/internal/journal"
)

// OpenRoot is the composition entry point: it opens the content vault and
// durable-state adapters under one data root and returns the application
// gateway. Callers depend only on the gateway, never on either adapter. The
// reconciliation report describes any unconfirmed commit found at startup.
func OpenRoot(root string) (*Gateway, journal.ReconcileReport, error) {
	vault, err := blob.Open(root)
	if err != nil {
		return nil, journal.ReconcileReport{}, err
	}
	return Open(root, NewVault(vault))
}

package service

import (
	"cadenceforge/internal/blob"
	"cadenceforge/internal/config"
	"cadenceforge/internal/domain"
	"cadenceforge/internal/journal"
	"cadenceforge/internal/persist"
	"context"
	"errors"
	"testing"
	"time"
)

// minimalState builds a state that passes domain validation: one source, a
// one-revision sheet and recipe, so a render can be submitted against them.
func minimalState() domain.State {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	st := domain.Empty()
	st.Sources["11111111111111111111111111111111"] = domain.Source{
		ID: "11111111111111111111111111111111", Name: "src",
		Digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Bytes:  100, Media: domain.Media{DurationMS: 2000, Video: true, Audio: true, SampleRate: 48000},
		CreatedAt: now,
	}
	sheet := domain.Sheet{
		ID: "22222222222222222222222222222222", Revision: 1, Name: "sheet", CreatedAt: now,
		Spans: []domain.Span{{SourceID: "11111111111111111111111111111111", InMS: 0, OutMS: 1000, GainDB: 0}},
	}
	recipe := domain.Recipe{
		ID: "33333333333333333333333333333333", Revision: 1, Name: "recipe", Format: "mp4",
		Width: 160, Height: 90, FPS: 24, SampleRate: 48000,
	}
	st.Sheets[sheet.ID] = []domain.Sheet{sheet}
	st.Recipes[recipe.ID] = []domain.Recipe{recipe}
	return st
}

func newServiceOn(t *testing.T, root string) *Service {
	t.Helper()
	vault, err := blob.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	gateway, _, err := persist.Open(root, persist.NewVault(vault))
	if err != nil {
		t.Fatal(err)
	}
	return &Service{
		Persistence: gateway,
		Config:      config.Config{QueueLimit: 16},
		wake:        make(chan struct{}, 1),
		active:      map[string]context.CancelFunc{},
	}
}

// injectUncertainCommit makes only the state-directory sync fail while the
// fault-copy sync succeeds, reproducing "replaced but durability unconfirmed".
func injectUncertainCommit(t *testing.T) {
	t.Helper()
	calls := 0
	journal.SetSyncFaultHook(func(path string) error {
		calls++
		if calls == 2 {
			return errors.New("simulated directory sync failure")
		}
		return nil
	})
	t.Cleanup(func() { journal.SetSyncFaultHook(nil) })
}

func baseRequest() Request {
	return Request{
		SheetID: "22222222222222222222222222222222", SheetRevision: 1,
		RecipeID: "33333333333333333333333333333333", RecipeRevision: 1,
	}
}

// TestIdempotentSubmitSurvivesUncertainCommitAndRestart proves the headline
// persistence guarantee across a real service restart: a replaced-but-
// unconfirmed commit is reconciled from the private fault copy, and the
// idempotency record still converges instead of duplicating the task.
func TestIdempotentSubmitSurvivesUncertainCommitAndRestart(t *testing.T) {
	root := t.TempDir()

	// Phase A: establish the baseline state durably.
	first := newServiceOn(t, root)
	if _, err := first.Persistence.Commit(func(st *domain.State) error {
		*st = minimalState()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	req := baseRequest()

	// Phase B: inject durability uncertainty and submit. The typed fault must
	// surface rather than a clean acceptance, so the client cannot assume a
	// blind resend is safe.
	injectUncertainCommit(t)
	render, err := first.Submit("idem-key-1", req)
	if !persist.IsUncertain(err) {
		t.Fatalf("expected uncertain commit fault, got %v", err)
	}
	if render.ID == "" {
		t.Fatal("no render identity produced with the candidate commit")
	}
	first.Close()

	// Phase C: restart. Reconciliation confirms the replacement landed and
	// keeps the single idempotency record.
	second := newServiceOn(t, root)
	defer second.Close()
	if report := second.Persistence.ReconcileReport(); !report.Recovered {
		t.Fatalf("expected recovered reconciliation, got %+v", report)
	}

	// Phase D: replaying the same key+input converges to the same task.
	replay, err := second.Submit("idem-key-1", req)
	if err != nil {
		t.Fatalf("idempotent replay after restart failed: %v", err)
	}
	if replay.ID != render.ID {
		t.Fatalf("idempotency broken: original=%s replay=%s", render.ID, replay.ID)
	}
	snap, err := second.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Renders) != 1 {
		t.Fatalf("uncertain commit created duplicate renders: %d", len(snap.Renders))
	}

	// Phase E: the recovered record retains the original request fingerprint,
	// so the same key with genuinely different input still conflicts.
	conflicting := req
	conflicting.SheetRevision = 1
	conflicting.RecipeRevision = 2 // points at a non-existent second revision
	_, err = second.Submit("idem-key-1", conflicting)
	var fault *domain.Fault
	if !errors.As(err, &fault) {
		t.Fatalf("expected a domain fault for changed input, got %v", err)
	}
}

// TestRetireKeepsFrozenMediaHistory proves the lineage rule is unchanged by
// the refactor: a source referenced by frozen render history cannot retire,
// which is what guarantees the frozen task still has its exact bytes.
func TestRetireKeepsFrozenMediaHistory(t *testing.T) {
	root := t.TempDir()
	svc := newServiceOn(t, root)
	defer svc.Close()
	if _, err := svc.Persistence.Commit(func(st *domain.State) error {
		*st = minimalState()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	render, err := svc.Submit("freeze-key", baseRequest())
	if err != nil {
		t.Fatal(err)
	}
	if render.ID == "" {
		t.Fatal("render not created")
	}
	err = svc.RetireSource("11111111111111111111111111111111")
	var fault *domain.Fault
	if !errors.As(err, &fault) || fault.Status != 409 {
		t.Fatalf("expected 409 lineage conflict, got %v", err)
	}
	snap, _ := svc.Snapshot()
	if _, ok := snap.Sources["11111111111111111111111111111111"]; !ok {
		t.Fatal("frozen source must remain registered to keep its bytes resolvable")
	}
}

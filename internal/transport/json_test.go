package transport

import (
	"cadenceforge/internal/domain"
	"cadenceforge/internal/persist"
	"errors"
	"net/http/httptest"
	"testing"
)

func TestFailureDistinguishesUncertainCommit(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{
			name:       "uncertain commit",
			err:        persist.NewFaultForTest(persist.UncertainCommit, "commit", errors.New("sync")),
			wantStatus: 500,
			wantCode:   "commit_unconfirmed",
		},
		{
			name:       "store unavailable",
			err:        persist.NewFaultForTest(persist.StoreUnavailable, "commit", errors.New("poison")),
			wantStatus: 500,
			wantCode:   "persistence_unavailable",
		},
		{
			name:       "ordinary persistence failure",
			err:        persist.NewFaultForTest(persist.OperationFailed, "receive", errors.New("io")),
			wantStatus: 500,
			wantCode:   "persistence_failed",
		},
		{
			name:       "domain conflict passes through",
			err:        domain.Conflict("revision changed"),
			wantStatus: 409,
			wantCode:   "conflict",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			failure(rec, tc.err)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status=%d want %d", rec.Code, tc.wantStatus)
			}
			body := rec.Body.String()
			if !contains(body, tc.wantCode) {
				t.Fatalf("body %q missing code %q", body, tc.wantCode)
			}
		})
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

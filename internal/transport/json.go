package transport

import (
	"cadenceforge/internal/domain"
	"cadenceforge/internal/persist"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
)

func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Warn("response write", "error", err)
	}
}

// failure maps errors to stable HTTP responses. Persistence faults keep the
// generic 500 status promised by the service contract but carry a distinct
// code so a client never mistakes an unconfirmed commit for a clean
// rejection that would justify blind resend.
func failure(w http.ResponseWriter, err error) {
	if f, ok := persist.AsFault(err); ok {
		code := "persistence_failed"
		switch {
		case persist.IsUncertain(f):
			code = "commit_unconfirmed"
		case errors.Is(f, persist.StoreUnavailable):
			code = "persistence_unavailable"
		}
		slog.Error("request failed", "error", err, "fault", code)
		write(w, 500, map[string]string{"error": code, "message": f.Error()})
		return
	}
	status, code, message := domain.Classify(err)
	if status == 500 {
		slog.Error("request failed", "error", err)
	}
	write(w, status, map[string]string{"error": code, "message": message})
}
func decode(w http.ResponseWriter, r *http.Request, dest any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(dest); err != nil {
		return domain.Invalid("invalid JSON: %s", err)
	}
	if err := d.Decode(&struct{}{}); err != io.EOF {
		return domain.Invalid("request requires exactly one JSON object")
	}
	return nil
}

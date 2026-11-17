package journal

import (
	"cadenceforge/internal/domain"
	"time"
)

func (s *Store) Recover() error {
	_, err := s.Update(func(st *domain.State) error {
		for id, r := range st.Renders {
			if r.State != "running" {
				continue
			}
			r.State = "failed"
			r.UpdatedAt = time.Now().UTC()
			if len(r.Attempts) > 0 {
				a := &r.Attempts[len(r.Attempts)-1]
				a.FinishedAt = &r.UpdatedAt
				a.Outcome = "interrupted"
				a.Detail = "process ended before artifact commit"
			}
			st.Renders[id] = r
			st.Emit(id, "render.interrupted", "explicit retry required; frozen input retained")
		}
		return nil
	})
	return err
}

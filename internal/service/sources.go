package service

import (
	"cadenceforge/internal/domain"
	"cadenceforge/internal/inspect"
	"cadenceforge/internal/media"
	"context"
	"fmt"
	"io"
	"strings"
	"time"
)

func (s *Service) Ingest(ctx context.Context, name string, body io.Reader) (domain.Source, error) {
	if strings.TrimSpace(name) == "" || len(name) > 160 {
		return domain.Source{}, domain.Invalid("source name must contain 1..160 bytes")
	}
	digest, n, err := s.Persistence.Receive(body, s.Config.UploadLimit)
	if err != nil {
		return domain.Source{}, domain.Invalid("upload rejected: %s", err)
	}
	path, err := s.Persistence.Lease(digest)
	if err != nil {
		return domain.Source{}, err
	}
	metadata, err := s.Probe.Probe(ctx, path)
	if err != nil {
		return domain.Source{}, domain.Invalid("unusable media: %s", err)
	}
	s.mutation.Lock()
	defer s.mutation.Unlock()
	source := domain.Source{ID: ID(), Name: name, Digest: digest, Bytes: n, Media: metadata, CreatedAt: time.Now().UTC()}
	_, err = s.Persistence.Commit(func(st *domain.State) error {
		for _, existing := range st.Sources {
			if existing.Digest == digest {
				source = existing
				return nil
			}
		}
		st.Sources[source.ID] = source
		st.Emit(source.ID, "source.accepted", name)
		return nil
	})
	return source, err
}
func (s *Service) RetireSource(id string) error {
	s.mutation.Lock()
	defer s.mutation.Unlock()
	_, err := s.Persistence.Commit(func(st *domain.State) error {
		if _, ok := st.Sources[id]; !ok {
			return domain.Missing("source", id)
		}
		for _, versions := range st.Sheets {
			for _, sheet := range versions {
				for _, p := range sheet.Spans {
					if p.SourceID == id {
						return domain.Conflict("source is referenced by sheet history")
					}
				}
				for _, cue := range sheet.Cues {
					if cue.SourceID == id {
						return domain.Conflict("source is referenced by cue history")
					}
				}
			}
		}
		for _, r := range st.Renders {
			if _, ok := r.Sources[id]; ok {
				return domain.Conflict("source is retained by render lineage")
			}
		}
		delete(st.Sources, id)
		st.Emit(id, "source.retired", "content retained for conservative reclamation")
		return nil
	})
	return err
}
func (s *Service) Loudness(ctx context.Context, id string) (media.Loudness, error) {
	st, err := s.Persistence.Snapshot()
	if err != nil {
		return media.Loudness{}, err
	}
	source, ok := st.Sources[id]
	if !ok {
		return media.Loudness{}, domain.Missing("source", id)
	}
	if !source.Media.Audio {
		return media.Loudness{}, domain.Invalid("source has no audio")
	}
	path, err := s.Persistence.Lease(source.Digest)
	if err != nil {
		return media.Loudness{}, err
	}
	actual, n, err := inspect.Digest(path)
	if err != nil {
		return media.Loudness{}, err
	}
	if actual != source.Digest || n != source.Bytes {
		return media.Loudness{}, fmt.Errorf("source digest mismatch")
	}
	return media.Measure(ctx, s.Config.FFmpeg, path)
}

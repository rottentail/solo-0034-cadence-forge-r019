package service

import (
	"cadenceforge/internal/domain"
	"cadenceforge/internal/inspect"
	"cadenceforge/internal/media"
	"context"
	"fmt"
)

func (s *Service) Visual(ctx context.Context, id, mode string) ([]byte, error) {
	state, err := s.Persistence.Snapshot()
	if err != nil {
		return nil, err
	}
	source, ok := state.Sources[id]
	if !ok {
		return nil, domain.Missing("source", id)
	}
	path, err := s.Persistence.Lease(source.Digest)
	if err != nil {
		return nil, err
	}
	digest, n, err := inspect.Digest(path)
	if err != nil {
		return nil, err
	}
	if digest != source.Digest || n != source.Bytes {
		return nil, fmt.Errorf("source content integrity failed")
	}
	return media.Visual(ctx, s.Config.FFmpeg, path, mode, source.Media)
}

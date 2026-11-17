package encoding

import (
	"cadenceforge/internal/domain"
	"cadenceforge/internal/media"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SourceLocator resolves a frozen source digest to a readable local path.
// The media process depends on this narrow port instead of a concrete
// storage implementation.
type SourceLocator interface {
	Path(digest string) (string, error)
}

type Runner struct {
	Binary  string
	Sources SourceLocator
}

func (r Runner) Encode(ctx context.Context, render domain.Render, workspace string) (string, error) {
	paths := map[string]string{}
	for id, s := range render.Sources {
		path, err := r.Sources.Path(s.Digest)
		if err != nil {
			return "", err
		}
		if _, err = os.Stat(path); err != nil {
			return "", fmt.Errorf("source %s unavailable: %w", id, err)
		}
		paths[id] = path
	}
	name := "composition." + render.Recipe.Format
	args, err := Arguments(render, paths, filepath.Join(workspace, name))
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if _, err = media.Run(ctx, r.Binary, args, 65536); err != nil {
		return "", err
	}
	if err = os.WriteFile(filepath.Join(workspace, "captions.vtt"), []byte(render.Plan.VTT), 0600); err != nil {
		return "", err
	}
	return name, nil
}

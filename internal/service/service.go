package service

import (
	"cadenceforge/internal/config"
	"cadenceforge/internal/domain"
	"cadenceforge/internal/media"
	"cadenceforge/internal/persist"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
)

type Service struct {
	Persistence *persist.Gateway
	Probe       media.Prober
	Config      config.Config
	mutation    sync.Mutex
	wake        chan struct{}
	activeMu    sync.Mutex
	active      map[string]context.CancelFunc
}

func New(c config.Config) (*Service, error) {
	gateway, report, err := persist.OpenRoot(c.Root)
	if err != nil {
		return nil, err
	}
	s := &Service{Persistence: gateway, Probe: media.Prober{Binary: c.FFprobe}, Config: c, wake: make(chan struct{}, 1), active: map[string]context.CancelFunc{}}
	if report.Recovered {
		slog.Warn("restart reconciled an unconfirmed state commit; fault copy matched committed bytes", "kind", "recovered")
	}
	if report.RolledBack {
		slog.Warn("restart discarded an unconfirmed state commit; prior committed history retained", "kind", "rolled-back")
	}
	if err = gateway.Recover(); err != nil {
		gateway.Close()
		return nil, err
	}
	return s, nil
}

// Snapshot is the read path offered to transport code; HTTP never touches
// the persistence adapters directly.
func (s *Service) Snapshot() (domain.State, error) {
	return s.Persistence.Snapshot()
}

// Close releases persistence resources.
func (s *Service) Close() error { return s.Persistence.Close() }

func ID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Errorf("entropy unavailable: %w", err))
	}
	return hex.EncodeToString(b[:])
}
func hash(value any) string {
	b, _ := json.Marshal(value)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func (s *Service) Notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

package main

import (
	"cadenceforge/internal/config"
	"cadenceforge/internal/service"
	"cadenceforge/internal/transport"
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

func run() error {
	c, err := config.Load()
	if err != nil {
		return err
	}
	for _, binary := range []string{c.FFmpeg, c.FFprobe} {
		if _, err = exec.LookPath(binary); err != nil {
			return err
		}
	}
	s, err := service.New(c)
	if err != nil {
		return err
	}
	defer s.Close()
	listener, err := net.Listen("tcp", c.Address)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workers := s.Start(ctx)
	server := &http.Server{Handler: transport.Handler(s), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 60 * time.Second, WriteTimeout: 180 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 16}
	errors := make(chan error, 1)
	go func() { errors <- server.Serve(listener) }()
	slog.Info("cadence forge ready", "address", listener.Addr().String())
	select {
	case <-ctx.Done():
	case err = <-errors:
		stop()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e := server.Shutdown(shutdown); e != nil {
		slog.Error("HTTP shutdown", "error", e)
		server.Close()
	}
	workers.Wait()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

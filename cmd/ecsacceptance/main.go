// ecsacceptance is a loopback-only synthetic ECS log receiver, not Monitor's UI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func main() {
	if err := run(); err != nil {
		slog.Error("isolated acceptance receiver stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "private JSON configuration (0600); no environment settings loaded")
	stateDir := flag.String("state-dir", "", "dedicated private state directory; created on first start")
	port := flag.Int("port", 61696, "loopback TCP port")
	flag.Parse()
	if *port < 1024 || *port > 65535 || flag.NArg() != 0 {
		return errors.New("explicit unprivileged port and named arguments required")
	}
	cfg, digest, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	lock, err := prepareState(*stateDir, digest)
	if err != nil {
		return err
	}
	defer lock.Close()
	m, handler, err := monitor.NewECSAcceptance(cfg, *stateDir)
	if err != nil {
		return err
	}
	defer m.Close()
	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		return err
	}
	defer listener.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	m.Start(ctx)
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 20 * time.Second, WriteTimeout: 20 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	slog.Info("isolated receiver listening; no UI or business database", "address", listener.Addr(), "state", *stateDir)
	select {
	case err := <-done:
		stop()
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		err := <-done
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

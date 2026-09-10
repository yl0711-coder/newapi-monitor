package ecslogagent

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func (a *Agent) maintain(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		for _, lane := range a.cfg.lanes() {
			if ctx.Err() != nil {
				return
			}
			if a.lease(lane) < a.now().Unix()+300 {
				call, cancel := context.WithTimeout(ctx, 8*time.Second)
				err := a.Renew(call, lane)
				cancel()
				if err != nil {
					slog.Warn("ECS registration pending; collector retains batches", "lane", lane)
					continue
				}
			}
			call, cancel := context.WithTimeout(ctx, 8*time.Second)
			err := a.Heartbeat(call, lane)
			cancel()
			if err != nil {
				slog.Warn("ECS agent heartbeat unavailable", "lane", lane)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// RunChild owns the child process and private proxy together. SIGTERM stops the
// child first while the proxy remains available for its in-flight final save.
// This is not an external archive or a guarantee against Fargate SIGKILL.
func (a *Agent) RunChild(parent context.Context, argv []string) error {
	if a.cfg.DeferredArchiveACK && a.archive == nil {
		return errors.New("deferred delivery requires configured external archive")
	}
	if a.cfg.FinalLogRoot != "" && a.producerStopped == nil {
		return errors.New("final boundary stop verifier required")
	}
	if err := parent.Err(); err != nil {
		return err
	}
	if len(argv) == 0 || argv[0] == "" {
		return errors.New("collector executable required")
	}
	listener, err := a.Listen()
	if err != nil {
		return err
	}
	server := &http.Server{Handler: a, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	ctx, cancel := context.WithCancel(context.Background())
	maintenanceDone := make(chan struct{})
	defer func() {
		cancel()
		shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = server.Shutdown(shutdown)
		_ = server.Close()
		<-maintenanceDone
	}()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = a.ChildEnvironment(os.Environ())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		close(maintenanceDone)
		return errors.New("collector process failed to start")
	}
	go func() { defer close(maintenanceDone); a.maintain(ctx) }()
	childDone := make(chan error, 1)
	go func() { childDone <- cmd.Wait() }()
	select {
	case err := <-childDone:
		return err
	case err := <-serverDone:
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = stopChild(cmd, childDone)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("private agent listener failed")
	case <-parent.Done():
		var before map[string]ecsarchive.FinalFile
		var boundaryErr error
		if a.cfg.FinalLogRoot != "" {
			check, finish := context.WithTimeout(context.Background(), 4*time.Second)
			boundaryErr = a.producerStopped(check)
			if boundaryErr == nil {
				before, boundaryErr = a.finalInventory(check)
			}
			finish()
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		if err := stopChild(cmd, childDone); err != nil {
			return err
		}
		var boundaries map[string]*ecsarchive.FinalBoundary
		if a.cfg.FinalLogRoot != "" && boundaryErr == nil {
			check, finish := context.WithTimeout(context.Background(), 4*time.Second)
			boundaries, boundaryErr = a.finishBoundaries(check, before)
			finish()
		}
		closure, finish := context.WithTimeout(context.Background(), archiveClosureBudget)
		defer finish()
		return errors.Join(boundaryErr, a.closeArchiveWithBoundaries(closure, boundaries))
	}
}

func stopChild(cmd *exec.Cmd, done <-chan error) error {
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		_ = cmd.Process.Kill()
		<-done
		return errors.New("collector shutdown deadline exceeded; final drain unverified")
	}
}

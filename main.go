// Command monitor 是一个独立的上游稳定性监控服务。
//
// 它完全自包含、零外部依赖:自带配置(环境变量)、自带本地采样库、
// 自带页面。后台采样器每 N 秒对 new-api 生产库做一条小窗口查询写本地;页面只读本地。
//
// 运行:
//
//	NEWAPI_LOG_DSN='ro_user:pass@tcp(host:3306)/newapi?charset=utf8mb4&timeout=5s&readTimeout=10s' \
//	  go run .
//
// 然后浏览器打开 http://localhost:8090
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	_ = godotenv.Load() // 可选 .env

	s := monitor.LoadSettings()
	m, err := monitor.New(s)
	if err != nil {
		slog.Error("启动失败", "err", err)
		os.Exit(1)
	}

	// 收到 SIGINT/SIGTERM 时取消 ctx:采样器退出 + HTTP 优雅关停。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	m.Start(ctx)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	m.RegisterRoutes(r)
	m.RegisterPublicBoard(r) // 对外公开看板:/status + /public/status(无鉴权、脱敏)

	srv := &http.Server{Addr: s.Addr, Handler: r}
	go func() {
		slog.Info("上游监控已启动", "addr", "http://localhost"+s.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("监听失败", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done() // 等待退出信号
	stop()
	slog.Info("收到退出信号,优雅关停…")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("关停超时", "err", err)
	}
}

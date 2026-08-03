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
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	_ = godotenv.Load() // 可选 .env
	if err := run(); err != nil {
		slog.Error("monitor 已退出", "err", err)
		os.Exit(1)
	}
}

// run 把所有需要优雅释放的资源放在同一调用栈中；main 仅在 run 返回后才
// os.Exit，确保监听失败、配置失败都不会跳过 stop/Shutdown 等 defer。
func run() error {
	s := monitor.LoadSettings()
	m, err := monitor.New(s)
	if err != nil {
		return fmt.Errorf("启动失败: %w", err)
	}
	defer m.Close()

	// 收到 SIGINT/SIGTERM 时取消 ctx:采样器退出 + HTTP 优雅关停。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	m.Start(ctx)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	if err := r.SetTrustedProxies(s.TrustedProxies); err != nil {
		return fmt.Errorf("MONITOR_TRUSTED_PROXIES 配置无效: %w", err)
	}
	m.RegisterRoutes(r)
	m.RegisterPublicBoard(r) // 对外公开看板:/status + /public/status(无鉴权、脱敏)

	listenErr := make(chan error, 2) // 管理端 + 可选 Portal；避免监听协程直接 os.Exit 跳过优雅关停。
	srv := monitoredHTTPServer(s.Addr, r)
	go func() {
		slog.Info("上游监控已启动", "addr", listenURL(s.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErr <- fmt.Errorf("管理端监听失败: %w", err)
		}
	}()

	// 客户端「用量报表」:独立引擎+独立端口(配置了才起),上面零管理端路由——客户域名只指它
	var portalSrv *http.Server
	if s.PortalAddr != "" {
		pr := gin.New()
		pr.Use(gin.Logger(), gin.Recovery())
		if err := pr.SetTrustedProxies(s.TrustedProxies); err != nil {
			return fmt.Errorf("MONITOR_TRUSTED_PROXIES 配置无效: %w", err)
		}
		m.RegisterPortalRoutes(pr)
		portalSrv = monitoredHTTPServer(s.PortalAddr, pr)
		go func() {
			slog.Info("客户用量报表已启动", "addr", listenURL(s.PortalAddr))
			if err := portalSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				listenErr <- fmt.Errorf("客户端监听失败: %w", err)
			}
		}()
	}

	var runErr error
	select {
	case <-ctx.Done(): // 正常退出信号
	case err := <-listenErr:
		slog.Error("监听失败，开始优雅关停", "err", err)
		runErr = err
	}
	stop()
	slog.Info("收到退出信号,优雅关停…")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("关停超时", "err", err)
	}
	if portalSrv != nil {
		if err := portalSrv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("客户端关停超时", "err", err)
		}
	}
	return runErr
}

// monitoredHTTPServer 为管理端和客户门户统一设置连接边界。ReadHeaderTimeout
// 防慢请求长期占住连接，ReadTimeout 限制请求体读取；WriteTimeout 留足 CSV
// 流式下载时间，IdleTimeout 及时回收空闲 keep-alive。业务 JSON 体积限制见
// requestBodyLimit 中间件。
func monitoredHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}
}

// listenURL 把监听地址拼成能直接点开的 URL。
// 监听地址有两种写法:":8090"(所有网卡)和 "127.0.0.1:8090"(指定网卡)。
// 直接拼 "http://localhost"+addr 时,后者会得到 "http://localhost127.0.0.1:8090" 这种废字符串,
// 所以只在 addr 以 ":" 开头(即没写主机)时才补 localhost。
func listenURL(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "http://localhost" + addr
	}
	return "http://" + addr
}

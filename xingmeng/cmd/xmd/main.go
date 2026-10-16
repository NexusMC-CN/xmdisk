// xmd 是星梦存储服务端入口：配置 → 节点 → 四监听面（docs/04/05）。
// 路由组织习惯：main 挂路由组，路由组挂鉴权+业务；鉴权按面独立后补。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"xingmeng/internal/api"
	"xingmeng/internal/config"
	"xingmeng/internal/node"
)

func main() {
	cfgPath := flag.String("config", "xmd.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fatal("load config: %v", err)
	}
	level := slog.LevelInfo
	switch cfg.Log.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	// KEK 只从环境变量读取（99-D009；开发期 .env 由启动脚本/source 注入）
	var kek []byte
	if v := os.Getenv("XM_KEK"); v != "" {
		kek = []byte(v)
	} else {
		slog.Warn("XM_KEK not set: credential API disabled (S3 auth will reject all)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	n, err := node.Start(ctx, cfg, kek)
	if err != nil {
		fatal("start node: %v", err)
	}

	servers := []*http.Server{
		listen(cfg.Listeners.DataHTTP, n.S3Handler(), "data-http"),
		listen(cfg.Listeners.Admin, api.AdminRouter(n, cfg.AdminToken), "admin"),
		listen(cfg.Listeners.Repl, n.ReplHandler(), "repl"),
	}
	if cfg.Listeners.DataSSH != "" {
		slog.Warn("SFTP adapter not implemented yet, data_ssh listener skipped", "addr", cfg.Listeners.DataSSH)
	}

	go func() {
		<-ctx.Done()
		slog.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, s := range servers {
			s.Shutdown(shutdownCtx)
		}
	}()

	for _, s := range servers {
		srv := s
		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("listener failed", "err", err)
				stop()
			}
		}()
	}
	<-ctx.Done()
	time.Sleep(11 * time.Second) // 等优雅退出完成
	n.Close()
}

func listen(addr string, h http.Handler, name string) *http.Server {
	return &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 10 * time.Second}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "xmd: "+format+"\n", args...)
	os.Exit(1)
}

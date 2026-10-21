// xmd 是星梦存储服务端入口：配置（YAML 可选 + env 初值，99-D023）→ 节点
// → 监听面（docs/04/05）。路由组织习惯：main 挂路由组，路由组挂鉴权+业务。
// XM_KEK 与 admin token 必填，缺失拒绝启动（容器内监听 0.0.0.0 倒逼）。
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
	"strings"
	"syscall"
	"time"

	"xingmeng/internal/api"
	"xingmeng/internal/config"
	"xingmeng/internal/node"
)

const defaultConfigPath = "xmd.yaml"

func main() {
	cfgPath := flag.String("config", defaultConfigPath, "配置文件路径（缺省 xmd.yaml 不存在则全默认 + env）")
	flag.Parse()

	var cfg *config.Config
	if _, err := os.Stat(*cfgPath); err == nil {
		cfg, err = config.Load(*cfgPath)
		if err != nil {
			fatal("load config: %v", err)
		}
	} else if errors.Is(err, os.ErrNotExist) && *cfgPath == defaultConfigPath {
		cfg = config.Defaults() // 无配置文件：默认值 + env（docker 形态，99-D023）
		slog.Info("config file not found; using defaults + XM_* env")
	} else {
		fatal("config %s: %v", *cfgPath, err)
	}
	if applied, err := config.ApplyEnv(cfg); err != nil {
		fatal("env overlay: %v", err)
	} else if len(applied) > 0 {
		slog.Info("env overrides applied", "keys", strings.Join(applied, ","))
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

	// 必填拒启（99-D023②）：容器内监听 0.0.0.0，空凭证不可接受。
	// 强度硬下限（S-04，99-D027）：admin/KEK ≥16、repl ≥8，env/YAML/运行时三路一致。
	kek := os.Getenv("XM_KEK")
	if kek == "" {
		fatal("XM_KEK is required (KEK of credential secrets; keep it identical across the cluster)")
	}
	if len(kek) < 16 {
		fatal("XM_KEK must be at least 16 characters (use a long high-entropy random string)")
	}
	if cfg.AdminToken == "" {
		fatal("admin token is required: set XM_ADMIN_TOKEN (or admin_token in config)")
	}
	if len(cfg.AdminToken) < 16 {
		fatal("admin token must be at least 16 characters (use a long high-entropy random string)")
	}
	if cfg.Replication.Token != "" && len(cfg.Replication.Token) < 8 {
		fatal("repl token must be at least 8 characters when set (empty = repl face disabled)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	n, err := node.Start(ctx, cfg, []byte(kek))
	if err != nil {
		fatal("start node: %v", err)
	}

	// 数据面 HTTP 与 admin 面；repl 复制面由节点运行时按 token 有无管理（99-D023③），
	// 公共读面独立只读端口（99-D024），SSH/SFTP 为原生 TCP 监听。
	servers := []*http.Server{
		listen(cfg.Listeners.DataHTTP, n.DataHandler(), "data-http"),
		listen(cfg.Listeners.Admin, api.AdminRouter(n, api.RouterConfig{
			AdminToken: cfg.AdminToken, WebUI: cfg.WebUIEnabled(),
		}), "admin"),
		listen(cfg.Listeners.Public, n.PublicHandler(), "public-read"),
	}
	if cfg.Listeners.DataSSH != "" {
		sshLn, err := net.Listen("tcp", cfg.Listeners.DataSSH)
		if err != nil {
			fatal("listen data_ssh %s: %v", cfg.Listeners.DataSSH, err)
		}
		go func() {
			for {
				conn, err := sshLn.Accept()
				if err != nil {
					return
				}
				n.SFTPConn(conn)
			}
		}()
		slog.Info("listening", "face", "data-ssh", "addr", cfg.Listeners.DataSSH)
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

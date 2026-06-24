package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// 全局常量集中管理, 消除魔法数字散落 (#16)
const (
	// 配置默认值 (main() 环境变量为空或非法时回退)
	defaultTrunk      int64 = 10 * 1024 * 1024 // 10M
	defaultSplit      int64 = 1024 * 1024      // 1M
	defaultFirstChunk int64 = 2 * 1024 * 1024  // 2M
	defaultConns      int   = 60

	// 缓存与统计
	cacheTTL          = 5 * time.Minute
	statsDir          = "/data"
	statsFile         = "/data/stats.json"
	cleanupInterval   = 30 * time.Second
	statsSaveInterval = 30 * time.Second
	logMaxEntries     = 50
	dailyHistoryCap   = 30

	// HTTP 超时与重试
	probeClientTimeout   = 30 * time.Second
	resolveClientTimeout = 15 * time.Second
	readHeaderTimeout    = 10 * time.Second
	idleTimeout          = 120 * time.Second
	maxRedirects         = 10

	// 优雅关停超时
	shutdownTimeout = 30 * time.Second
)

var version = "2.0.0"

var dashboardToken string

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	trunk := os.Getenv("TRUNK")
	if trunk == "" {
		trunk = "10M"
	}
	split := os.Getenv("SPLIT")
	if split == "" {
		split = "1M"
	}
	firstChunk := os.Getenv("FIRST_CHUNK")
	if firstChunk == "" {
		firstChunk = "2M"
	}
	connsStr := os.Getenv("CONNS")
	conns, _ := strconv.Atoi(connsStr) // normalizeConns 会兜底, 这里忽略 err 安全
	host := os.Getenv("HOST")
	if host == "" {
		host = "0.0.0.0"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8010"
	}

	headers := make(map[string]string)
	if h := strings.TrimSpace(os.Getenv("HEADERS")); h != "" && h != "{}" {
		if err := json.Unmarshal([]byte(h), &headers); err != nil {
			log.Printf("HEADERS 解析失败: %v", err)
		}
	}

	initAllowedHosts()
	dashboardToken = strings.TrimSpace(os.Getenv("DASHBOARD_TOKEN"))

	srv := newServer(trunk, split, firstChunk, conns, headers)

	os.MkdirAll(statsDir, 0755)
	stats.load(statsFile)
	go func() {
		ticker := time.NewTicker(statsSaveInterval)
		defer ticker.Stop()
		for range ticker.C {
			stats.save(statsFile)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", srv.handleHealth)
	mux.HandleFunc("/api/stats", srv.handleAPIStats)
	mux.HandleFunc("/stream", srv.handleStream)
	mux.HandleFunc("/", srv.handleRoot)

	addr := fmt.Sprintf("%s:%s", host, port)
	log.Printf("Thunder-MT v%s 启动，监听 %s", version, addr)
	log.Printf("配置: trunk=%s split=%s firstChunk=%s conns=%d", trunk, split, firstChunk, conns)
	if allowedHosts != nil {
		log.Printf("ALLOW_HOSTS 白名单: %d 个 host", len(allowedHosts))
	}
	if dashboardToken != "" {
		log.Printf("仪表盘鉴权: 已启用")
	}

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Printf("收到信号，优雅关停...")
		stats.save(statsFile)
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		httpSrv.Shutdown(ctx)
	}()

	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("启动失败: %v", err)
	}
	log.Printf("已停止")
}

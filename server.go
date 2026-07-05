package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// dashboardHTML 由 //go:embed 在编译期注入, 避免大段字符串常量嵌在 Go 代码里 (#15)
//
//go:embed dashboard.html
var dashboardHTML string

type server struct {
	trunk      int64
	split      int64
	firstChunk int64
	firstTrunk int64
	conns      int
	headers    map[string]string
	cache      *proxyCache
}

func isFatal(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return !strings.Contains(msg, "broken pipe") && !strings.Contains(msg, "connection reset")
}

func flushHeaders(wr *responseWriter, contentRange string, contentLength int64) {
	wr.Header().Set("Content-Range", contentRange)
	wr.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	wr.WriteHeader(http.StatusPartialContent)
	wr.headerSentAt = time.Now()
	if f, ok := wr.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func logIfFatal(err error, format string, args ...interface{}) {
	if isFatal(err) {
		log.Printf(format, args...)
	}
}

func (s *server) checkAuth(r *http.Request) bool {
	if dashboardToken == "" {
		return true
	}
	if r.URL.Query().Get("token") == dashboardToken {
		return true
	}
	if b := r.Header.Get("Authorization"); strings.HasPrefix(b, "Bearer ") {
		if strings.TrimPrefix(b, "Bearer ") == dashboardToken {
			return true
		}
	}
	return false
}

func newServer(trunk, split, firstChunk, firstTrunk string, conns int, headers map[string]string) *server {
	t := normalizeTrunk(parseSize(trunk))
	return &server{
		trunk:      t,
		split:      normalizeSplit(parseSize(split)),
		firstChunk: normalizeFirstChunk(parseSize(firstChunk)),
		firstTrunk: normalizeFirstTrunk(parseSize(firstTrunk), t),
		conns:      normalizeConns(conns),
		headers:    headers,
		cache:      newProxyCache(cacheTTL),
	}
}

type responseWriter struct {
	http.ResponseWriter
	wrote        int64
	firstByteAt  time.Time
	start        time.Time
	headerSentAt time.Time
}

func (w *responseWriter) Write(b []byte) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("write panic: %v", r)
		}
	}()
	if w.firstByteAt.IsZero() {
		w.firstByteAt = time.Now()
	}
	n, err = w.ResponseWriter.Write(b)
	w.wrote += int64(n)
	return
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

func (s *server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if !s.checkAuth(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML))
}

func (s *server) handleAPIStats(w http.ResponseWriter, r *http.Request) {
	if !s.checkAuth(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	// #17: 编码错误说明 stats 数据异常, 记录便于排查 (响应已部分发送, 无法回写错误码)
	if err := json.NewEncoder(w).Encode(stats.snapshot()); err != nil {
		log.Printf("stats API 响应编码失败: %v", err)
	}
}

func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	atomic.AddInt32(&stats.ActiveStreams, 1)
	defer atomic.AddInt32(&stats.ActiveStreams, -1)

	start := stats.recordStart()
	wr := &responseWriter{ResponseWriter: w, start: start}
	ua := r.Header.Get("User-Agent")
	rangeHeader := r.Header.Get("Range")

	backendURL := r.URL.Query().Get("url")
	if backendURL == "" {
		http.Error(wr, "Missing 'url' parameter", http.StatusBadRequest)
		return
	}
	if err := isURLAllowed(backendURL); err != nil {
		log.Printf("拒绝 URL: %v", err)
		http.Error(wr, "URL not allowed", http.StatusForbidden)
		return
	}

	if strings.Contains(ua, "Lavf") {
		log.Printf("Lavf 302 → %s", backendURL)
		http.Redirect(wr, r, backendURL, http.StatusFound)
		stats.recordEnd(start, wr, ua, rangeHeader, 0, true, nil)
		return
	}

	proxy, err := s.cache.getOrCreate(backendURL, func() (*urlProxy, error) {
		directURL, err := resolveDirectURL(backendURL, s.headers)
		if err != nil {
			return nil, err
		}
		return newURLProxy(directURL, s.trunk, s.firstTrunk, s.split, s.conns, s.headers)
	})
	if err != nil {
		log.Printf("解析直链失败: %v", err)
		http.Error(wr, "无法解析后端地址", http.StatusBadGateway)
		stats.recordEnd(start, wr, ua, rangeHeader, 0, false, err)
		return
	}

	size := proxy.length

	disposition := fmt.Sprintf(`inline; filename*=UTF-8''%s`, url.PathEscape(proxy.fileName))
	wr.Header().Set("Content-Type", proxy.contentType)
	wr.Header().Set("Content-Disposition", disposition)
	wr.Header().Set("Accept-Ranges", "bytes")

	// streamSortedAndRecord: 封装 sortedStream 分支的 flush+stream+log+record 样板
	streamSortedAndRecord := func(begin, end int64, logPrefix string) {
		length := end - begin + 1
		flushHeaders(wr, fmt.Sprintf("bytes %d-%d/%d", begin, end, size), length)
		streamErr := proxy.sortedStream(begin, end, wr)
		logIfFatal(streamErr, logPrefix+": %v", streamErr)
		stats.recordEnd(start, wr, ua, rangeHeader, wr.wrote, false, streamErr)
	}

	if rangeHeader == "" {
		firstEnd := s.firstChunk - 1
		if firstEnd >= size {
			firstEnd = size - 1
		}
		log.Printf("无 Range: 首 chunk 0→%d (firstChunk=%d), size=%d", firstEnd, s.firstChunk, size)
		streamSortedAndRecord(0, firstEnd, "连续流错误")
		return
	}

	m := rangeRe.FindStringSubmatch(rangeHeader)
	if m == nil {
		http.Error(wr, "Invalid Range header", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	begin, _ := strconv.ParseInt(m[1], 10, 64)
	endStr := m[2]

	if begin >= size {
		wr.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		http.Error(wr, "Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	if endStr != "" {
		end, _ := strconv.ParseInt(endStr, 10, 64)
		if end > begin+proxy.trunk {
			end = begin + proxy.trunk
		}
		if end >= size {
			end = size - 1
		}
		log.Printf("Range(B): %s → begin=%d end=%d length=%d", rangeHeader, begin, end, end-begin+1)
		streamSortedAndRecord(begin, end, "sortedStream 错误")
	} else {
		log.Printf("Range(U): %s 连续流 %d→%d", rangeHeader, begin, size)
		flushHeaders(wr, fmt.Sprintf("bytes %d-%d/%d", begin, size-1, size), size-begin)
		streamErr := proxy.continuousStream(begin, wr)
		logIfFatal(streamErr, "连续流错误: %v", streamErr)
		stats.recordEnd(start, wr, ua, rangeHeader, wr.wrote, false, streamErr)
	}
}

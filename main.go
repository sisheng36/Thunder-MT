package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var version = "1.1.0"
var rangeRe = regexp.MustCompile(`bytes=(\d+)-(\d*)`)
var filenameStarRe = regexp.MustCompile(`filename\*\s*=\s*UTF-8''(.+)`)
var filenamePlainRe = regexp.MustCompile(`filename\s*=\s*"?([^";]+)"?`)

var allowedHosts map[string]bool
var dashboardToken string

func initAllowedHosts() {
	h := strings.TrimSpace(os.Getenv("ALLOW_HOSTS"))
	if h == "" {
		return
	}
	allowedHosts = make(map[string]bool)
	for _, host := range strings.Split(h, ",") {
		host = strings.TrimSpace(strings.ToLower(host))
		if host != "" {
			allowedHosts[host] = true
		}
	}
}

func isURLAllowed(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("URL 解析失败: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("协议不允许: %s", u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("缺少 host")
	}
	if allowedHosts != nil {
		host := strings.ToLower(u.Hostname())
		if !allowedHosts[host] {
			return fmt.Errorf("host 不在白名单: %s", host)
		}
	}
	return nil
}

func sanitizeFilename(name string) string {
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, name)
	name = strings.ReplaceAll(name, "\"", "")
	name = strings.ReplaceAll(name, "\\", "")
	name = strings.TrimSpace(name)
	if name == "" || name == "." || strings.ContainsAny(name, "/\r\n") {
		return "downloaded_file"
	}
	return name
}

func parseSize(s string) int64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0
	}
	multiplier := int64(1)
	last := s[len(s)-1]
	switch last {
	case 'K':
		multiplier = 1024
		s = s[:len(s)-1]
	case 'M':
		multiplier = 1024 * 1024
		s = s[:len(s)-1]
	case 'G':
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}
	if s == "" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return v * multiplier
}

// setHeaders 设置 Range 和自定义 headers 到请求, 统一 3 处重复样板
func setHeaders(req *http.Request, headers map[string]string, rangeVal string) {
	if rangeVal != "" {
		req.Header.Set("Range", rangeVal)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
}

func extractFileName(rawURL, contentDisposition string) string {
	if contentDisposition != "" {
		if m := filenameStarRe.FindStringSubmatch(contentDisposition); m != nil {
			if decoded, err := url.QueryUnescape(m[1]); err == nil {
				return sanitizeFilename(decoded)
			}
			return sanitizeFilename(m[1])
		}
		if m := filenamePlainRe.FindStringSubmatch(contentDisposition); m != nil {
			if decoded, err := url.QueryUnescape(m[1]); err == nil {
				return sanitizeFilename(decoded)
			}
			return sanitizeFilename(m[1])
		}
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "downloaded_file"
	}
	parts := strings.Split(u.Path, "/")
	if len(parts) > 0 {
		name := parts[len(parts)-1]
		if name != "" {
			if decoded, err := url.QueryUnescape(name); err == nil {
				return sanitizeFilename(decoded)
			}
			return sanitizeFilename(name)
		}
	}
	return "downloaded_file"
}

type urlProxy struct {
	url         string
	contentType string
	fileName    string
	length      int64
	trunk       int64
	split       int64
	conns       int
	client      *http.Client
	headers     map[string]string
}

func newURLProxy(targetURL string, trunk, split int64, conns int, headers map[string]string) (*urlProxy, error) {
	client := &http.Client{Timeout: 30 * time.Second}

	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		return nil, err
	}
	setHeaders(req, headers, "bytes=0-0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("获取文件信息失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("服务器返回 %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	fileName := extractFileName(targetURL, resp.Header.Get("Content-Disposition"))

	length := int64(0)
	cr := resp.Header.Get("Content-Range")
	if cr != "" {
		parts := strings.Split(cr, "/")
		if len(parts) >= 2 {
			length, _ = strconv.ParseInt(parts[len(parts)-1], 10, 64)
		}
	}
	if length == 0 {
		length = resp.ContentLength
	}
	if length == 0 {
		return nil, fmt.Errorf("无法获取文件大小")
	}

	return &urlProxy{
		url:         targetURL,
		contentType: contentType,
		fileName:    fileName,
		length:      length,
		trunk:       trunk,
		split:       split,
		conns:       conns,
		client:      client,
		headers:     headers,
	}, nil
}

func (p *urlProxy) downloadChunk(ctx context.Context, begin, end int64) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", p.url, nil)
		if err != nil {
			return nil, err
		}
		setHeaders(req, p.headers, fmt.Sprintf("bytes=%d-%d", begin, end))

		resp, err := p.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == 503 && attempt == 0 {
			resp.Body.Close()
			time.Sleep(time.Second)
			continue
		}
		if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("下载失败: %d", resp.StatusCode)
		}

		buf := make([]byte, end-begin+1)
		n, err := io.ReadFull(resp.Body, buf)
		resp.Body.Close()
		if err != nil || n < len(buf) {
			if err != nil {
				lastErr = err
			} else {
				lastErr = fmt.Errorf("短读: 期望 %d 实得 %d", len(buf), n)
			}
			if attempt < 1 {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			return nil, lastErr
		}
		return buf, nil
	}
	return nil, lastErr
}

type chunkData struct {
	start int64
	data  []byte
}

func (p *urlProxy) sortedStream(begin, end int64, w io.Writer) error {
	chunkSize := p.split
	totalChunks := int((end-begin)/chunkSize) + 1
	chunkCh := make(chan chunkData, totalChunks)
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)
		chunks := make(map[int64][]byte)
		nextPos := begin
		received := 0

		for received < totalChunks {
			select {
			case <-ctx.Done():
				return
			case ck, ok := <-chunkCh:
				if !ok {
					return
				}
				received++
				chunks[ck.start] = ck.data
				for {
					d, ok := chunks[nextPos]
					if !ok {
						break
					}
					delete(chunks, nextPos)
					if _, err := w.Write(d); err != nil {
						cancel()
						select {
						case errCh <- err:
						default:
						}
						return
					}
					nextPos += int64(len(d))
				}
			}
		}
	}()

	var wg sync.WaitGroup
	sem := make(chan struct{}, p.conns)

	for pos := begin; pos <= end; pos += chunkSize {
		chunkEnd := pos + chunkSize - 1
		if chunkEnd > end {
			chunkEnd = end
		}
		wg.Add(1)
		go func(start, chunkEnd int64) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			data, err := p.downloadChunk(ctx, start, chunkEnd)
			if err != nil {
				cancel()
				select {
				case errCh <- err:
				default:
				}
				return
			}
			select {
			case chunkCh <- chunkData{start: start, data: data}:
			case <-ctx.Done():
			}
		}(pos, chunkEnd)
	}

	wg.Wait()
	close(chunkCh)
	<-writerDone

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (p *urlProxy) continuousStream(begin int64, w io.Writer) error {
	nextBegin := begin
	for nextBegin < p.length {
		end := nextBegin + p.trunk - 1
		if end >= p.length {
			end = p.length - 1
		}
		if err := p.sortedStream(nextBegin, end, w); err != nil {
			return err
		}
		nextBegin = end + 1
	}
	return nil
}

type cachedProxy struct {
	lastAccess time.Time
	proxy      *urlProxy
}

type singleflightCall struct {
	wg  sync.WaitGroup
	val *urlProxy
	err error
}

type proxyCache struct {
	mu       sync.RWMutex
	items    map[string]*cachedProxy
	inflight map[string]*singleflightCall
	ttl      time.Duration
}

func newProxyCache(ttl time.Duration) *proxyCache {
	pc := &proxyCache{
		items:    make(map[string]*cachedProxy),
		inflight: make(map[string]*singleflightCall),
		ttl:      ttl,
	}
	go pc.cleanupLoop()
	return pc
}

func (pc *proxyCache) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		pc.cleanup()
	}
}

func (pc *proxyCache) cleanup() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	now := time.Now()
	cleaned := 0
	for k, v := range pc.items {
		if now.Sub(v.lastAccess) > pc.ttl {
			v.proxy.client.CloseIdleConnections()
			delete(pc.items, k)
			cleaned++
		}
	}
	if cleaned > 0 {
		log.Printf("缓存清理: %d 项已过期", cleaned)
	}
}

func (pc *proxyCache) get(key string) *urlProxy {
	pc.mu.RLock()
	entry, ok := pc.items[key]
	pc.mu.RUnlock()
	if !ok {
		return nil
	}
	pc.mu.Lock()
	pc.touchAndHit(entry)
	pc.mu.Unlock()
	return entry.proxy
}

func (pc *proxyCache) set(key string, proxy *urlProxy) {
	pc.mu.Lock()
	pc.items[key] = &cachedProxy{lastAccess: time.Now(), proxy: proxy}
	pc.mu.Unlock()
}

// touchAndHit 更新 lastAccess + 记 cache hit, 调用者必须持有 pc.mu 写锁
func (pc *proxyCache) touchAndHit(entry *cachedProxy) {
	entry.lastAccess = time.Now()
	stats.recordCacheHit()
}

func (pc *proxyCache) getOrCreate(key string, create func() (*urlProxy, error)) (*urlProxy, error) {
	if p := pc.get(key); p != nil {
		return p, nil
	}

	pc.mu.Lock()
	if entry, ok := pc.items[key]; ok {
		pc.touchAndHit(entry)
		pc.mu.Unlock()
		return entry.proxy, nil
	}
	if c, ok := pc.inflight[key]; ok {
		pc.mu.Unlock()
		c.wg.Wait()
		if c.err != nil {
			return nil, c.err
		}
		return c.val, nil
	}
	c := &singleflightCall{}
	c.wg.Add(1)
	pc.inflight[key] = c
	pc.mu.Unlock()

	proxy, err := create()
	c.val = proxy
	c.err = err

	pc.mu.Lock()
	delete(pc.inflight, key)
	if err == nil {
		if existing, ok := pc.items[key]; ok {
			proxy.client.CloseIdleConnections()
			pc.touchAndHit(existing)
			c.val = existing.proxy
		} else {
			pc.items[key] = &cachedProxy{lastAccess: time.Now(), proxy: proxy}
		}
	}
	pc.mu.Unlock()

	c.wg.Done()
	if err != nil {
		return nil, err
	}
	return c.val, nil
}

func resolveDirectURL(backendURL string, headers map[string]string) (string, error) {
	client := &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("重定向次数过多")
			}
			return nil
		},
	}

	req, err := http.NewRequest("GET", backendURL, nil)
	if err != nil {
		return "", err
	}
	setHeaders(req, headers, "bytes=0-0")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("解析直链失败: %w", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	return resp.Request.URL.String(), nil
}

type hourlyBucket struct {
	Hour    int   `json:"h"`
	Bytes   int64 `json:"b"`
	Streams int64 `json:"s"`
	Lavf    int64 `json:"l"`
	Errors  int64 `json:"e"`
}

type dailyRecord struct {
	Date    string `json:"date"`
	Bytes   int64  `json:"bytes"`
	Streams int64  `json:"streams"`
	Lavf    int64  `json:"lavf"`
	Errors  int64  `json:"errors"`
}

type logEntry struct {
	Time         string `json:"time"`
	UA           string `json:"ua"`
	Range        string `json:"range"`
	Bytes        int64  `json:"bytes"`
	Latency      int64  `json:"latency"`
	TransferTime int64  `json:"transferTime,omitempty"`
	Status       int    `json:"status"`
	Error        string `json:"error,omitempty"`
}

type statsSnapshot struct {
	Date              string           `json:"date"`
	Streams           int64            `json:"streams"`
	Lavf              int64            `json:"lavf"`
	SuccessRate       float64          `json:"successRate"`
	TotalBytes        int64            `json:"totalBytes"`
	TotalLatency      int64            `json:"totalLatency"`
	TotalTransferTime int64            `json:"totalTransferTime"`
	Errors            int64            `json:"errors"`
	AvgLatency        int64            `json:"avgLatency"`
	AvgTransferTime   int64            `json:"avgTransferTime"`
	CacheHits         int64            `json:"cacheHits"`
	Active            int32            `json:"active"`
	Hourly            [24]hourlyBucket `json:"hourly"`
	Daily             []dailyRecord    `json:"daily"`
	Logs              []logEntry       `json:"logs"`
}

type statsCollector struct {
	mu                sync.Mutex
	Date              string
	TotalStreams      int64
	TotalLavf         int64
	TotalSuccess      int64
	TotalErrors       int64
	TotalBytes        int64
	TotalLatency      int64
	TotalTransferTime int64
	CacheHits         int64
	ActiveStreams     int32
	Hourly            [24]hourlyBucket
	Daily             []dailyRecord
	Logs              []logEntry
	LogMax            int
}

var stats = &statsCollector{
	LogMax: 50,
	Date:   time.Now().Format("2006-01-02"),
	Daily:  make([]dailyRecord, 0),
	Logs:   make([]logEntry, 0, 50),
}

func (s *statsCollector) recordStart() time.Time { return time.Now() }

func (s *statsCollector) recordEnd(start time.Time, wr *responseWriter, ua, rangeStr string, bytes int64, isLavf bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkDate()
	now := time.Now()
	totalDuration := now.Sub(start).Milliseconds()
	// latency = TTFB(首字节延迟); 无首字节(0 bytes 或 Lavf 302)用 totalDuration 兜底
	var latency int64
	if wr != nil && wr.firstByteAt.IsZero() == false {
		latency = wr.firstByteAt.Sub(start).Milliseconds()
	} else {
		latency = totalDuration
	}
	s.TotalStreams++
	s.TotalLatency += latency
	s.TotalTransferTime += totalDuration
	s.Hourly[now.Hour()].Streams++
	if isLavf {
		s.TotalLavf++
		s.Hourly[now.Hour()].Lavf++
		return
	}
	if isFatal(err) {
		s.TotalErrors++
		s.Hourly[now.Hour()].Errors++
	} else {
		s.TotalSuccess++
	}
	s.TotalBytes += bytes
	s.Hourly[now.Hour()].Bytes += bytes
	entry := logEntry{
		Time:         now.Format("15:04:05"),
		UA:           shortUA(ua),
		Range:        rangeStr,
		Bytes:        bytes,
		Latency:      latency,
		TransferTime: totalDuration,
	}
	if isFatal(err) {
		entry.Error = err.Error()
		entry.Status = 500
	} else if isLavf {
		entry.Status = 302
	} else {
		entry.Status = 200
	}
	s.Logs = append([]logEntry{entry}, s.Logs...)
	if len(s.Logs) > s.LogMax {
		s.Logs = s.Logs[:s.LogMax]
	}
}

func (s *statsCollector) recordCacheHit() {
	s.mu.Lock()
	s.CacheHits++
	s.mu.Unlock()
}

func (s *statsCollector) checkDate() {
	today := time.Now().Format("2006-01-02")
	if today == s.Date {
		return
	}
	var dayBytes, dayStreams, dayLavf, dayErrors int64
	for _, h := range s.Hourly {
		dayBytes += h.Bytes
		dayStreams += h.Streams
		dayLavf += h.Lavf
		dayErrors += h.Errors
	}
	s.Daily = append([]dailyRecord{{Date: s.Date, Bytes: dayBytes, Streams: dayStreams, Lavf: dayLavf, Errors: dayErrors}}, s.Daily...)
	if len(s.Daily) > 30 {
		s.Daily = s.Daily[:30]
	}
	s.Date = today
	s.TotalBytes = 0
	s.TotalStreams = 0
	s.TotalLavf = 0
	s.TotalSuccess = 0
	s.TotalErrors = 0
	s.TotalLatency = 0
	s.TotalTransferTime = 0
	s.CacheHits = 0
	s.Hourly = [24]hourlyBucket{}
}

func shortUA(ua string) string {
	switch {
	case strings.Contains(ua, "Lavf"):
		return "ffprobe"
	case strings.Contains(ua, "libmpv"):
		return "mpv"
	case strings.Contains(ua, "Infuse"):
		return "Infuse"
	case ua == "":
		return "-"
	case len(ua) > 30:
		return ua[:30]
	default:
		return ua
	}
}

func (s *statsCollector) snapshot() statsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	realStreams := s.TotalStreams - s.TotalLavf
	successRate := 0.0
	if realStreams > 0 {
		successRate = float64(s.TotalSuccess) / float64(realStreams) * 100
	}
	avgLatency := int64(0)
	avgTransferTime := int64(0)
	if s.TotalStreams > 0 {
		avgLatency = s.TotalLatency / s.TotalStreams
		avgTransferTime = s.TotalTransferTime / s.TotalStreams
	}
	logsCopy := make([]logEntry, len(s.Logs))
	copy(logsCopy, s.Logs)
	dailyCopy := make([]dailyRecord, len(s.Daily))
	copy(dailyCopy, s.Daily)
	return statsSnapshot{
		Date:              s.Date,
		Streams:           s.TotalStreams,
		Lavf:              s.TotalLavf,
		SuccessRate:       successRate,
		TotalBytes:        s.TotalBytes,
		TotalLatency:      s.TotalLatency,
		TotalTransferTime: s.TotalTransferTime,
		Errors:            s.TotalErrors,
		AvgLatency:        avgLatency,
		AvgTransferTime:   avgTransferTime,
		CacheHits:         s.CacheHits,
		Active:            s.ActiveStreams,
		Hourly:            s.Hourly, // [24]数组是值类型,赋值即深拷贝
		Daily:             dailyCopy,
		Logs:              logsCopy,
	}
}

func (s *statsCollector) save(path string) {
	data, _ := json.MarshalIndent(s.snapshot(), "", "  ")
	os.MkdirAll("/data", 0755)
	os.WriteFile(path, data, 0644)
}

func (s *statsCollector) load(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var snap statsSnapshot
	if json.Unmarshal(data, &snap) != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// 只恢复今天的数据; 跨天则从空白开始(历史已在 Daily 里)
	if snap.Date != time.Now().Format("2006-01-02") {
		// 仍恢复 Daily 历史(跨天看趋势)
		s.Daily = snap.Daily
		return
	}
	s.Date = snap.Date
	s.TotalStreams = snap.Streams
	s.TotalLavf = snap.Lavf
	s.TotalErrors = snap.Errors
	s.TotalBytes = snap.TotalBytes
	s.CacheHits = snap.CacheHits
	s.TotalLatency = snap.TotalLatency
	// TTFB 迁移: 旧版本(v<=1.0.7)的 totalLatency 实为传输时长,不是 TTFB
	// 新版本若提供 totalTransferTime, 优先用; 否则把旧 totalLatency 当 transferTime, TTFB 重置
	if snap.TotalTransferTime > 0 {
		s.TotalTransferTime = snap.TotalTransferTime
	} else {
		s.TotalTransferTime = snap.TotalLatency
		s.TotalLatency = 0
	}
	if snap.SuccessRate > 0 {
		s.TotalSuccess = int64(float64(s.TotalStreams) * snap.SuccessRate / 100)
	}
	s.Daily = snap.Daily
	s.Logs = snap.Logs
	if len(s.Logs) > s.LogMax {
		s.Logs = s.Logs[:s.LogMax]
	}
	s.Hourly = snap.Hourly
}

type server struct {
	trunk      int64
	split      int64
	firstChunk int64
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

func newServer(trunk, split, firstChunk string, conns int, headers map[string]string) *server {
	cacheTTL := 300 * time.Second
	fc := parseSize(firstChunk)
	if fc <= 0 {
		fc = 2 * 1024 * 1024
	}
	return &server{
		trunk:      parseSize(trunk),
		split:      parseSize(split),
		firstChunk: fc,
		conns:      conns,
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

const dashboardHTML = `<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Thunder-MT Dashboard</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:linear-gradient(135deg,#0f0f1a 0%,#1a1025 50%,#0f0f1a 100%);color:#e0e0e0;min-height:100vh;padding:20px}
h1{font-size:24px;font-weight:600;margin-bottom:4px;background:linear-gradient(135deg,#a78bfa,#60a5fa);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.subtitle{color:#6b7280;font-size:13px;margin-bottom:24px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:24px}
.card{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:16px;backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);transition:transform .15s,border-color .15s}
.card:hover{transform:translateY(-1px);border-color:rgba(255,255,255,0.15)}
.card-label{font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}
.card-value{font-size:26px;font-weight:700;color:#f0f0f0}
.card-value.green{color:#34d399}
.card-value.blue{color:#60a5fa}
.card-value.yellow{color:#fbbf24}
.card-value.red{color:#f87171}
.chart-card{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:16px;backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);margin-bottom:24px}
.chart-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px}
.chart-title{font-size:14px;font-weight:600}
.btn-group{display:flex;gap:4px}
.btn{padding:5px 12px;border:1px solid rgba(255,255,255,0.10);background:rgba(255,255,255,0.05);color:#9ca3af;border-radius:6px;cursor:pointer;font-size:12px;transition:all .15s}
.btn:hover{background:rgba(255,255,255,0.10);color:#e0e0e0}
.btn.active{background:rgba(139,92,246,0.25);border-color:rgba(139,92,246,0.5);color:#a78bfa}
canvas{width:100%;height:260px}
.table-card{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:16px;backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:8px 12px;color:#6b7280;font-weight:500;border-bottom:1px solid rgba(255,255,255,0.08);font-size:11px;text-transform:uppercase}
td{padding:8px 12px;border-bottom:1px solid rgba(255,255,255,0.04);white-space:nowrap}
tr:last-child td{border-bottom:none}
.status-200{color:#34d399}
.status-302{color:#fbbf24}
.status-500{color:#f87171}
.empty{text-align:center;color:#4b5563;padding:32px;font-style:italic}
.badge{display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;font-weight:600}
.badge-ok{background:rgba(52,211,153,0.15);color:#34d399}
.badge-err{background:rgba(248,113,113,0.15);color:#f87171}
.footer{text-align:center;color:#374151;font-size:11px;margin-top:24px}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.loading{animation:pulse 2s infinite}
</style>
</head>
<body>
<h1>Thunder-MT</h1>
<div class="subtitle">Stats Dashboard <span id="clock" class="loading"></span></div>
<div class="grid">
<div class="card"><div class="card-label">Streams</div><div class="card-value blue" id="v-streams">-</div></div>
<div class="card"><div class="card-label">Lavf / ffprobe</div><div class="card-value yellow" id="v-lavf">-</div></div>
<div class="card"><div class="card-label">Success Rate</div><div class="card-value green" id="v-rate">-</div></div>
<div class="card"><div class="card-label">Cache Hits</div><div class="card-value blue" id="v-cache">-</div></div>
<div class="card"><div class="card-label">Traffic</div><div class="card-value" id="v-bytes">-</div></div>
<div class="card"><div class="card-label">Errors</div><div class="card-value red" id="v-errors">-</div></div>
<div class="card"><div class="card-label">Active</div><div class="card-value green" id="v-active">-</div></div>
<div class="card"><div class="card-label">Avg Latency</div><div class="card-value" id="v-latency">-</div></div>
<div class="card"><div class="card-label">Avg Transfer</div><div class="card-value" id="v-transfer">-</div></div>
</div>
<div class="chart-card">
<div class="chart-header">
<span class="chart-title">Traffic (Bytes)</span>
<div class="btn-group">
<button class="btn active" data-range="1h">1H</button>
<button class="btn" data-range="6h">6H</button>
<button class="btn" data-range="24h">24H</button>
<button class="btn" data-range="7d">7D</button>
<button class="btn" data-range="15d">15D</button>
<button class="btn" data-range="30d">30D</button>
</div>
</div>
<canvas id="chart"></canvas>
</div>
<div class="table-card">
<table>
<thead><tr><th>Time</th><th>UA</th><th>Range</th><th>Bytes</th><th>TTFB</th><th>Transfer</th><th>Status</th></tr></thead>
<tbody id="log-body"><tr><td colspan="7" class="empty">No data yet</td></tr></tbody>
</table>
</div>
<div class="footer">Thunder-MT Proxy &middot; auto-refresh every 10s</div>
<script>
function humanizeBytes(b){
if(b===undefined||b===null)return'0 B';
if(b<1024)return b+' B';
var kb=b/1024;
if(kb<1024)return kb.toFixed(1)+' KB';
var mb=kb/1024;
if(mb<1024)return mb.toFixed(1)+' MB';
var gb=mb/1024;
return gb.toFixed(2)+' GB';
}
function humanizeMs(ms){
if(ms===undefined||ms===null)return'0ms';
if(ms<1000)return ms+'ms';
return(ms/1000).toFixed(1)+'s';
}
function fmtPct(v){
if(v===undefined||v===null)return'-';
return v.toFixed(1)+'%';
}
var chartRange='1h';
var chartData=null;
var canvas=document.getElementById('chart');
var ctx=canvas.getContext('2d');
function drawChart(){
if(!canvas||!ctx)return;
var dpr=window.devicePixelRatio||1;
var rect=canvas.getBoundingClientRect();
canvas.width=rect.width*dpr;
canvas.height=rect.height*dpr;
ctx.setTransform(dpr,0,0,dpr,0,0);
var w=rect.width;
var h=rect.height;
ctx.clearRect(0,0,w,h);
var datasets=[];
var labels=[];
if(chartRange==='1h'||chartRange==='6h'||chartRange==='24h'){
var hours=chartRange==='1h'?1:chartRange==='6h'?6:24;
var now=new Date().getHours();
for(var i=hours-1;i>=0;i--){
var hr=(now-i+24)%24;
labels.push(hr+':00');
}
if(chartData&&chartData.hourly){
for(var j=0;j<hours;j++){
var idx=(now-j+24)%24;
var bk=chartData.hourly[idx]||{b:0};
datasets.push(bk.b);
}
datasets.reverse();
}
}else{
var days=chartRange==='7d'?7:chartRange==='15d'?15:30;
if(chartData&&chartData.daily){
var dl=chartData.daily.slice(0,days).reverse();
for(var k=0;k<dl.length;k++){
labels.push(dl[k].date.slice(5));
datasets.push(dl[k].bytes||0);
}
}else{
for(var d=0;d<Math.min(days,30);d++){labels.push('--');datasets.push(0);}
}
}
if(datasets.length===0){ctx.fillStyle='#4b5563';ctx.font='14px sans-serif';ctx.textAlign='center';ctx.fillText('No data',w/2,h/2);return}
var maxVal=Math.max.apply(null,datasets);
if(maxVal===0)maxVal=1;
var padding={top:10,right:16,bottom:32,left:58};
var pw=w-padding.left-padding.right;
var ph=h-padding.top-padding.bottom;
ctx.strokeStyle='rgba(255,255,255,0.06)';
ctx.lineWidth=1;
for(var i=0;i<=4;i++){
var y=padding.top+(ph/4)*i;
ctx.beginPath();
ctx.moveTo(padding.left,y);
ctx.lineTo(w-padding.right,y);
ctx.stroke();
var lbl=i===0?humanizeBytes(maxVal):humanizeBytes(maxVal*(1-i/4));
ctx.fillStyle='#4b5563';
ctx.font='10px sans-serif';
ctx.textAlign='right';
ctx.fillText(lbl,padding.left-6,y+4);
}
var gradient=ctx.createLinearGradient(0,padding.top,0,h-padding.bottom);
gradient.addColorStop(0,'rgba(139,92,246,0.25)');
gradient.addColorStop(1,'rgba(96,165,250,0.05)');
ctx.beginPath();
var barWidth=pw/datasets.length*.6;
var gap=pw/datasets.length*.4;
datasets.forEach(function(v,i){
var barH=(v/maxVal)*ph;
var x=padding.left+i*(barWidth+gap)+gap/2;
var y=h-padding.bottom-barH;
ctx.fillStyle=gradient;
ctx.fillRect(x,y,barWidth,barH);
ctx.fillStyle='#6b7280';
ctx.font='10px sans-serif';
ctx.textAlign='center';
var showLabel=(chartRange==='24h'&&labels.length>12)?(i%4===0):(chartRange==='7d'||chartRange==='15d'||chartRange==='30d')?(i%Math.ceil(labels.length/8)===0):true;
if(showLabel)ctx.fillText(labels[i],x+barWidth/2,h-padding.bottom+16);
});
}
function updateClock(){document.getElementById('clock').textContent=new Date().toLocaleTimeString();document.getElementById('clock').classList.remove('loading');}
function esc(s){
if(s===undefined||s===null)return'';
return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
function renderTable(logs){
var tbody=document.getElementById('log-body');
if(!logs||logs.length===0){tbody.innerHTML='<tr><td colspan="7" class="empty">No data yet</td></tr>';return}
var shown=logs.slice(0,10);
var cls={'200':'status-200','302':'status-302','500':'status-500'};
tbody.innerHTML=shown.map(function(l){
var sc=cls[l.status]||'';
var errBadge=l.error?' <span class="badge badge-err">'+esc(l.error.substring(0,30))+'</span>':'';
return'<tr><td>'+esc(l.time)+'</td><td>'+esc(l.ua)+'</td><td>'+esc(l.range)+'</td><td>'+humanizeBytes(l.bytes)+'</td><td>'+humanizeMs(l.latency)+'</td><td>'+humanizeMs(l.transferTime||l.latency)+'</td><td class="'+sc+'">'+esc(l.status)+'</td></tr>';
}).join('');
}
function refresh(){
fetch('/api/stats').then(function(r){return r.json();}).then(function(d){
chartData=d;
document.getElementById('v-streams').textContent=d.streams||0;
document.getElementById('v-lavf').textContent=d.lavf||0;
document.getElementById('v-rate').textContent=fmtPct(d.successRate);
document.getElementById('v-cache').textContent=d.cacheHits||0;
document.getElementById('v-bytes').textContent=humanizeBytes(d.totalBytes);
document.getElementById('v-errors').textContent=d.errors||0;
document.getElementById('v-active').textContent=d.active||0;
document.getElementById('v-latency').textContent=humanizeMs(d.avgLatency);
document.getElementById('v-transfer').textContent=humanizeMs(d.avgTransferTime);
renderTable(d.logs);
drawChart();
updateClock();
}).catch(function(){});
}
document.querySelectorAll('.btn').forEach(function(b){
b.addEventListener('click',function(){
document.querySelectorAll('.btn').forEach(function(x){x.classList.remove('active');});
b.classList.add('active');
chartRange=b.dataset.range;
drawChart();
});
});
refresh();
setInterval(refresh,10000);
window.addEventListener('resize',drawChart);
</script>
</body>
</html>`


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
	json.NewEncoder(w).Encode(stats.snapshot())
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
		return newURLProxy(directURL, s.trunk, s.split, s.conns, s.headers)
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
	conns := 60
	if connsStr != "" {
		conns, _ = strconv.Atoi(connsStr)
	}
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

	os.MkdirAll("/data", 0755)
	stats.load("/data/stats.json")
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			stats.save("/data/stats.json")
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
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Printf("收到信号，优雅关停...")
		stats.save("/data/stats.json")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		httpSrv.Shutdown(ctx)
	}()

	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("启动失败: %v", err)
	}
	log.Printf("已停止")
}

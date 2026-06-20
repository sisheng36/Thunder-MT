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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var version = "1.0.5"
var rangeRe = regexp.MustCompile(`bytes=(\d+)-(\d*)`)
var filenameRe = regexp.MustCompile(`filename\*=UTF-8''(.+)`)

func parseSize(s string) int64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	if len(s) < 2 {
		v, _ := strconv.ParseInt(s, 10, 64)
		return v
	}
	v, _ := strconv.ParseInt(s[:len(s)-1], 10, 64)
	switch s[len(s)-1] {
	case 'K':
		return v * 1024
	case 'M':
		return v * 1024 * 1024
	case 'G':
		return v * 1024 * 1024 * 1024
	default:
		return v
	}
}

func extractFileName(rawURL, contentDisposition string) string {
	if contentDisposition != "" {
		m := filenameRe.FindStringSubmatch(contentDisposition)
		if m != nil {
			decoded, err := url.QueryUnescape(m[1])
			if err == nil {
				return decoded
			}
			return m[1]
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
			decoded, err := url.QueryUnescape(name)
			if err == nil {
				return decoded
			}
			return name
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
	req.Header.Set("Range", "bytes=0-0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

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
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", begin, end))
		for k, v := range p.headers {
			req.Header.Set(k, v)
		}

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
		if err != nil && err != io.ErrUnexpectedEOF {
			lastErr = err
			if attempt < 1 {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			return nil, err
		}
		return buf[:n], nil
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

type proxyCache struct {
	mu    sync.RWMutex
	items map[string]*cachedProxy
	ttl   time.Duration
}

func newProxyCache(ttl time.Duration) *proxyCache {
	pc := &proxyCache{
		items: make(map[string]*cachedProxy),
		ttl:   ttl,
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
	entry.lastAccess = time.Now()
	pc.mu.Unlock()
	stats.recordCacheHit()
	return entry.proxy
}

func (pc *proxyCache) set(key string, proxy *urlProxy) {
	pc.mu.Lock()
	pc.items[key] = &cachedProxy{lastAccess: time.Now(), proxy: proxy}
	pc.mu.Unlock()
}

func (pc *proxyCache) getOrCreate(key string, create func() (*urlProxy, error)) (*urlProxy, error) {
	if p := pc.get(key); p != nil {
		return p, nil
	}

	pc.mu.Lock()
	if entry, ok := pc.items[key]; ok {
		pc.mu.Unlock()
		entry.lastAccess = time.Now()
		stats.recordCacheHit()
		return entry.proxy, nil
	}
	pc.mu.Unlock()

	proxy, err := create()
	if err != nil {
		return nil, err
	}

	pc.mu.Lock()
	if entry, ok := pc.items[key]; ok {
		pc.mu.Unlock()
		proxy.client.CloseIdleConnections()
		entry.lastAccess = time.Now()
		stats.recordCacheHit()
		return entry.proxy, nil
	}
	pc.items[key] = &cachedProxy{lastAccess: time.Now(), proxy: proxy}
	pc.mu.Unlock()
	return proxy, nil
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
	req.Header.Set("Range", "bytes=0-0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("解析直链失败: %w", err)
	}
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
	Time    string `json:"time"`
	UA      string `json:"ua"`
	Range   string `json:"range"`
	Bytes   int64  `json:"bytes"`
	Latency int64  `json:"latency"`
	Status  int    `json:"status"`
	Error   string `json:"error,omitempty"`
}

type statsCollector struct {
	mu            sync.Mutex
	Date          string
	TotalStreams  int64
	TotalLavf     int64
	TotalSuccess  int64
	TotalErrors   int64
	TotalBytes    int64
	TotalLatency  int64
	CacheHits     int64
	ActiveStreams int32
	Hourly        [24]hourlyBucket
	Daily         []dailyRecord
	Logs          []logEntry
	LogMax        int
}

var stats = &statsCollector{
	LogMax: 50,
	Date:   time.Now().Format("2006-01-02"),
	Daily:  make([]dailyRecord, 0),
	Logs:   make([]logEntry, 0, 50),
}

func (s *statsCollector) recordStart() time.Time { return time.Now() }

func (s *statsCollector) recordEnd(start time.Time, ua, rangeStr string, bytes int64, isLavf bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkDate()
	now := time.Now()
	latency := now.Sub(start).Milliseconds()
	s.TotalStreams++
	s.TotalLatency += latency
	s.Hourly[now.Hour()].Streams++
	if isLavf {
		s.TotalLavf++
		s.Hourly[now.Hour()].Lavf++
		return
	}
	if err != nil {
		s.TotalErrors++
		s.Hourly[now.Hour()].Errors++
	} else {
		s.TotalSuccess++
	}
	s.TotalBytes += bytes
	s.Hourly[now.Hour()].Bytes += bytes
	entry := logEntry{Time: now.Format("15:04:05"), UA: shortUA(ua), Range: rangeStr, Bytes: bytes, Latency: latency}
	if err != nil {
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
	s.CacheHits = 0
	s.Hourly = [24]hourlyBucket{}
}

func shortUA(ua string) string {
	if strings.Contains(ua, "Lavf") {
		return "ffprobe"
	}
	if strings.Contains(ua, "libmpv") {
		return "mpv"
	}
	if strings.Contains(ua, "Infuse") {
		return "Infuse"
	}
	if ua == "" {
		return "-"
	}
	if len(ua) > 30 {
		return ua[:30]
	}
	return ua
}

func (s *statsCollector) snapshot() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := s.TotalStreams + s.TotalLavf
	successRate := 0.0
	if s.TotalStreams > 0 {
		successRate = float64(s.TotalSuccess) / float64(s.TotalStreams) * 100
	}
	avgLatency := int64(0)
	if total > 0 {
		avgLatency = s.TotalLatency / total
	}
	return map[string]interface{}{
		"date":        s.Date,
		"streams":     s.TotalStreams,
		"lavf":        s.TotalLavf,
		"successRate": successRate,
		"totalBytes":  s.TotalBytes,
		"errors":      s.TotalErrors,
		"avgLatency":  avgLatency,
		"cacheHits":   s.CacheHits,
		"active":      s.ActiveStreams,
		"hourly":      s.Hourly[:],
		"daily":       s.Daily,
		"logs":        s.Logs,
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
	var snap map[string]interface{}
	if json.Unmarshal(data, &snap) != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := snap["date"].(string); ok {
		if v == time.Now().Format("2006-01-02") {
			s.Date = v
			if n, ok := snap["streams"].(float64); ok {
				s.TotalStreams = int64(n)
			}
			if n, ok := snap["lavf"].(float64); ok {
				s.TotalLavf = int64(n)
			}
			if n, ok := snap["errors"].(float64); ok {
				s.TotalErrors = int64(n)
			}
			if n, ok := snap["totalBytes"].(float64); ok {
				s.TotalBytes = int64(n)
			}
			if n, ok := snap["cacheHits"].(float64); ok {
				s.CacheHits = int64(n)
			}
			if n, ok := snap["successRate"].(float64); ok {
				s.TotalSuccess = int64(float64(s.TotalStreams) * n / 100)
			}
		}
	}
	if daily, ok := snap["daily"].([]interface{}); ok {
		for _, d := range daily {
			if dm, ok := d.(map[string]interface{}); ok {
				dr := dailyRecord{}
				if date, ok := dm["date"].(string); ok {
					dr.Date = date
				}
				if b, ok := dm["bytes"].(float64); ok {
					dr.Bytes = int64(b)
				}
				if ss, ok := dm["streams"].(float64); ok {
					dr.Streams = int64(ss)
				}
				if l, ok := dm["lavf"].(float64); ok {
					dr.Lavf = int64(l)
				}
				if e, ok := dm["errors"].(float64); ok {
					dr.Errors = int64(e)
				}
				s.Daily = append(s.Daily, dr)
			}
		}
	}
}

type server struct {
	trunk   int64
	split   int64
	conns   int
	headers map[string]string
	cache   *proxyCache
}

func isFatal(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return !strings.Contains(msg, "broken pipe") && !strings.Contains(msg, "connection reset")
}

func flushHeaders(w http.ResponseWriter, contentRange string, contentLength int64) {
	w.Header().Set("Content-Range", contentRange)
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.WriteHeader(http.StatusPartialContent)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func logIfFatal(err error, format string, args ...interface{}) {
	if isFatal(err) {
		log.Printf(format, args...)
	}
}

func newServer(trunk, split string, conns int, headers map[string]string) *server {
	cacheTTL := 300 * time.Second
	return &server{
		trunk:   parseSize(trunk),
		split:   parseSize(split),
		conns:   conns,
		headers: headers,
		cache:   newProxyCache(cacheTTL),
	}
}

type responseWriter struct {
	http.ResponseWriter
	wrote int64
}

func (w *responseWriter) Write(b []byte) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("write panic: %v", r)
		}
	}()
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
<thead><tr><th>Time</th><th>UA</th><th>Range</th><th>Bytes</th><th>Latency</th><th>Status</th></tr></thead>
<tbody id="log-body"><tr><td colspan="6" class="empty">No data yet</td></tr></tbody>
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
function renderTable(logs){
var tbody=document.getElementById('log-body');
if(!logs||logs.length===0){tbody.innerHTML='<tr><td colspan="6" class="empty">No data yet</td></tr>';return}
var shown=logs.slice(0,10);
var cls={'200':'status-200','302':'status-302','500':'status-500'};
tbody.innerHTML=shown.map(function(l){
var sc=cls[l.status]||'';
var errBadge=l.error?' <span class="badge badge-err">'+l.error.substring(0,30)+'</span>':'';
return'<tr><td>'+l.time+'</td><td>'+l.ua+'</td><td>'+l.range+'</td><td>'+humanizeBytes(l.bytes)+'</td><td>'+humanizeMs(l.latency)+'</td><td class="'+sc+'">'+l.status+'</td></tr>';
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
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML))
}

func (s *server) handleAPIStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats.snapshot())
}

func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	start := stats.recordStart()
	wr := &responseWriter{ResponseWriter: w}
	ua := r.Header.Get("User-Agent")
	rangeHeader := r.Header.Get("Range")

	backendURL := r.URL.Query().Get("url")
	if backendURL == "" {
		http.Error(wr, "Missing 'url' parameter", http.StatusBadRequest)
		stats.recordEnd(start, ua, rangeHeader, 0, false, fmt.Errorf("missing url"))
		return
	}

	if strings.Contains(ua, "Lavf") {
		log.Printf("Lavf 302 → %s", backendURL)
		http.Redirect(wr, r, backendURL, http.StatusFound)
		stats.recordEnd(start, ua, rangeHeader, 0, true, nil)
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
		stats.recordEnd(start, ua, rangeHeader, 0, false, err)
		return
	}

	size := proxy.length

	disposition := fmt.Sprintf(`inline; filename="%s"`, proxy.fileName)
	wr.Header().Set("Content-Type", proxy.contentType)
	wr.Header().Set("Content-Disposition", disposition)
	wr.Header().Set("Accept-Ranges", "bytes")

	var streamErr error

	if rangeHeader == "" {
		firstEnd := proxy.split - 1
		if firstEnd >= size {
			firstEnd = size - 1
		}
		log.Printf("无 Range: 首 chunk 0→%d, size=%d", firstEnd, size)
		flushHeaders(wr, fmt.Sprintf("bytes 0-%d/%d", firstEnd, size), firstEnd+1)
		streamErr = proxy.sortedStream(0, firstEnd, wr)
		logIfFatal(streamErr, "连续流错误: %v", streamErr)
		stats.recordEnd(start, ua, rangeHeader, wr.wrote, false, streamErr)
		return
	}

	m := rangeRe.FindStringSubmatch(rangeHeader)
	if m == nil {
		http.Error(wr, "Invalid Range header", http.StatusRequestedRangeNotSatisfiable)
		stats.recordEnd(start, ua, rangeHeader, 0, false, fmt.Errorf("invalid range"))
		return
	}

	begin, _ := strconv.ParseInt(m[1], 10, 64)
	endStr := m[2]

	if begin >= size {
		wr.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		http.Error(wr, "Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)
		stats.recordEnd(start, ua, rangeHeader, 0, false, fmt.Errorf("range out of bounds"))
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
		length := end - begin + 1
		log.Printf("Range(B): %s → begin=%d end=%d length=%d", rangeHeader, begin, end, length)
		flushHeaders(wr, fmt.Sprintf("bytes %d-%d/%d", begin, end, size), length)
		streamErr = proxy.sortedStream(begin, end, wr)
		logIfFatal(streamErr, "sortedStream 错误: %v", streamErr)
	} else {
		log.Printf("Range(U): %s 连续流 %d→%d", rangeHeader, begin, size)
		flushHeaders(wr, fmt.Sprintf("bytes %d-%d/%d", begin, size-1, size), size-begin)
		streamErr = proxy.continuousStream(begin, wr)
		logIfFatal(streamErr, "连续流错误: %v", streamErr)
	}
	stats.recordEnd(start, ua, rangeHeader, wr.wrote, false, streamErr)
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
	if h := os.Getenv("HEADERS"); h != "" {
		h = strings.TrimSpace(h)
		if strings.HasPrefix(h, "{") {
			keyVal := strings.Trim(h, "{}")
			for _, pair := range strings.Split(keyVal, ",") {
				parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
				if len(parts) == 2 {
					k := strings.Trim(strings.TrimSpace(parts[0]), `"`)
					v := strings.Trim(strings.TrimSpace(parts[1]), `"`)
					headers[k] = v
				}
			}
		}
	}

	srv := newServer(trunk, split, conns, headers)

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
	log.Printf("配置: trunk=%s split=%s conns=%d", trunk, split, conns)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("启动失败: %v", err)
	}
}

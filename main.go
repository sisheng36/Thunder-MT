package main

import (
	"context"
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

var version = "1.0.3"
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
		headers:     headers,
	}, nil
}

func (p *urlProxy) downloadChunk(client *http.Client, begin, end int64) ([]byte, error) {
	req, err := http.NewRequest("GET", p.url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", begin, end))
	for k, v := range p.headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("下载失败: %d", resp.StatusCode)
	}

	buf := make([]byte, end-begin+1)
	n, err := io.ReadFull(resp.Body, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	return buf[:n], nil
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

	transport := &http.Transport{
		MaxConnsPerHost: p.conns,
	}
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	defer transport.CloseIdleConnections()

	go func() {
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

			data, err := p.downloadChunk(client, start, chunkEnd)
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
		entry.lastAccess = time.Now()
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

type server struct {
	trunk   int64
	split   int64
	conns   int
	headers map[string]string
	cache   *proxyCache
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

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

func (s *server) handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"service":"Thunder-MT","version":"%s"}`, version)
}

func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	backendURL := r.URL.Query().Get("url")
	if backendURL == "" {
		http.Error(w, "Missing 'url' parameter", http.StatusBadRequest)
		return
	}

	if strings.Contains(r.Header.Get("User-Agent"), "Lavf") {
		log.Printf("Lavf 302 → %s", backendURL)
		http.Redirect(w, r, backendURL, http.StatusFound)
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
		http.Error(w, "无法解析后端地址", http.StatusBadGateway)
		return
	}

	size := proxy.length
	rangeHeader := r.Header.Get("Range")

	disposition := fmt.Sprintf(`inline; filename="%s"`, proxy.fileName)
	w.Header().Set("Content-Type", proxy.contentType)
	w.Header().Set("Content-Disposition", disposition)
	w.Header().Set("Accept-Ranges", "bytes")

	if rangeHeader == "" {
		log.Printf("无 Range: 连续流 0→%d, trunk=%d", size, proxy.trunk)
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if err := proxy.continuousStream(0, w); err != nil {
			log.Printf("连续流错误: %v", err)
		}
		return
	}

	m := rangeRe.FindStringSubmatch(rangeHeader)
	if m == nil {
		http.Error(w, "Invalid Range header", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	begin, _ := strconv.ParseInt(m[1], 10, 64)
	endStr := m[2]

	if begin >= size {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		http.Error(w, "Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)
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

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", begin, end, size))
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.WriteHeader(http.StatusPartialContent)

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if err := proxy.sortedStream(begin, end, w); err != nil {
			log.Printf("sortedStream 错误: %v", err)
		}
	} else {
		log.Printf("Range(U): %s 连续流 %d→%d", rangeHeader, begin, size)

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", begin, size-1, size))
		w.Header().Set("Content-Length", strconv.FormatInt(size-begin, 10))
		w.WriteHeader(http.StatusPartialContent)

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if err := proxy.continuousStream(begin, w); err != nil {
			log.Printf("连续流错误: %v", err)
		}
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

	mux := http.NewServeMux()
	mux.HandleFunc("/health", srv.handleHealth)
	mux.HandleFunc("/stream", srv.handleStream)
	mux.HandleFunc("/", srv.handleRoot)

	addr := fmt.Sprintf("%s:%s", host, port)
	log.Printf("Thunder-MT v%s 启动，监听 %s", version, addr)
	log.Printf("配置: trunk=%s split=%s conns=%d", trunk, split, conns)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("启动失败: %v", err)
	}
}

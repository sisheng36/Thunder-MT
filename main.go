package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Listen  string
	Piece   int64
	Buffer  int64
	Workers int
	Timeout time.Duration
	TTL     time.Duration
}

type piece struct {
	offset int64
	data   []byte
}

type session struct {
	srv *Server

	url           string
	contentLength int64
	contentType   string
	pieceSize     int64
	bufferSize    int64

	err       error
	infoReady bool

	mu    sync.Mutex
	cond  *sync.Cond
	ctx   context.Context
	cancel context.CancelFunc

	chunks      []piece
	downloading map[int64]bool
	playHead    int64
	lastAccess  time.Time
}

type Server struct {
	cfg      Config
	sessions sync.Map
	sem      chan struct{}
}

func parseSize(s string) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	m := int64(1)
	switch {
	case strings.HasSuffix(s, "G"):
		m, s = 1<<30, strings.TrimSuffix(s, "G")
	case strings.HasSuffix(s, "M"):
		m, s = 1<<20, strings.TrimSuffix(s, "M")
	case strings.HasSuffix(s, "K"):
		m, s = 1<<10, strings.TrimSuffix(s, "K")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, err
	}
	return n * m, nil
}

func formatSize(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fG", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fM", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fK", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func main() {
	cfg := Config{
		Listen:  ":8010",
		Piece:   1 << 20,
		Buffer:  50 << 20,
		Workers: 10,
		Timeout: 30 * time.Second,
		TTL:     120 * time.Second,
	}

	flag.StringVar(&cfg.Listen, "listen", cfg.Listen, "listen address")
	flag.Func("piece", "piece size (default 1M)", func(s string) error { n, e := parseSize(s); cfg.Piece = n; return e })
	flag.Func("buffer", "max buffer per session (default 50M)", func(s string) error { n, e := parseSize(s); cfg.Buffer = n; return e })
	flag.IntVar(&cfg.Workers, "workers", cfg.Workers, "max concurrent downloads")
	flag.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "per-piece download timeout")
	flag.DurationVar(&cfg.TTL, "session-ttl", cfg.TTL, "idle session cleanup TTL")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "thunder-mt - multi-threaded stream engine for SmartStrm\n\nFlags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExample:\n  thunder-mt --listen :8010 --piece 1M --buffer 50M --workers 10\n")
	}
	flag.Parse()

	if cfg.Piece <= 0 {
		log.Fatal("piece must be positive")
	}
	if cfg.Buffer < cfg.Piece*2 {
		log.Fatal("buffer must be >= 2x piece")
	}
	if cfg.Workers <= 0 {
		log.Fatal("workers must be positive")
	}

	s := &Server{
		cfg: cfg,
		sem: make(chan struct{}, cfg.Workers),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/stream", s.streamHandler)
	mux.HandleFunc("/health", s.healthHandler)

	httpServer := &http.Server{
		Addr:         cfg.Listen,
		Handler:      mux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("thunder-mt start: listen=%s piece=%s buffer=%s workers=%d timeout=%s ttl=%s",
			cfg.Listen, formatSize(cfg.Piece), formatSize(cfg.Buffer),
			cfg.Workers, cfg.Timeout, cfg.TTL)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	go s.sessionGC()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down...")
	s.sessions.Range(func(k, v any) bool { v.(*session).stop(); return true })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	httpServer.Shutdown(ctx)
	log.Println("stopped")
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	count := 0
	s.sessions.Range(func(k, v any) bool { count++; return true })
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","sessions":%d,"goroutines":%d,"mem_mb":%.1f}`+"\n",
		count, runtime.NumGoroutine(), float64(m.Alloc)/(1<<20))
}

func (s *Server) streamHandler(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("url")
	if raw == "" {
		http.Error(w, "missing url parameter", http.StatusBadRequest)
		return
	}

	ssURL, err := url.QueryUnescape(raw)
	if err != nil {
		http.Error(w, "invalid url parameter", http.StatusBadRequest)
		return
	}

	directURL, err := s.resolveDirectURL(ssURL)
	if err != nil {
		http.Error(w, fmt.Sprintf("resolve direct URL: %v", err), http.StatusBadGateway)
		return
	}

	log.Printf("resolved: %s -> %s", ssURL, directURL)

	ss := s.getOrCreateSession(directURL)
	if err := ss.serveHTTP(w, r); err != nil {
		if err != context.Canceled {
			log.Printf("serve error %q: %v", directURL, err)
		}
	}
}

func (s *Server) resolveDirectURL(ssURL string) (string, error) {
	client := &http.Client{
		Timeout: s.cfg.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	resp, err := client.Get(ssURL)
	if err != nil {
		return "", fmt.Errorf("request SS: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusMovedPermanently ||
		resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect {
		loc := resp.Header.Get("Location")
		if loc == "" {
			return "", fmt.Errorf("SS returned %d without Location header", resp.StatusCode)
		}
		return loc, nil
	}

	if resp.StatusCode == http.StatusOK {
		ct := resp.Header.Get("Content-Type")
		if strings.HasPrefix(ct, "video/") || strings.HasPrefix(ct, "audio/") {
			return ssURL, nil
		}
	}

	return "", fmt.Errorf("unexpected SS response: %d", resp.StatusCode)
}

func parseRange(val string, size int64) (start, end int64, ok bool) {
	val = strings.TrimPrefix(val, "bytes=")
	a, b, found := strings.Cut(val, "-")
	if !found {
		return 0, 0, false
	}
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)

	if a == "" {
		suffix, err := strconv.ParseInt(b, 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false
		}
		start = size - suffix
		if start < 0 {
			start = 0
		}
		end = size
		return start, end, true
	}

	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil {
		return 0, 0, false
	}

	if b == "" {
		end = size
	} else {
		e, err := strconv.ParseInt(b, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		end = e + 1
	}

	if start >= size || start >= end {
		return 0, 0, false
	}
	if end > size {
		end = size
	}
	return start, end, true
}

func (s *Server) getOrCreateSession(directURL string) *session {
	if v, ok := s.sessions.Load(directURL); ok {
		ss := v.(*session)
		ss.mu.Lock()
		ss.lastAccess = time.Now()
		ss.mu.Unlock()
		ss.cond.Broadcast()
		return ss
	}

	ctx, cancel := context.WithCancel(context.Background())
	ss := &session{
		srv:         s,
		url:         directURL,
		pieceSize:   s.cfg.Piece,
		bufferSize:  s.cfg.Buffer,
		downloading: make(map[int64]bool),
		ctx:         ctx,
		cancel:      cancel,
		lastAccess:  time.Now(),
	}
	ss.cond = sync.NewCond(&ss.mu)

	v, loaded := s.sessions.LoadOrStore(directURL, ss)
	if loaded {
		cancel()
		return v.(*session)
	}

	go s.initSession(ss)
	return ss
}

func (s *Server) initSession(ss *session) {
	if err := ss.fetchFileInfo(); err != nil {
		ss.mu.Lock()
		ss.err = err
		ss.infoReady = true
		ss.mu.Unlock()
		ss.cond.Broadcast()
		log.Printf("fetch info failed %q: %v", ss.url, err)
		return
	}

	ss.mu.Lock()
	ss.infoReady = true
	ss.mu.Unlock()
	ss.cond.Broadcast()

	log.Printf("session start: size=%s type=%q url=%s",
		formatSize(ss.contentLength), ss.contentType, ss.url)

	s.launchWorkers(ss)
}

func (s *Server) launchWorkers(ss *session) {
	n := int(math.Ceil(float64(ss.bufferSize) / float64(ss.pieceSize)))
	if n < 2 {
		n = 2
	}
	if n > s.cfg.Workers {
		n = s.cfg.Workers
	}
	for i := 0; i < n; i++ {
		go ss.downloadLoop()
	}
}

func (ss *session) fetchFileInfo() error {
	client := &http.Client{Timeout: ss.srv.cfg.Timeout}
	req, err := http.NewRequestWithContext(ss.ctx, "GET", ss.url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", "bytes=0-0")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	ct := resp.Header.Get("Content-Type")
	size := resp.ContentLength

	if cr := resp.Header.Get("Content-Range"); cr != "" {
		parts := strings.Split(cr, "/")
		if len(parts) == 2 {
			if n, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil && n > 0 {
				size = n
			}
		}
	}

	if size <= 0 {
		return fmt.Errorf("cannot determine file size (status %d)", resp.StatusCode)
	}

	ss.mu.Lock()
	ss.contentLength = size
	ss.contentType = ct
	ss.mu.Unlock()
	return nil
}

func (ss *session) downloadLoop() {
	pieceSize := ss.pieceSize

	for {
		select {
		case <-ss.ctx.Done():
			return
		default:
		}

		off, ok := ss.nextPiece()
		if !ok {
			select {
			case <-ss.ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		cl := ss.contentLength
		end := off + pieceSize
		if cl > 0 && end > cl {
			end = cl
		}

		ss.srv.sem <- struct{}{}
		data, err := downloadPiece(ss.ctx, ss.url, off, end-1, ss.srv.cfg)
		<-ss.srv.sem

		ss.mu.Lock()
		if err != nil {
			if !strings.Contains(err.Error(), "context canceled") {
				ss.err = fmt.Errorf("chunk %d: %w", off, err)
				log.Printf("download chunk %d failed: %v", off, err)
			}
			delete(ss.downloading, off)
			ss.mu.Unlock()
			ss.cond.Broadcast()
			return
		}

		delete(ss.downloading, off)
		insertSorted(&ss.chunks, piece{offset: off, data: data})
		ss.mu.Unlock()
		ss.cond.Broadcast()
	}
}

func (ss *session) nextPiece() (int64, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	size := ss.contentLength
	if size <= 0 || ss.playHead >= size {
		return 0, false
	}

	windowEnd := ss.playHead + ss.bufferSize
	if windowEnd > size {
		windowEnd = size
	}

	buffered := int64(0)
	for _, p := range ss.chunks {
		buffered += int64(len(p.data))
	}
	for range ss.downloading {
		buffered += ss.pieceSize
	}
	if buffered >= ss.bufferSize {
		return 0, false
	}

	off := alignDown(ss.playHead, ss.pieceSize)
	for ; off < windowEnd; off += ss.pieceSize {
		if ss.downloading[off] {
			continue
		}
		if chunkIndex(ss.chunks, off) >= 0 {
			continue
		}
		ss.downloading[off] = true
		return off, true
	}
	return 0, false
}

func alignDown(off, align int64) int64 {
	return (off / align) * align
}

func insertSorted(chunks *[]piece, p piece) {
	i := sort.Search(len(*chunks), func(i int) bool { return (*chunks)[i].offset >= p.offset })
	*chunks = append(*chunks, piece{})
	copy((*chunks)[i+1:], (*chunks)[i:])
	(*chunks)[i] = p
}

func chunkIndex(chunks []piece, offset int64) int {
	i := sort.Search(len(chunks), func(i int) bool { return chunks[i].offset >= offset })
	if i < len(chunks) && chunks[i].offset == offset {
		return i
	}
	return -1
}

func downloadPiece(ctx context.Context, url string, start, end int64, cfg Config) ([]byte, error) {
	client := &http.Client{Timeout: cfg.Timeout}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return io.ReadAll(resp.Body)
}

func (ss *session) serveHTTP(w http.ResponseWriter, r *http.Request) error {
	ss.mu.Lock()
	for !ss.infoReady && ss.err == nil {
		ss.cond.Wait()
		if ss.ctx.Err() != nil {
			ss.mu.Unlock()
			return context.Canceled
		}
	}
	if ss.err != nil {
		err := ss.err
		ss.mu.Unlock()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return err
	}
	cl := ss.contentLength
	ct := ss.contentType
	ss.mu.Unlock()

	end := cl
	start := int64(0)
	if rh := r.Header.Get("Range"); rh != "" {
		if s, e, ok := parseRange(rh, cl); ok {
			start, end = s, e
		}
	}

	if cl > 0 {
		w.Header().Set("Accept-Ranges", "bytes")
		if ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		if start > 0 || end < cl {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, cl))
			w.Header().Set("Content-Length", strconv.FormatInt(end-start, 10))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.Header().Set("Content-Length", strconv.FormatInt(cl, 10))
			w.WriteHeader(http.StatusOK)
		}
	}

	return ss.streamData(w, r, start, end)
}

func (ss *session) streamData(w http.ResponseWriter, r *http.Request, start, end int64) error {
	offset := start

	for offset < end {
		if r.Context().Err() != nil {
			return r.Context().Err()
		}
		if ss.ctx.Err() != nil {
			return ss.ctx.Err()
		}

		ps := ss.pieceSize
		chunkOff := alignDown(offset, ps)

		ss.mu.Lock()

		if ss.err != nil {
			err := ss.err
			ss.mu.Unlock()
			return err
		}

		idx := chunkIndex(ss.chunks, chunkOff)
		if idx >= 0 {
			p := ss.chunks[idx]
			internalOff := offset - chunkOff
			toWrite := int64(len(p.data)) - internalOff
			if offset+toWrite > end {
				toWrite = end - offset
			}
			buf := p.data[internalOff : internalOff+toWrite]
			ss.mu.Unlock()

			n, err := w.Write(buf)
			if err != nil {
				return err
			}
			offset += int64(n)

			ss.mu.Lock()
			if offset > ss.playHead {
				ss.playHead = offset
			}
			ss.purgeBefore(offset)
			ss.mu.Unlock()
			ss.cond.Broadcast()

			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			continue
		}

		if offset > ss.playHead {
			ss.playHead = offset
		}
		ss.cond.Wait()
		ss.mu.Unlock()
	}

	ss.mu.Lock()
	ss.lastAccess = time.Now()
	ss.mu.Unlock()
	return nil
}

func (ss *session) purgeBefore(off int64) {
	n := 0
	for _, p := range ss.chunks {
		if p.offset+int64(len(p.data)) > off {
			break
		}
		n++
	}
	if n > 0 {
		ss.chunks = append([]piece(nil), ss.chunks[n:]...)
	}
}

func (ss *session) stop() {
	ss.cancel()
	ss.mu.Lock()
	ss.cond.Broadcast()
	ss.mu.Unlock()
}

func (s *Server) sessionGC() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		s.sessions.Range(func(k, v any) bool {
			ss := v.(*session)
			ss.mu.Lock()
			idle := now.Sub(ss.lastAccess)
			ss.mu.Unlock()
			if idle > s.cfg.TTL {
				s.sessions.Delete(k)
				ss.stop()
				log.Printf("session GC: %s", ss.url)
			}
			return true
		})
	}
}

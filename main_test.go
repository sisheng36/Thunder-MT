package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"0", 0},
		{"5", 5},
		{"15", 15},
		{"100", 100},       // P1 bug: 旧版返回 10
		{"1024", 1024},     // P1 bug: 旧版返回 102
		{"1K", 1024},
		{"10M", 10 * 1024 * 1024},
		{"1G", 1024 * 1024 * 1024},
		{"2g", 2 * 1024 * 1024 * 1024},
		{" 3M ", 3 * 1024 * 1024}, // 空格 + 大小写
		{"abc", 0}, // 非法 → 0
	}
	for _, c := range cases {
		got := parseSize(c.in)
		if got != c.want {
			t.Errorf("parseSize(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestSanitizeFilename(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"normal.mp4", "normal.mp4"},
		{"a\r\nb", "ab"},                    // CRLF 被剥掉, 结果合法不回退
		{"a\"b", "ab"},                      // 引号删除
		{"", "downloaded_file"},
		{".", "downloaded_file"},
		{"a/b", "downloaded_file"},          // 含斜杠 → 回退
		{"\r\n", "downloaded_file"},         // 纯 CRLF 剥光后空 → 回退
		{"\x01\x02x", "x"},                  // 控制字符删除
	}
	for _, c := range cases {
		got := sanitizeFilename(c.in)
		if got != c.want {
			t.Errorf("sanitizeFilename(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestNewServerFirstChunk(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"", 2 * 1024 * 1024},      // 默认 2M
		{"1M", 1024 * 1024},
		{"512K", 512 * 1024},
		{"3M", 3 * 1024 * 1024},
	}
	for _, c := range cases {
		s := newServer("10M", "1M", c.in, "40M", 3, nil)
		if s.firstChunk != c.want {
			t.Errorf("newServer firstChunk=%q -> %d, want %d", c.in, s.firstChunk, c.want)
		}
	}
	// trunk/split 不受影响
	s := newServer("120M", "3M", "2M", "40M", 3, nil)
	if s.trunk != 120*1024*1024 {
		t.Errorf("trunk 应为 120M, 实得 %d", s.trunk)
	}
	if s.split != 3*1024*1024 {
		t.Errorf("split 应为 3M, 实得 %d", s.split)
	}
}

func TestNewServerFirstTrunk(t *testing.T) {
	cases := []struct {
		name      string
		trunk     string
		firstTrunk string
		want      int64
	}{
		{"默认 40M", "120M", "", 40 * 1024 * 1024},
		{"自定义 30M", "120M", "30M", 30 * 1024 * 1024},
		{"v > trunk 时 cap 到 trunk", "120M", "200M", 120 * 1024 * 1024},
		{"v = trunk 时等于 trunk", "120M", "120M", 120 * 1024 * 1024},
		{"v < 1M 兜底默认", "120M", "100K", 40 * 1024 * 1024},
		{"v 非法兜底默认", "120M", "abc", 40 * 1024 * 1024},
	}
	for _, c := range cases {
		s := newServer(c.trunk, "1M", "2M", c.firstTrunk, 3, nil)
		if s.firstTrunk != c.want {
			t.Errorf("%s: firstTrunk=%q (trunk=%q) -> %d, want %d", c.name, c.firstTrunk, c.trunk, s.firstTrunk, c.want)
		}
	}
}

func TestRecordEndTTFB(t *testing.T) {
	// 重置 stats
	stats = &statsCollector{LogMax: 50, Date: time.Now().Format("2006-01-02"), Daily: []dailyRecord{}, Logs: []logEntry{}}

	// 场景1: 有首字节 → latency=TTFB, transferTime=总时长
	start := time.Now()
	wr := &responseWriter{start: start}
	time.Sleep(10 * time.Millisecond)
	wr.firstByteAt = time.Now() // 模拟首字节
	time.Sleep(20 * time.Millisecond) // 模拟后续传输
	stats.recordEnd(start, wr, "test", "bytes=0-", 1024, false, nil)

	snap := stats.snapshot()
	avgLat := snap.AvgLatency
	avgTr := snap.AvgTransferTime
	if avgLat < 5 || avgLat > 25 {
		t.Errorf("TTFB latency 应 ~10ms, 实得 %d", avgLat)
	}
	if avgTr < 25 || avgTr > 50 {
		t.Errorf("transferTime 应 ~30ms, 实得 %d", avgTr)
	}
	if avgLat >= avgTr {
		t.Errorf("TTFB(%d) 应 < transferTime(%d)", avgLat, avgTr)
	}

	// 场景2: 无首字节(0 bytes) → latency 兜底用总时长 = transferTime
	stats = &statsCollector{LogMax: 50, Date: time.Now().Format("2006-01-02"), Daily: []dailyRecord{}, Logs: []logEntry{}}
	start2 := time.Now()
	wr2 := &responseWriter{start: start2} // firstByteAt 零值
	time.Sleep(15 * time.Millisecond)
	stats.recordEnd(start2, wr2, "test", "", 0, false, fmt.Errorf("read error"))

	snap2 := stats.snapshot()
	avgLat2 := snap2.AvgLatency
	avgTr2 := snap2.AvgTransferTime
	if avgLat2 != avgTr2 {
		t.Errorf("无首字节时 latency(%d) 应 = transferTime(%d)", avgLat2, avgTr2)
	}
}

func TestLoadLegacyLatencyMigration(t *testing.T) {
	// 旧版本 stats.json: 有 totalLatency(实为传输时长), 无 totalTransferTime
	// 加载后应把 totalLatency 迁移到 TotalTransferTime, TotalLatency 重置为 0
	tmpDir := t.TempDir()
	path := tmpDir + "/stats.json"
	legacy := `{"date":"` + time.Now().Format("2006-01-02") + `","streams":10,"lavf":0,"errors":0,"totalBytes":1000,"cacheHits":5,"totalLatency":50000,"successRate":100,"daily":[],"logs":[],"hourly":[]}`
	os.WriteFile(path, []byte(legacy), 0644)

	stats = &statsCollector{LogMax: 50, Date: time.Now().Format("2006-01-02"), Daily: []dailyRecord{}, Logs: []logEntry{}}
	stats.load(path)

	if stats.TotalTransferTime != 50000 {
		t.Errorf("旧 totalLatency 50000 应迁移到 TotalTransferTime, 实得 %d", stats.TotalTransferTime)
	}
	if stats.TotalLatency != 0 {
		t.Errorf("迁移后 TotalLatency 应重置为 0(TTFB 无历史数据), 实得 %d", stats.TotalLatency)
	}
	if stats.TotalStreams != 10 {
		t.Errorf("streams 应恢复 10, 实得 %d", stats.TotalStreams)
	}
}

func TestIsURLAllowed(t *testing.T) {
	// 无白名单时：仅协议+host 校验
	allowedHosts = nil
	bad := []string{
		"file:///etc/passwd",
		"ftp://x/y",
		"data:text/plain,x",
		"javascript:alert(1)",
		"http://",  // 空 host (实际 url.Parse 会给 host="")  — 校验逻辑: u.Host=="" → 拒
	}
	for _, u := range bad {
		if err := isURLAllowed(u); err == nil {
			t.Errorf("isURLAllowed(%q) 应被拒绝，却放行", u)
		}
	}
	good := []string{
		"http://example.com/x.mp4",
		"https://127.0.0.1:8099/health",
		"https://api.foo.com/a/b/c",
	}
	for _, u := range good {
		if err := isURLAllowed(u); err != nil {
			t.Errorf("isURLAllowed(%q) 应放行，却报错: %v", u, err)
		}
	}
	// 白名单启用
	allowedHosts = map[string]bool{"example.com": true, "allow.com": true}
	if err := isURLAllowed("https://example.com/x"); err != nil {
		t.Errorf("白名单内 example.com 应放行: %v", err)
	}
	if err := isURLAllowed("https://evil.com/x"); err == nil {
		t.Errorf("白名单外 evil.com 应被拒")
	}
	allowedHosts = nil
}

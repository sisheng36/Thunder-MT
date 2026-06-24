package main

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// filenameStarRe / filenamePlainRe 解析 Content-Disposition, extractFileName 使用.
// rangeRe 解析请求 Range 头, handleStream 使用.
var (
	rangeRe          = regexp.MustCompile(`bytes=(\d+)-(\d*)`)
	filenameStarRe   = regexp.MustCompile(`filename\*\s*=\s*UTF-8''(.+)`)
	filenamePlainRe  = regexp.MustCompile(`filename\s*=\s*"?([^";]+)"?`)
)

var allowedHosts map[string]bool

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

// parseSize 解析 "10M"/"1G"/"512K"/"1024" 形式的字节大小字符串.
// 仅支持非负整数 + 单一后缀(K/M/G, 大小写不敏感), 不支持小数和组合后缀("1.5M"/"1KB" 返回 0).
// 非法/空输入返回 0, 调用方负责用 normalize* 系列函数兜底.
// (#7 文档化: 旧版 "100" 因 bug 返回 10, v1.0.7 已修, 此处明确契约)
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

// normalizeTrunk 保证 trunk 在安全下限之上, 防 chunkSize=0 死循环/除零 (#1#2)
// 1MB 是合理下限: 小于此值单次 Range 几乎无意义, 且可能触发死循环
func normalizeTrunk(v int64) int64 {
	if v < 1024*1024 {
		return defaultTrunk
	}
	return v
}

// normalizeSplit 保证 split ≥1MB, 防 for 死循环/除零 (#1#2)
func normalizeSplit(v int64) int64 {
	if v < 1024*1024 {
		return defaultSplit
	}
	return v
}

// normalizeFirstChunk 保证起播首块 ≥1MB
func normalizeFirstChunk(v int64) int64 {
	if v < 1024*1024 {
		return defaultFirstChunk
	}
	return v
}

// normalizeConns 保证 conns ≥1, 防无缓冲 channel 死锁 (#1#2)
// conns=0 会让 sem := make(chan struct{}, 0) 变成无缓冲, 所有下载 goroutine 永久阻塞
func normalizeConns(v int) int {
	if v < 1 {
		return defaultConns
	}
	return v
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

// decodeOrRaw 优先 url.QueryUnescape, 失败回退原始字符串 (#10: 消除 4 处重复 decode 样板)
func decodeOrRaw(s string) string {
	if decoded, err := url.QueryUnescape(s); err == nil {
		return decoded
	}
	return s
}

func extractFileName(rawURL, contentDisposition string) string {
	if contentDisposition != "" {
		if m := filenameStarRe.FindStringSubmatch(contentDisposition); m != nil {
			return sanitizeFilename(decodeOrRaw(m[1]))
		}
		if m := filenamePlainRe.FindStringSubmatch(contentDisposition); m != nil {
			return sanitizeFilename(decodeOrRaw(m[1]))
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
			return sanitizeFilename(decodeOrRaw(name))
		}
	}
	return "downloaded_file"
}

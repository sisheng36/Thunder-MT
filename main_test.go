package main

import "testing"

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

package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

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
	LogMax: logMaxEntries,
	Date:   time.Now().Format("2006-01-02"),
	Daily:  make([]dailyRecord, 0),
	Logs:   make([]logEntry, 0, logMaxEntries),
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
	if len(s.Daily) > dailyHistoryCap {
		s.Daily = s.Daily[:dailyHistoryCap]
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
	// #6 跨天归档时清空 Logs, 保证与今日 stats 自洽
	// (旧逻辑保留昨日 Logs → dashboard 今日表格里混入昨日记录)
	s.Logs = s.Logs[:0]
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
	data, err := json.MarshalIndent(s.snapshot(), "", "  ")
	if err != nil {
		log.Printf("stats 序列化失败: %v", err)
		return
	}
	if err := os.MkdirAll(statsDir, 0755); err != nil {
		log.Printf("创建 stats 目录失败: %v", err)
		return
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		log.Printf("stats 落盘失败: %v", err)
	}
}

func (s *statsCollector) load(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		// 文件不存在(首次启动)属正常, 静默
		return
	}
	var snap statsSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		// #17: 文件存在但解析失败 → 数据损坏, 记录便于排查
		log.Printf("stats 文件解析失败(将忽略历史): %v", err)
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

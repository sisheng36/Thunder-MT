package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
	client := &http.Client{Timeout: probeClientTimeout}

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

func resolveDirectURL(backendURL string, headers map[string]string) (string, error) {
	client := &http.Client{
		Timeout: resolveClientTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
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

	// #13 校验 status code: 4xx/5xx 不算成功解析, 避免误导后续 newURLProxy 浪费往返
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("解析直链失败: 服务器返回 %d", resp.StatusCode)
	}

	return resp.Request.URL.String(), nil
}

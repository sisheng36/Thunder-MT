package main

import (
	"log"
	"sync"
	"time"
)

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
	ticker := time.NewTicker(cleanupInterval)
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
	// inflight 无论成败都删除: 失败时 items 不写入, 下次 getOrCreate 会重新走 create 重试.
	// 只有成功的 proxy 才进 items 并受 TTL 约束.
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

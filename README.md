# Thunder-MT
此项目仅用于交流和学习。
此项目仅用于交流和学习。
此项目仅用于交流和学习。
多分片并行下载代理,带统计仪表盘。专为 emby/Infuse/mpv 等媒体播放器加速大文件起播。

## 流程图

### 启动流程

```mermaid
flowchart TD
    A[main 启动] --> B["读 ENV 配置<br/>TRUNK SPLIT FIRST_CHUNK CONNS<br/>HOST PORT HEADERS<br/>ALLOW_HOSTS DASHBOARD_TOKEN"]
    B --> C["initAllowedHosts<br/>解析 ALLOW_HOSTS 白名单"]
    C --> D["newServer<br/>parseSize 各参数<br/>firstChunk<=0 兜底 2M<br/>newProxyCache 5min TTL"]
    D --> E["os.MkdirAll /data"]
    E --> F["stats.load /data/stats.json<br/>Unmarshal statsSnapshot<br/>旧数据迁移: totalLatency→TotalTransferTime<br/>跨天只恢复 Daily 历史"]
    F --> G["启动持久化 goroutine<br/>ticker 30s → stats.save"]
    G --> H["注册路由<br/>/health /api/stats /stream /"]
    H --> I["启动信号监听 goroutine<br/>SIGINT/SIGTERM → save → Shutdown 30s"]
    I --> J["httpSrv.ListenAndServe<br/>ReadHeaderTimeout=10s<br/>IdleTimeout=120s"]
    J --> K{err}
    K -->|ErrServerClosed| L[已停止]
    K -->|其他错误| M["log.Fatalf 启动失败"]
```

### 请求路由与安全校验

```mermaid
flowchart TD
    REQ[HTTP 请求进入] --> MUX{"路由匹配 mux"}
    MUX -->|/health| H1[200 ok]
    MUX -->|/api/stats| H2{"checkAuth?"}
    MUX -->|/stream| H3{"方法校验<br/>GET/HEAD?"}
    MUX -->|/| H4{"checkAuth?"}

    H2 -->|否| A401[401 Unauthorized]
    H2 -->|是| H2a[200 JSON snapshot]

    H4 -->|否| A401
    H4 -->|是| H4a[200 dashboard HTML]

    H3 -->|否| A405[405 Method Not Allowed]
    H3 -->|是| S1[atomic +1 ActiveStreams<br/>start = now<br/>wr = responseWriter]

    S1 --> S2{"url 参数为空?"}
    S2 -->|是| A400[400 Missing url]
    S2 -->|否| S3{"isURLAllowed?<br/>协议 http/https<br/>ALLOW_HOSTS 白名单"}
    S3 -->|否| A403[403 URL not allowed]
    S3 -->|是| S4{"UA 含 Lavf?"}
    S4 -->|是| S4a[302 直跳 url<br/>recordEnd isLavf=true]
    S4 -->|否| S5[cache.getOrCreate<br/>命中: touchAndHit<br/>inflight: 等待<br/>新建: resolveDirectURL + newURLProxy]
    S5 -->|失败| A502[502 无法解析后端<br/>recordEnd]
    S5 -->|成功| S6[设置响应头<br/>Content-Type<br/>Content-Disposition RFC6266<br/>Accept-Ranges]

    S6 --> R{"Range 头判断"}
    R -->|无 Range| R1["firstChunk=2M<br/>首 chunk 0→2M<br/>streamSortedAndRecord"]
    R -->|Range B<br/>bytes=N-M| R2["解析 begin/end<br/>cap end≤begin+trunk<br/>streamSortedAndRecord"]
    R -->|Range U<br/>bytes=N-| R3["解析 begin<br/>endStr为空<br/>continuousStream 循环 trunk"]
    R -->|无效格式| A416[416 Range Not Satisfiable]
    R -->|begin≥size| A416

    R1 --> END[recordEnd<br/>latency=TTFB<br/>transferTime=总时长]
    R2 --> END
    R3 --> END
```

### 流式传输核心 sortedStream

```mermaid
flowchart TD
    subgraph Writer[Writer Goroutine 单线程有序写]
        W1["chunks map int64 byte<br/>nextPos = begin"] --> W2{"received < totalChunks?"}
        W2 -->|是| W3["<-chunkCh"]
        W3 --> W4["chunks start = data"]
        W4 --> W5{"nextPos in chunks?"}
        W5 -->|是| W6[w.Write chunks nextPos<br/>→ HTTP Response]
        W6 --> W7[nextPos += len data]
        W7 --> W5
        W5 -->|否| W2
        W2 -->|否| WD[close writerDone]
    end

    subgraph Downloaders["Downloader Goroutines conns=60 并发"]
        D1["for pos := begin; pos<=end; pos+=split"] --> D2["go func start, chunkEnd"]
        D2 --> D3["sem 结构体 信号量"]
        D3 --> D4["downloadChunk ctx, start, chunkEnd"]
        D4 --> D5["HTTP GET url Range: S-E<br/>setHeaders<br/>503 重试 1 次<br/>ReadFull 短读=错误重试"]
        D5 --> D6["chunkCh <- chunkData start, data"]
        D6 --> D7["<-sem"]
        D7 --> D8{"pos <= end?"}
        D8 -->|是| D2
        D8 -->|否| DD[wg.Wait]
    end

    DD --> CL[close chunkCh]
    CL --> W2
    WD --> ERR{"errCh 有错误?"}
    ERR -->|是| RET[返回错误]
    ERR -->|否| OK[返回 nil]

    D4 -.->|失败| CAN[cancel ctx]
    W6 -.->|Write 失败| CAN
    CAN --> STP[全体停止]
```

### 缓存与统计闭环

```mermaid
flowchart LR
    subgraph Cache[proxyCache 缓存层]
        C1[getOrCreate key, create] --> C2{"get key 命中?"}
        C2 -->|是| C3[touchAndHit 返回]
        C2 -->|否| C4[Lock]
        C4 --> C5{"items key 存在?"}
        C5 -->|是| C3
        C5 -->|否| C6{"inflight key 存在?"}
        C6 -->|是| C7[wg.Wait 等待]
        C6 -->|否| C8[创建 call inflight key]
        C8 --> C9["Unlock + create<br/>resolveDirectURL + newURLProxy"]
        C9 --> C10["Lock + delete inflight<br/>items key = proxy"]
        C10 --> C11[wg.Done]
        C7 --> C3
    end

    CL2["cleanupLoop<br/>30s ticker"] --> CL3["删除 lastAccess > 5min"]
```

```mermaid
flowchart TD
    RS[recordStart → start time] --> ST[流式传输]
    ST --> RE[recordEnd start, wr]
    RE --> RE1["latency = TTFB<br/>wr.firstByteAt - start"]
    RE --> RE2["transferTime = now - start"]
    RE1 --> RE3[TotalStreams++]
    RE2 --> RE4["TotalLatency += latency<br/>TotalTransferTime += transferTime"]
    RE3 --> RE5["Hourly h.Streams++"]
    RE4 --> RE5
    RE5 --> RE6{"isLavf?"}
    RE6 -->|是| RE7[TotalLavf++ return]
    RE6 -->|否| RE8{"isFatal?"}
    RE8 -->|是| RE9[TotalErrors++]
    RE8 -->|否| RE10[TotalSuccess++]
    RE9 --> RE11["TotalBytes += bytes<br/>Logs 插入 logEntry"]
    RE10 --> RE11

    RE11 --> SNAP["snapshot → /api/stats → Dashboard"]
    RE11 --> SAVE["save 30s → /data/stats.json<br/>重启 load 恢复"]
    RE11 --> CD["checkDate 跨天<br/>Hourly → Daily<br/>重置当天计数"]
```

### 用户视角数据流

```mermaid
sequenceDiagram
    participant C as emby/Infuse/mpv
    participant T as Thunder-MT
    participant S as 源站 127.0.0.1:8024

    C->>T: GET /stream?url=http://127.0.0.1:8024/x.mp4
    T->>T: isURLAllowed 校验
    T->>T: cache.getOrCreate
    T->>S: GET url Range: 0-0 (resolveDirectURL 跟随重定向)
    S-->>T: 返回直链
    T->>S: GET 直链 Range: 0-0 (newURLProxy 探测 size)
    S-->>T: Content-Range size=1.35GB

    Note over T: 无 Range 分支: 首 chunk 起播加速
    T-->>C: 206 Partial Content Content-Range: 0-2M
    T->>S: GET Range: 0-1M (并行下载 60 并发)
    T->>S: GET Range: 1M-2M
    S-->>T: chunks 乱序到达
    Note over T: sortedStream map 缓存有序写出
    T-->>C: 首字节到达 TTFB ~200ms
    T-->>C: 2MB 数据

    C->>T: GET Range: bytes=0- (正式播放)
    Note over T: continuousStream 循环 trunk 窗口
    T-->>C: 206 + 持续流
    T->>S: GET Range: 2M-122M
    T->>S: GET Range: 122M-242M

    C->>T: GET Range: bytes=60M- (seek 拖动)
    Note over T: sortedStream 60M 窗口
    T-->>C: 206 + 60M 起数据

    C->>T: GET /stream?url=... UA: Lavf/ffprobe
    Note over T: Lavf 302 直跳 不代理
    T-->>C: 302 → url
```

## 配置项

| env | 作用 | 默认 |
|---|---|---|
| `TRUNK` | 单次 Range 返回量/缓冲窗口 | 10M |
| `SPLIT` | 并行下载分块大小 | 1M |
| `FIRST_CHUNK` | 无 Range 请求首块大小(起播) | 2M |
| `CONNS` | 并行 goroutine 数 | 60 |
| `HOST` | 监听地址 | 0.0.0.0 |
| `PORT` | 监听端口 | 8010 |
| `HEADERS` | 请求直链时附加的 HTTP 头(JSON) | {} |
| `ALLOW_HOSTS` | 目标 host 白名单(逗号分隔) | 空=不限 |
| `DASHBOARD_TOKEN` | 仪表盘+API 鉴权 | 空=开放 |

## 部署

```bash
docker compose up -d
```

仪表盘: http://localhost:8010/

## 版本历史

- v1.1.0 — 代码精简重构(零行为变更)
- v1.0.9 — latency 改 TTFB + transferTime
- v1.0.7 — 安全加固 + FIRST_CHUNK 拆分

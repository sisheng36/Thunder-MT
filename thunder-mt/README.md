# Thunder-MT

迅雷网盘多线程流引擎，配合 SmartStrm STRM 内容替换插件使用。

## 工作方式

```
SmartStrm 内容替换插件
  将 STRM 里: http://127.0.0.1:8024/smartstrm_fid/迅雷/...
  替换为:     http://127.0.0.1:8010/stream?url=http://127.0.0.1:8024/smartstrm_fid/迅雷/...

Thunder-MT (:8010)
  收到 /stream 请求 → GET SS 地址拿 302 直链 → N 路并发下载 → 返回播放器
```

## 快速开始

```bash
go build -o thunder-mt .
./thunder-mt --listen :8010 --piece 1M --buffer 50M --workers 10
```

## 配置

| 参数 | 默认 | 说明 |
|---|---|---|
| `--listen` | `:8010` | 监听地址（与 STRM 替换后的地址一致） |
| `--piece` | `1M` | 分片大小 |
| `--buffer` | `50M` | 最大缓冲（每会话） |
| `--workers` | `10` | 最大并发连接 |
| `--timeout` | `30s` | 分片下载超时 |
| `--session-ttl` | `120s` | 空闲会话清理间隔 |

## 健康检查

```bash
curl http://localhost:8010/health
# {"status":"ok","sessions":1,"goroutines":15,"mem_mb":8.2}
```

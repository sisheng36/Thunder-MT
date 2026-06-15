# Thunder-MT

迅雷云盘多线程流加速引擎（Forked from [qy527145/url_proxy](https://github.com/qy527145/url_proxy)）。

配合 SmartStrm STRM 内容替换插件使用。

## 工作方式

```
STRM 文件 → http://127.0.0.1:8010/stream?url=http://SS:8024/smartstrm_fid/迅雷/...
              ↓
         Thunder-MT
           ├── resolve_direct_url: GET SS → 302 → 迅雷直链
           ├── 懒初始化：首分片提取 Content-Range 获取文件大小
           ├── ThreadPoolExecutor N 路并发分片下载
           └── StreamingResponse 返回播放器
```

## 配置

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `LISTEN_HOST` | `0.0.0.0` | 监听地址 |
| `LISTEN_PORT` | `8010` | 监听端口 |
| `TRUNK` | `32M` | 块大小 |
| `SPLIT` | `3M` | 分片大小 |
| `CONNS` | `4` | 并发线程数 |
| `TIMEOUT` | `30` | 下载超时（秒） |
| `SESSION_TTL` | `120` | 空闲会话清理间隔（秒） |

## Docker Compose

```bash
docker compose up -d
```

## SmartStrm 集成

SS 后台 → 插件管理 → STRM 内容替换：

| 查找 | 替换 |
|---|---|
| `http://127.0.0.1:8024/smartstrm_fid/迅雷/` | `http://127.0.0.1:8010/stream?url=http://127.0.0.1:8024/smartstrm_fid/迅雷/` |

## 健康检查

```bash
curl http://localhost:8010/health
# {"status":"ok","sessions":2}
```

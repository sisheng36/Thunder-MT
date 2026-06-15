# Thunder-MT —— SmartStrm 迅雷多线程加速方案

## 背景

SmartStrm 的 STRM 内容替换插件 + 外部多线程 stream 引擎，零侵入实现迅雷驱动的多线程加速。

## 方案

**SS 侧（零代码改动）：** 使用 STRM 内容替换插件，将迅雷驱动的链接前缀替换为 Thunder-MT 的 stream 地址。

| 查找 | 替换 |
|---|---|
| `http://127.0.0.1:8024/smartstrm_fid/迅雷/` | `http://127.0.0.1:8010/stream?url=http://127.0.0.1:8024/smartstrm_fid/迅雷/` |

**播放链路：**

```
播放器 → Emby → 读 STRM → GET :8010/stream?url=SS:8024/smartstrm_fid/迅雷/...
                              │
                              ↓
                         Thunder-MT
                           │ 1. GET SS 地址 → 读 302 → 拿到迅雷直链
                           │ 2. GET bytes=0-0 → 文件大小
                           │ 3. N 路并发分片下载
                           │ 4. 单路返回播放器
                           ↓
                        播放器  ←  视频数据
```

## 部署

```bash
./thunder-mt --listen :8010 --piece 1M --buffer 50M --workers 10
```

## 配置

| 参数 | 默认 | 说明 |
|---|---|---|
| `--listen` | `:8010` | 监听地址 |
| `--piece` | `1M` | 分片大小 |
| `--buffer` | `50M` | 最大缓冲（每会话） |
| `--workers` | `10` | 最大并发连接 |
| `--timeout` | `30s` | 分片下载超时 |
| `--session-ttl` | `120s` | 空闲会话清理间隔 |

## 限制

- 仅用于局域网场景（家宽上行瓶颈不在此）
- 云盘需支持 HTTP Range 请求
- 直链内 token 过期会导致下载中断

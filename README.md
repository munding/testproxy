# testproxy

`testproxy` 是一个用 Go 编写的命令行工具，用来测试 HTTP CONNECT 和 SOCKS5 代理的连接能力。

它主要测试两类行为：

- 代理协议握手成功后，代理到目标站点的 TCP 连接在空闲状态下能保持多久
- 代理最多能同时建立多少条到目标站点的 TCP 连接，直到代理返回拒绝或连接建立失败

## 构建

```bash
go build ./cmd/testproxy
```

构建后会在当前目录生成 `testproxy` 二进制文件。

## 代理 URL

使用 `-x` 指定代理服务器 URL：

```text
http://[user:pass@]host:port
socks5://[user:pass@]host:port
```

使用 `-t` 指定代理需要连接的目标地址：

```text
host:port
```

## 空闲连接保持时间测试

```bash
./testproxy idle -x socks5://127.0.0.1:1080 -t example.com:443 --timeout 30m
```

常用参数：

- `--timeout`：最长等待时间，默认 `1h`
- `--connect-timeout`：建立代理连接的超时时间，默认 `10s`
- `--probe-interval`：读超时检测间隔，默认 `1s`

`idle` 会先通过代理和目标地址完成连接建立。建立成功后，程序不会再发送任何应用数据，只等待 `Read` 返回错误。

测试过程中会在 `stderr` 刷新显示当前已等待时间，例如 `idle_elapsed=1m20s`。最终测试结果仍输出到 `stdout`。

如果连接被对端 FIN/RST 关闭，程序会输出大致空闲保持时间。如果一直没有关闭，则在 `--timeout` 到达后退出。

## 最大连接数测试

```bash
./testproxy maxconn -x http://user:pass@127.0.0.1:8080 -t example.com:443 --rate 5
```

常用参数：

- `--rate`：每秒发起多少个代理通道建立请求，默认 `1`
- `--limit`：最大尝试建立连接次数，默认 `500`；设置为 `0` 表示不限制，会一直运行到手动中断
- `--connect-timeout`：每条代理通道建立的超时时间，默认 `10s`
- `--conn-idle-timeout`：成功建立后的连接如果多久没有读到数据就主动关闭，默认 `0`，表示不主动关闭

`maxconn` 会按 `--rate` 指定的速率持续发起代理连接建立请求。每条成功建立的连接都会保持打开，并启动后台 `Read` 监控；如果连接被 FIN/RST 关闭，会从存活连接数里剔除。

如果设置了 `--conn-idle-timeout`，后台 `Read` 在这段时间内没有读到任何数据也没有收到关闭事件时，会主动关闭该连接，并从存活连接数里剔除。

测试过程中每秒会打印一次 `[status]` 行，显示已经发送了多少次请求、当前正常存活连接数、历史最大存活连接数、成功数和失败数。

达到 `--limit` 后，程序会等待所有已发送的建连请求都有结果，包括等待 `--connect-timeout` 后产生的失败，再打印最终完整统计。

`max_live_connections` 是测试过程中观察到的最大存活连接数，可以用来估算代理最大可保持连接数。

测试会在以下情况结束：

- 达到 `--limit`，仅当 `--limit > 0`
- 手动中断，例如按 `Ctrl+C`

以下情况会被计入失败：

- SOCKS5 返回非成功 reply code
- HTTP CONNECT 返回非 2xx 状态码
- TCP 连接、代理握手、认证或超时等错误

## 输出示例

运行过程中每秒输出一行状态。

```text
[status] elapsed=10s attempts=50 live_connections=48 max_live_connections=48 successful=48 failed=0
[status] elapsed=20s attempts=100 live_connections=93 max_live_connections=93 successful=95 failed=5
----------------------------------------
attempts=100
successful=95
failed=5
live_connections=93
max_live_connections=93
reached_limit=false
error_summary:
  socks5_0x05=5
```

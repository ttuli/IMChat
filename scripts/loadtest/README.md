# 压测脚本（loadtest）

三个相互独立、可单独运行的压测工具，分别覆盖：

| 脚本目录 | 压测目标 | 关键指标 |
| --- | --- | --- |
| [`http_bench/`](http_bench/main.go) | 任意 HTTP(S) 接口 | QPS、成功率、状态码分布、延迟 P50/P90/P95/P99 |
| [`ws_conn/`](ws_conn/main.go) | 网关 WebSocket **连接数** | 建连成功/失败、在线峰值、握手延迟、掉线原因 |
| [`ws_msg/`](ws_msg/main.go) | 网关 WebSocket **消息并发** | 发送吞吐、ACK 成功率、ACK 往返时延、端到端投递数 |

均为纯 Go 标准库 + 项目已有依赖（`gorilla/websocket`、`google.golang.org/protobuf`）实现，
`ws_msg` 直接复用项目自身的 protobuf 类型（`IM2/pkg/proto/...`），保证报文与线上完全一致。

---

## 前置说明：账号与鉴权

WS 两个脚本需要**登录态**。网关建连要经过 `WithJwtAuth` + `WithWsSessionAuth` 两层中间件，
token 必须携带非空 `device_id` 且 `ver` 与 Redis 会话一致。

> ⚠️ **每条并发 WS 连接必须使用不同账号。**
> 网关对同一 `userID` 的新连接会踢掉旧连接，且每次登录都会递增会话 `version` 使旧 token 失效。

两个脚本获取登录态的方式不同：

- **`ws_conn`**：直连 Redis 自动生成虚拟账号的 Token（无需注册账号），见下文配置说明。
- **`ws_msg`**：走真实登录接口，需要账号文件 `accounts.csv`（`#` 开头为注释，`device_id` 可省略）：

```
# 账号(数字用户ID),密码[,设备ID]
10001,123456
10002,123456,dev-10002
10003,123456
```

如果你已有现成的 AccessToken，`ws_msg` 可用 `-tokens tokens.txt`（每行一个）跳过登录。

---

## 1. HTTP 接口压测 · `http_bench`

通用 HTTP 压测器，两种模式二选一：`-n` 固定请求数，或 `-d` 固定时长（默认 10s）。

```bash
# 压测登录接口 30 秒，200 并发
go run ./scripts/loadtest/http_bench \
    -url http://127.0.0.1:8888/auth/login -method POST \
    -body '{"account":10001,"password":"123456","device_id":"bench","remeber_me":false}' \
    -c 200 -d 30s

# 压测鉴权接口，固定 5 万次请求
go run ./scripts/loadtest/http_bench \
    -url 'http://127.0.0.1:8889/user/info?id=10001' \
    -token "$AT" -c 100 -n 50000

# 请求体从文件读取
go run ./scripts/loadtest/http_bench -url http://127.0.0.1:8888/auth/login \
    -method POST -body @login.json -c 100 -d 20s
```

常用参数：`-c` 并发数 | `-n` 总请求数 | `-d` 时长 | `-timeout` 单请求超时 |
`-H "K: V"` 自定义头（可重复） | `-token` Bearer 令牌 | `-keepalive=false` 关闭长连接复用 |
`-insecure` 跳过 TLS 校验 | `-think` 每 worker 请求间隔。

---

## 2. WS 连接数压测 · `ws_conn`

**配置驱动**（[config.yaml](ws_conn/config.yaml)），不走登录接口：直接用 `TokenManager.OnLogin`
批量生成虚拟账号的 Token 并把 session 写入 Redis（需 `.env` 提供
`REDIS_TOKEN_HOST` / `REDIS_TOKEN_PASS` / `JWT_SECRET`），因此**无需预注册账号**。
流程：生成 Token → 按 `rate` 爬升建连 → 保持 `hold` 时长 → 自动清理 Redis 测试数据 → 汇总。

```bash
# 方式一：进入脚本目录运行（读取当前目录 config.yaml）
cd scripts/loadtest/ws_conn && go run .

# 方式二：在项目根目录运行，用 -config 指定配置
go run ./scripts/loadtest/ws_conn -config scripts/loadtest/ws_conn/config.yaml
```

配置项（见 config.yaml 内注释）：`host`/`path`/`tls`/`insecure` 网关地址 |
`num` 目标连接数 | `start_user_id` 起始虚拟用户ID | `rate` 建连速率（条/秒） |
`hold` 保持时长 | `dial_timeout` 握手超时 | `jwt_expire` Token 有效期（秒）。

**指标说明：** 报告区分「建连失败」（握手阶段）与「中途掉线」（建连成功后断开，
其中被服务端踢下线的会单独列出）；Ctrl-C 中断时在途拨号计为「中断取消」，不算失败。
`start_user_id` 请避开真实用户的 ID 区间，防止踢掉真实用户的会话。

---

## 3. WS 消息并发压测 · `ws_msg`

建立 N 条连接，每条按 `-rate` 并发发送 `CHAT_TEXT`，读循环按 `ClientId` 匹配 `MSG_ACK` 算往返时延。

```bash
# 200 连接，每条 50 msg/s，持续 30s，两两互发
go run ./scripts/loadtest/ws_msg \
    -host 127.0.0.1:8888 -login-url http://127.0.0.1:8888 \
    -accounts accounts.csv -n 200 -rate 50 -d 30s

# 所有连接都发给固定用户 10001，每条各发 1000 条后停止
go run ./scripts/loadtest/ws_msg -host 127.0.0.1:8888 -login-url http://127.0.0.1:8888 \
    -accounts accounts.csv -n 100 -target 10001 -count 1000

# 加大单条消息体到 1KB
go run ./scripts/loadtest/ws_msg -host 127.0.0.1:8888 -login-url http://127.0.0.1:8888 \
    -accounts accounts.csv -n 100 -rate 100 -payload-size 1024 -d 30s
```

常用参数：`-n` 连接数 | `-rate` 每连接发送速率（0=不限速） | `-count` 每连接总条数 |
`-d` 时长 | `-target` 固定目标（0=两两互发，需 `-accounts`） | `-content`/`-payload-size` 文本内容/大小。

**指标解读：**
- **ACK**：网关把消息发布到 NATS 后立即回发的确认，衡量「网关摄入 + MQ 发布」时延，与下游 Message 服务是否消费无关。
- **未收到 ACK**：通常是网关侧背压（连接发送缓冲区满导致 ACK 被丢弃），是重要的过载信号。
- **收到消息**：需要 Message 服务消费 NATS 并回投才有值，衡量端到端投递；只启网关时为 0 属正常。

---

## 提示

- 大规模压测前先调高本机文件描述符上限（`ulimit -n`；出现 `too many open files` 即为此限制）。
- 单机客户端可能先于服务端到达瓶颈（CPU/端口/带宽），必要时多机分布式发压。
- 登录、建连、发送三个阶段的并发分别由 `-login-c` / `-rate` 控制，避免瞬时洪峰打满客户端。
- 所有脚本支持 `Ctrl-C` 优雅结束并打印已采集到的结果。

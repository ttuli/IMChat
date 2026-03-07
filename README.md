## IM2 即时通讯服务

IM2 是一个基于 Go 的分布式即时通讯（IM）后端，包含账号体系、好友与群组管理、消息存储与推送、文件上传等能力，整体采用多服务拆分 + WebSocket 网关的架构。

### 技术栈

- **语言/运行时**: Go (见 `go.mod`)
- **Web & RPC 框架**: go-zero（REST + gRPC）
- **存储**:
  - MySQL + GORM —— 账号、好友、群组、会话等关系数据
  - MongoDB —— 消息正文存储（按会话+Seq 查询）
  - Redis —— 会话序号、在线状态、时间线等缓存
- **消息总线**: NATS JetStream（跨节点消息转发）
- **配置与工具**: Viper、Nacos、Zap 日志等

### 主要服务与进程

项目使用 `cmd/<Service>/<api|rpc>/main.go` 作为各服务入口，典型服务包括：

- **Auth**
  - `cmd/Auth/api`：认证相关 HTTP API（登录、注册、刷新/注销 Token）
  - `cmd/Auth/rpc`：认证 RPC 服务（供内部调用）
- **User**
  - `cmd/User/api`：用户信息、好友相关 API
  - `cmd/User/rpc`：用户/好友 RPC 服务
- **Group**
  - `cmd/Group/api`：群组、成员、群申请相关 API
  - `cmd/Group/rpc`：群组 RPC 服务
- **Message**
  - `cmd/Message/api`：会话列表、历史消息等 HTTP API
  - `cmd/Message/rpc`：消息、会话相关 RPC 服务（含 MongoDB 消息存储）
- **File**
  - `cmd/File/api`：文件上传回调与上传签名获取等 API
- **WebSocket Gateway**
  - `cmd/websocket/gateway`：长连接网关，负责 WebSocket 连接管理、消息下发、会话更新推送等

各服务的业务实现代码一般位于：

- API 层：`internal/apps/<Service>/api`
- RPC 层：`internal/apps/<Service>/rpc`
- 公共模型与组件：`internal/model`、`pkg/*`

### 本地快速启动（示意）

> 实际启动前，请根据你部署环境准备好 MySQL、Redis、MongoDB、NATS 等依赖，并在配置文件中填好连接信息。

1. **准备配置**
   - 配置通常通过 go-zero 的 `RestConf` / `RpcClientConf` 加载，默认路径可在每个 `cmd/.../main.go` 中的 `configparser.DefaultConfigPath("<Service>/api")` 或类似调用中找到。
   - 根据样例配置（如果有）填写数据库、Redis、Mongo、NATS、APISIX 等连接信息。

2. **启动核心 RPC 服务**
   - 示例（具体路径以实际为准）：
     - `go run ./cmd/Auth/rpc`
     - `go run ./cmd/User/rpc`
     - `go run ./cmd/Group/rpc`
     - `go run ./cmd/Message/rpc`

3. **启动 API 服务**
   - `go run ./cmd/Auth/api`
   - `go run ./cmd/User/api`
   - `go run ./cmd/Group/api`
   - `go run ./cmd/Message/api`
   - `go run ./cmd/File/api`

4. **启动 WebSocket 网关**
   - `go run ./cmd/websocket/gateway`
   - 客户端通过 WebSocket 连接网关，收发即时消息。

> 提示：各服务对 APISIX 也有可选集成（见 `pkg/service/rest.go` 中的 `APISIXConfig`），如在本地不使用 APISIX，可以关闭对应开关。

### 目录结构概览

- `cmd/`：各服务的入口程序（api / rpc / gateway）
- `internal/`
  - `apps/<Service>/api`：HTTP 接口、路由、业务逻辑
  - `apps/<Service>/rpc`：gRPC 定义与实现、内部逻辑
  - `model/`：数据库模型
  - `common/`：公共 proto / 工具
- `pkg/`：通用基础设施（日志、配置、REST/RPC 启动封装、Redis 封装、Token 管理等）
- `scripts/`：辅助脚本（如 `scripts/db` 用于建表初始化等）

### 后续可以完善的内容

- 各服务配置样例（YAML/JSON）
- 完整部署文档（包含 APISIX、NATS、Mongo、MySQL、Redis）
- 典型调用流程示例（登录 → 建立 WebSocket → 发消息 → 会话列表/未读同步）


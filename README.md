# Kafka Pilot

Kafka 数据操作工具，提供 **命令行（CLI）** 与 **Web UI** 双端，用于查询、生产（注入）和消费审计 Kafka 数据。单二进制（Go）+ React 前端，配置与审计数据落地到本地 SQLite / YAML。

---

## 1. 构建

需要 Go 1.22+。

```bash
# 在项目根目录
go build -o kp.exe ./cmd/kp        # Windows
# 或
go build -o kp ./cmd/kp             # Linux / macOS
```

构建产物即 `kp`（或 `kp.exe`），所有功能都在这个二进制里。

---

## 2. 配置目录

所有配置（连接 profile）和审计数据都放在 **应用数据目录**：

- 环境变量 `KP_APPDATA` 指定时：使用其值。
- 未指定时：
  - Windows：`%APPDATA%/kafka-pilot`
  - Linux / macOS：`~/.kafka-pilot`

示例（用独立目录做测试，避免污染正式配置）：

```bash
export KP_APPDATA=D:/workspace/proj/kafka-pilot/test-data
```

---

## 3. 服务生命周期（Web UI 后端）

Kafka Pilot 的 Web UI 由一个本地 HTTP 服务托管。服务**只监听 `127.0.0.1`**，仅本机可访问。

| 命令 | 作用 |
|------|------|
| `kp serve --port 18080` | 启动服务并监听指定端口（推荐固定端口，方便书签） |
| `kp serve` | 启动服务，自动找一个空闲端口并打印访问 URL |
| `kp desktop` | 启动服务（自动端口）并自动打开浏览器 |
| `kp stop` | 停止正在运行的服务（基于 PID 文件，无需手动找进程） |
| `kp restart` | 停止旧服务并以**相同端口**重启，重新加载配置 |

示例：

```bash
# 启动（固定端口）
kp serve --port 18080
# 浏览器打开 http://127.0.0.1:18080

# 改了连接配置后，一键重启让配置生效
kp restart

# 关机 / 收工时停止
kp stop
```

> **实现说明**：`kp serve` / `kp desktop` 启动时会把 PID 与端口写入 `<应用数据目录>/kp.pid`。`kp stop` 读取该文件并结束对应进程，`kp restart` 先停旧进程再以相同端口启新进程。`stop` / `restart` 不依赖 Kafka 连接，因此即使配置损坏也能执行。

> **动态更新**：通过 Web UI「设置」页或 `PUT /api/profiles` 保存 profile 会**即时生效**（内存更新 + 落盘 + 失效该 profile 的 Kafka 连接缓存），**无需重启**。`kp restart` 仅在两种情况下才需要：① 你**直接手改了 `config.yaml` 文件**（绕过 UI/API，内存不会刷新）；② 用的是修复前的旧二进制（曾有内存污染 bug，保存反而会坏）。

---

## 4. 连接配置（Profile）

Web UI 里「设置」页可以添加 / 编辑 profile；也可用 CLI 管理。

```bash
# 添加一个 SASL_PLAINTEXT 集群
kp profile add --name prod \
  --bootstrap-servers 192.168.85.2:9092 \
  --sasl-mechanism plain \
  --sasl-username appuser \
  --sasl-password app-secret

# 添加一个 SASL_SSL 集群（必须带 tls 块）
kp profile add --name prod-ssl \
  --bootstrap-servers broker1:9093 \
  --sasl-mechanism scram-sha-256 \
  --sasl-username appuser \
  --sasl-password app-secret \
  --tls --tls-ca /path/to/ca.pem

# 设为默认（不带 --profile 时自动选用）
kp profile use prod

# 查看 / 列出
kp profile list
kp profile show prod
```

密码在磁盘上会被加密存储（Windows 用 DPAPI，其它平台 base64），API 返回时脱敏为 `****`。

> **TLS 提醒**：若 broker 监听器是 `SASL_SSL`（通常 9093），客户端**必须**加 `--tls` 并配置 CA；否则 TCP 连上后 broker 立即关连接，报 `broker closed the connection ... is SASL missing?`。明文端口（通常 9092，`SASL_PLAINTEXT`）则不需要 `--tls`。

---

## 5. CLI 命令参考

所有命令通用全局参数：`--profile <名称>`（不填用默认）、`--format json|table`（默认 json）。

### 5.1 profile — 连接配置
```bash
kp profile list | show <name> | add | use <name> | remove <name>
```

### 5.2 topic — 浏览主题
```bash
kp topic list --filter my-     # 按子串过滤
kp topic describe <topic>       # 分区数、各分区最新 offset
```

### 5.3 group — 消费组
```bash
kp group list --topic <topic>   # 列出消费组（可按 topic 过滤）
kp group lag <group> --topic <topic>
```

### 5.4 scan — 扫描消息（查询）
```bash
# 回扫最近 100 条
kp scan --topic orders --last 100

# 指定分区
kp scan --topic orders --partition 0,2 --last 500

# 按 key / key 正则 / value 关键字 过滤
kp scan --topic orders --key order-123
kp scan --topic orders --key-regex '^order-'
kp scan --topic orders --keyword "ERROR" --ignore-case

# 时间窗口
kp scan --topic orders --since 2026-11-01T00:00:00Z --until 2026-11-08T00:00:00Z

# 精确 offset 区间（与 --last 互斥）
kp scan --topic orders --offset-from 1000 --offset-to 2000

# 实验性：仅扫 key 经 murmur2 计算得到的分区（可能漏数据，需 --key 精确）
kp scan --topic orders --key order-123 --key-partition

# 输出到文件
kp scan --topic orders --last 1000 --output result.json
```

### 5.5 produce — 生产 / 注入消息
```bash
# 单条（key 必填，分区由 key 推导）
kp produce --topic orders --key order-123 --value '{"amount":99}'

# 批量（JS 脚本，见 Web UI「数据注入」页说明）
kp produce --topic orders --script batch.js --count 100 --concurrency 8
kp produce --topic orders --script batch.js --count 100 --dry-run   # 仅预览
```

### 5.6 session / audit — 操作审计
```bash
kp session new --label "排查订单丢失"
kp session list
kp audit list --source human --since 2026-11-01T00:00:00Z
kp audit show <op-id>
kp audit clear --before 2026-01-01T00:00:00Z
```

### 5.7 schema — 命令元信息
```bash
kp schema        # 输出可用命令与退出码（JSON）
```

---

## 6. Web UI 使用

启动服务后访问 `http://127.0.0.1:<端口>`，主要功能页：

- **设置**：管理连接 profile（含 SASL / TLS），保存后自动失效旧连接，下次操作按新配置重连。
- **主题**：浏览 topic、查看分区与 offset。
- **查询（Scan）**：可视化配置各项扫描过滤器；支持「按 key 定位分区（实验性）」开关——开启后用 murmur2 预估 key 所在分区并预置分区筛选，但**可能因异源分区器 / 显式分区而漏数据**，请谨慎。
- **数据注入（Produce）**：单条或 JS 脚本批量注入；可在页内查看 `ctx` 可用变量 / 函数说明与示例。
- **审计**：查看历史操作记录。

---

## 7. 常见问题

**Q：改了 SASL 密码，Web UI 还是连不上？**
A：通过 Web UI / API 保存是**即时动态生效**的，本不需要重启。若仍连不上，常见原因是：① SASL `mechanism` 与服务端不一致；② 之前手改过 `config.yaml` 文件却没重启（内存未刷新）。先 `GET /api/profiles` 确认实际生效的 `mechanism` / 端口，再决定是否 `kp restart`。

**Q：`broker closed the connection ... is SASL missing?`**
A：通常是 TLS 没配对。若 broker 是 `SASL_SSL`（9093），必须加 `--tls`（`SASL_PLAINTEXT` / 9092 不需要）。也可能是 `mechanism` 与服务端不一致，或密码错误。

**Q：`kp stop` 提示找不到服务？**
A：说明没有运行中的服务，或 PID 文件丢失。直接 `kp serve` 启动即可；残留的 `kp.pid` 会在下次 `stop` 时自动清理。

---

## 8. 退出码

| 码 | 含义 |
|----|------|
| 0  | 成功 |
| 2  | 参数错误 |
| 3  | 连接错误（Kafka / SASL / TLS） |
| 4  | 超时 |
| 5  | 内部错误 |

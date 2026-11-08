---
name: kp-cli
description: >-
  Kafka Pilot (kp) 命令行与本地 REST 服务的详尽使用指南。当需要操作 Kafka——
  查询/描述 topic 与分区、按关键字/key/时间窗/offset 扫描消息、单条或批量生产消息、
  查看 consumer group 积压(lag)、管理审计与会话、配置连接 profile、启动/停止/重启
  本地 Web/API 服务——且目标是通过 kp 的 CLI 或本地 HTTP API 完成时，加载本 skill。
  涵盖所有子命令与 flag、配置与 SASL/TLS 陷阱、扫描结果字段口径、退出码、REST 端点、
  以及反复踩过的故障排查。
---

# Kafka Pilot CLI 指南（详尽版）

Kafka Pilot（命令名 `kp`）是一个单二进制 (`kp.exe` / `kp`) 的 Kafka 数据工具，
提供 **CLI 直接操作** 和 **本地 Web UI / REST 服务** 两种入口。本指南供 AI Agent
在调用 kp 时参考，确保命令正确、避免已知的坑。

---

## 1. 何时使用本 skill

- 用户要检查某个 topic 有几个分区、最新 offset、某分区存了什么消息。
- 用户要按 key / 关键字 / 时间窗 / offset 范围在 Kafka 里翻找消息（排查数据是否写入、落哪个分区）。
- 用户要向 topic 写一条/一批消息（含脚本化批量造数、dry-run 预览）。
- 用户要查 consumer group 的积压（lag）排查消费延迟。
- 用户要配置/修改 Kafka 连接（SASL/TLS），或启动/停止/重启本地 kp 服务。
- 用户要审计“谁在什么时候对哪个 topic 做了什么”（kp 自带操作审计）。

> 注意：kp 是**只读/写数据的运维工具**，不负责创建 topic、改分区数、建 ACL 等集群管理动作。

---

## 2. 构建与基本可用性

```bash
# 在项目根目录
go build -o kp.exe ./cmd/kp      # Windows
go build -o kp    ./cmd/kp       # Linux/macOS

./kp --help                     # 顶层命令列表
./kp <command> --help            # 某命令的 flag 列表
./kp schema                     # 机器可读：命令清单 + 退出码（可直接解析）
```

顶层命令：`profile` `topic` `group` `scan` `produce` `session` `audit` `schema`
`serve` `desktop` `stop` `restart`。

---

## 3. 配置目录与环境变量（极易踩坑，先读）

### 3.1 配置目录 `KP_APPDATA`

kp 把所有连接配置、pid 文件、审计库放在一个“应用数据目录”：

| 系统 | 默认目录 |
|------|----------|
| Windows | `%APPDATA%/kafka-pilot` （通常 `C:\Users\<user>\AppData\Roaming\kafka-pilot`） |
| Linux/macOS | `~/.kafka-pilot` |

可用环境变量覆盖：

```bash
export KP_APPDATA=/abs/path/to/dir     # 所有 kp 子命令、serve 都读这个目录
```

> ⚠️ **一致性铁律**：`kp serve`（服务进程）和 `kp <任何读写配置命令>` 必须指向**同一个**
> `KP_APPDATA` 目录。否则会出现“我明明配了 profile，但服务列表里看不到”的假象——
> 那只是服务加载了另一个目录的 `config.yaml`。要么都不设（用默认目录），要么每次都设同一个绝对路径。

### 3.2 配置文件 `config.yaml` 结构

```yaml
default_profile: prod            # 不传 --profile 时使用的 profile
profiles:
  - name: prod
    bootstrap_servers: ["broker1:9092", "broker2:9092"]   # 必填
    sasl:                         # 可选；无则匿名/plaintext 连接
      mechanism: plain            # plain | scram-sha-256 | scram-sha-512
      username: appuser
      password: app-secret        # 见 3.3 密码处理
    tls:                          # 可选；SASL_SSL 或 broker 要求 TLS 时必填
      ca_cert: /path/to/ca.pem    # 单向：客户端校验服务端证书
      skip_verify: true           # 跳过证书校验（仅测试/自签，生产慎用）
```

字段说明：
- `bootstrap_servers`：逗号分隔字符串会被 `ParseBootstrapServers` 拆成数组；CLI 用 `--bootstrap-servers "b1:9092,b2:9092"`。
- `sasl.mechanism`：必须和 broker 端 `sasl.enabled.mechanisms` **完全一致**，否则 broker 拒绝连接。
- `tls`：见 3.4。

### 3.3 密码的加密与“跨机器失效”陷阱

- 通过 **UI/CLI 输入明文密码**后，kp 在落盘时自动用 DPAPI（Windows）或 base64（其它系统）加密；启动时自动解密回明文，内存里始终是明文。
- ⚠️ Windows 的 DPAPI 密文与**生成它的机器 + Windows 用户**绑定。把 `config.yaml` 拷到别的机器/用户，
  解密会静默失败 → 发出的密码变成无效值 → broker 报 `Invalid username or password`
  （表现和“SASL missing”几乎一样）。**正确做法是在目标机器上重新用 UI/CLI 输入明文密码**，不要手贴密文。

### 3.4 SASL / TLS 监听器类型（连不上时的头号元凶）

| broker listener | 端口(典型) | 客户端配置 |
|----------------|-----------|-----------|
| `PLAINTEXT` | 9092 | 不要 sasl、不要 tls |
| `SASL_PLAINTEXT` | 9092 | 仅 `sasl`，**不要** `tls` |
| `SASL_SSL` | 9093 | `sasl` **且** `tls`（缺 tls 会报 `broker closed ... SASL missing?`） |

> 报错 `broker closed the connection immediately after a request was issued ... is SASL missing?`
> 几乎总是“TCP 连上后 broker 立刻关连接”，根因通常是 **SASL_SSL 没配 tls**、**mechanism 不匹配**、
> 或**生效配置里的密码不是真密码**（见 3.3）。

### 3.5 配置的动态更新（无需重启）

- 通过 **Web UI / REST API / CLI `profile add|use`** 保存 profile 是**即时生效**的：
  内存更新 + 落盘 + 失效该 profile 的 Kafka 连接缓存，下次操作即用新配置。
- **唯一仍需重启的场景**：你**直接手改 `config.yaml` 文件**（绕过 UI/API），内存不会刷新，此时才需 `kp restart`。
- 因此“改了 SASL 还连不上”通常不是代码问题，而是生效配置里密码不对 / 目录不一致 / 旧进程内存被污染（旧二进制 bug，已修复）。

---

## 4. 全局 flags（所有命令通用）

| flag | 含义 |
|------|------|
| `--profile <name>` | 指定连接 profile；省略则用 `default_profile` |
| `--session <id>` | 关联审计会话 ID（先用 `kp session new` 拿到） |
| `--intent "<text>"` | AI 声明的操作意图，写入审计（合规追溯） |
| `--format json|table` | 输出格式，默认 `json`（`table` 目前等价 JSON 缩进） |

---

## 5. 命令参考

### 5.1 `profile` — 连接配置管理
| 子命令 | 说明 | 关键 flag |
|--------|------|-----------|
| `kp profile list` | 列出所有 profile（密码被 mask 成 `****`） | — |
| `kp profile add` | 新增 profile | `--name`(必填) `--bootstrap-servers`(必填) `--sasl-mechanism` `--sasl-username` `--sasl-password` `--tls` `--tls-ca` `--tls-skip-verify` |
| `kp profile use <name>` | 设为默认 profile | — |
| `kp profile show <name>` | 查看详情（密码 mask） | — |
| `kp profile remove <name>` | 删除 profile（若为默认则自动改指第一个） | — |

示例：
```bash
kp profile add --name prod \
  --bootstrap-servers "192.168.85.2:9092" \
  --sasl-mechanism plain --sasl-username appuser --sasl-password app-secret
kp profile use prod
```

### 5.2 `topic` — 浏览 topic
| 子命令 | 说明 | 关键 flag |
|--------|------|-----------|
| `kp topic list` | 列出 topic（名称、分区数、最新 offset） | `--filter <子串>` |
| `kp topic describe <topic>` | 描述单 topic（分区数、副本、配置等） | — |

```bash
kp topic list --filter order
kp topic describe kp-multi-part --profile prod
```

### 5.3 `group` — consumer group
| 子命令 | 说明 | 关键 flag |
|--------|------|-----------|
| `kp group list` | 列出消费组 | `--topic <topic>` 按 topic 过滤 |
| `kp group lag <group>` | 查看某组的积压 lag | `--topic <topic>` 按 topic 过滤 |

```bash
kp group list --profile prod
kp group lag my-consumer --topic kp-multi-part --profile prod
```

### 5.4 `scan` — 扫描消息（核心，最容易用错）

**flags：**

| flag | 默认值 | 说明 |
|------|--------|------|
| `--topic` | 必填 | 目标 topic |
| `--partition` | 全部 | 逗号分隔的分区号，如 `0,2` |
| `--last` | 100 | **每分区**回扫条数，CLI 上限 10000；Web UI 固定五档 100/500/1000/5000/10000 |
| `--since` / `--until` | — | 时间窗，RFC3339（如 `2026-11-08T00:00:00+08:00`） |
| `--offset-from` / `--offset-to` | — | 精确 offset 范围（与 `--last` 互斥，设了后者归零） |
| `--key` | — | 精确 key 过滤 |
| `--key-regex` | — | key 正则过滤 |
| `--key-partition` | false | **实验**：只扫 murmur2(key) 算出的单分区（见 5.4.1） |
| `--keyword` | — | value 子串过滤 |
| `--ignore-case` | true | 关键字大小写不敏感 |
| `--expr` | — | jq 表达式过滤 |
| `--timeout` | 30 | 扫描超时（秒） |
| `--output <file>` | — | 结果写文件而非 stdout |

**结果字段口径（很重要，避免误读）：**
- `total`：所有被扫描分区的**总扫描条目数**（= 分区数 × 每分区回扫量，受扫描上限约束）。
- `matched`：应用全部过滤器（key/keyword/时间窗/expr）后的**命中数**。
- `matched_raw`：仅在被“安全上限/超时”截断时出现，表示截断前的原始命中数。
- `truncated`：是否发生了截断（安全上限 50000 条，或超时）。**正常小扫描不会截断**。
- `warnings`：如 `result-cap` / `timeout`。
- `messages[]`：命中的消息列表（含 partition、offset、key、value、timestamp）。
- `key_partition` / `key_partition_count`：**仅当使用 `--key-partition`** 时出现，标明实际只扫了哪个分区、总共几个分区。

> 例：`kp-multi-part` 有 12 个分区，`--last 100` → `total=1200`；若再加 `--keyword xxx` 命中 100 条，
> 则 `matched=100`。这**不是截断**，而是 12 个分区各回扫 100 条共 1200 条被检查、其中 100 条匹配。
> 没有任何“全局截断”——之前 `matched` 远小于 `total` 的旧行为是个已修复的 bug。

**5.4.1 `--key-partition` 实验开关（会静默漏数据！）**
- 开启后，kp 用标准 Kafka 默认分区器的 murmur2 哈希：`murmur2(key) & 0x7fffffff % 分区数`，
  只扫描算出的**那一个**分区，并忽略 `--partition` 显式选择。
- **风险**：只要生产端用了**非标准分区器**（如 `ManualPartitioner` 显式指定分区）或发生过**分区扩缩容**，
  相同 key 的记录可能落在别的 partition → **本开关会静默漏掉这些数据**。即使“没扩缩容”，
  只要写入端用的是异源分区器也会漏。默认关闭，仅在你能 100% 确认生产端用默认分区器时再用。
- 想先“算一下 key 会落哪个分区”而不缩小扫描范围，用 REST `GET /api/key-partition`
  （见第 8 节）或 Web UI 的「按 key 定位分区」开关（它只预置分区选择、不强制缩小）。

示例：
```bash
# 回扫最近 500 条/分区，关键字匹配
kp scan --topic kp-multi-part --last 500 --keyword "order-id-123" --profile prod

# 只看 0、2 两个分区，时间窗内
kp scan --topic orders --partition 0,2 --since 2026-11-01T00:00:00+08:00 --until 2026-11-08T00:00:00+08:00

# 精确 key（单条定位，推荐用这个而不是 --key-partition）
kp scan --topic orders --key "user-42" --profile prod
```

### 5.5 `produce` — 生产消息

**单条模式（默认）：**
| flag | 必填 | 说明 |
|------|------|------|
| `--topic` | ✅ | 目标 topic |
| `--key` 或 `--key-file` | ✅ | 消息 key；**key 决定分区**（默认分区器） |
| `--value` 或 `--value-file` | ✅ | 消息 value |
| `--header key=value` | 可重复 | 消息头 |
| `--dry-run` | — | 仅 CLI batch 有；单条无 |

```bash
kp produce --topic orders --key "user-42" --value '{"amount":99}' --header src=cli --profile prod
```

**批量模式（JS 脚本）：** 加 `--script <file.js>` 即进入批量。
| flag | 默认 | 说明 |
|------|------|------|
| `--script` | — | 本地 `.js` 文件路径（Web UI 用内联内容） |
| `--count` | 100 | 生成调用次数（generate 被调用 count 次） |
| `--concurrency` | 8 | 并发（1–32） |
| `--rate` | 0 | 限速 msgs/sec（0=不限） |
| `--dry-run` | false | 仅预览不发送，结果含 `samples` |

脚本约定（goja JS 沙箱）：
- **必须导出** `function generate(ctx) { ... }`（返回单条消息对象 / 消息数组 / `null` 表示结束流）。
- `ctx` 提供的变量：
  - `ctx.index`：当前调用序号（int，从 0 开始）
  - `ctx.time`：当前毫秒时间戳（`time.Now().UnixMilli()`）
  - `ctx.random()`：返回 `[0,1)` 浮点随机数
  - `ctx.uuid()`：返回 v4 UUID 字符串
  - `ctx.pick(arr)`：从数组随机取一个元素
- 消息对象字段：`{ key, value, headers?, partition? }`（`headers` 为 `{k:v}` 对象，`partition` 可选可省略）。
- 单条 `generate` 调用返回数组即可一次造多条；脚本**无需自己写循环**，`--count` 决定调用次数。

示例脚本 `gen.js`：
```js
function generate(ctx) {
  return {
    key: "user-" + ctx.index,
    value: JSON.stringify({ ts: ctx.time, r: ctx.random(), id: ctx.uuid() }),
    headers: { batch: "smoke" }
  };
}
```
```bash
kp produce --topic orders --script gen.js --count 1000 --concurrency 16 --profile prod
kp produce --topic orders --script gen.js --count 10 --dry-run --profile prod   # 先看样本
```

### 5.6 `session` — 审计会话
| 子命令 | 说明 | flag |
|--------|------|------|
| `kp session new` | 创建会话，返回 `sessionId` | `--label` |
| `kp session end <id>` | 结束会话 | — |
| `kp session list` | 列出会话 | — |
| `kp session show <id>` | 查看会话详情 + 其下操作 | — |

用法：先把 `--session <id>` 传给后续 `scan`/`produce`，便于在审计里按会话归类。

### 5.7 `audit` — 操作审计
| 子命令 | 说明 | flag |
|--------|------|------|
| `kp audit list` | 列出操作 | `--session` `--source ai|human` `--since`(RFC3339) |
| `kp audit show <op-id>` | 操作详情 | — |
| `kp audit clear` | 清空审计 | `--before`(RFC3339) |

审计会记录每条 `scan`/`produce` 的调用者来源（ai/human）、意图（`--intent`）、参数摘要、结果。Web UI 也有审计流。

### 5.8 `schema` — 命令自描述
`kp schema` 输出 JSON：所有命令名 + 退出码映射。Agent 可直接解析它来确认支持的命令与退出码，无需硬编码。

### 5.9 服务生命周期：`serve` / `desktop` / `stop` / `restart`
| 命令 | 说明 |
|------|------|
| `kp serve --port <n>` | 启动本地 HTTP 服务；`--port 0` 自动选空闲端口并打印 URL |
| `kp desktop` | 自动端口 + 自动打开浏览器 |
| `kp stop` | 读 `kp.pid` 杀掉运行中的服务并删 pid 文件 |
| `kp restart` | 停旧进程后**以相同端口**重启，配置重载（不依赖 Kafka 连接） |

- 服务**只监听 `127.0.0.1`**（本机），远程访问不到。
- `stop`/`restart` 通过 `KP_APPDATA` 目录下的 `kp.pid` 定位实例（记录 `pid`/`port`/`started_at`），
  **不接收端口参数**，因此 stop/restart 与 serve 必须在同一 `KP_APPDATA` 下。
- ⚠️ **常驻陷阱**：服务是前台长驻进程。若用 `./kp serve &` 在普通 shell 里起，shell 退出会被回收（exit 0），
  表现就是“服务莫名其妙没了”。
  - 推荐：用工具/系统的**后台任务机制**常驻，或用 **Windows 任务计划程序**（`schtasks`）/ `nssm` 注册成服务，
    实现开机自启、会话无关。
  - 示例（管理员 PowerShell）：
    ```powershell
    schtasks /Create /TN "KafkaPilot" /TR "D:\path\kp.exe serve --port 18080" /SC ONSTART /RL HIGHEST /F
    schtasks /Run /TN "KafkaPilot"
    ```

---

## 6. 退出码（供 Agent 判错）

| 码 | 含义 | 典型原因 |
|----|------|----------|
| 0 | 成功 | — |
| 2 | 参数错误 | 缺必填 flag、格式错误（`--since` 非 RFC3339）、offset 范围非法 |
| 3 | 连接错误 | Kafka 不可达、SASL/TLS 配置错误、认证失败 |
| 4 | 超时 | 扫描/生产超过 `--timeout` |
| 5 | 内部错误 | 序列化失败、未预期异常 |

错误输出是 JSON 到 stderr：`{"code":N,"message":"...","hint":"..."}`（部分含 `hint` 排查建议）。

---

## 7. 典型工作流

**场景 A：确认数据有没有写入某个 topic、落在哪**
```bash
kp profile use prod
kp scan --topic orders --key "order-1001" --profile prod      # 精确 key 定位
kp scan --topic orders --keyword "1001" --last 1000 --profile prod   # 关键字兜底
```

**场景 B：批量造测试数据并预览**
```bash
kp produce --topic orders --script gen.js --count 50 --dry-run --profile prod   # 先预览
kp produce --topic orders --script gen.js --count 5000 --concurrency 16 --profile prod
```

**场景 C：排查消费延迟**
```bash
kp group lag my-consumer --topic orders --profile prod
```

**场景 D：起服务供 Web UI / 脚本调用**
```bash
kp serve --port 18080        # 然后用 curl 调 /api/*（见下）
```

---

## 8. REST API 速查（`kp serve` 之后）

服务监听 `http://127.0.0.1:<port>`。所有 JSON 响应错误含 `code`/`message`/`hint`。
（与 CLI 等价，便于 Agent 用 `curl`/HTTP 客户端调用，避免反复起进程。）

| 方法 & 路径 | 说明 |
|-------------|------|
| `GET /api/health` | 健康检查 `{"status":"ok"}` |
| `GET /api/profiles` | 列出 profile（密码 mask） |
| `POST /api/profiles` | 新增 profile（body 同 Profile 结构） |
| `GET /api/profiles/{name}` | 查看 |
| `PUT /api/profiles/{name}` | 更新（**即时生效**，密码传 `****` 表示不改） |
| `DELETE /api/profiles/{name}` | 删除 |
| `PUT /api/profiles/{name}/default` | 设为默认 |
| `GET /api/topics?profile=&filter=` | 列 topic |
| `GET /api/topics/{topic}?profile=` | 描述 topic |
| `GET /api/key-partition?topic=&key=&profile=` | 算 key 落哪个分区（只读提示，不缩小扫描） |
| `POST /api/scan` | 扫描，body 见 `ScanRequest`（字段同 CLI flags：topic/partitions/last/since/until/offset_from/offset_to/key/key_regex/keyword/ignore_case/expr/timeout_sec/key_partition/profile） |
| `POST /api/produce` | 单条生产，body `{profile,topic,key,value,headers}` |
| `POST /api/produce/batch` | 批量生产，body 同 `BatchRequest`（topic/script_content/count/concurrency/rate/dry_run） |
| `GET /api/groups?profile=&topic=` | 列消费组 |
| `GET /api/groups/{group}/lag?profile=&topic=` | 消费组 lag |
| `GET /api/sessions` / `GET /api/sessions/{id}` / `DELETE /api/sessions` | 会话 |
| `GET /api/audit/ops` / `DELETE /api/audit/ops` / `GET /api/audit/ops/{op_id}` / `GET /api/audit/ops/{op_id}/snapshot` / `GET /api/audit/stream` / `DELETE /api/audit` | 审计 |

示例：
```bash
# 算 key 落哪个分区（只读提示）
curl "http://127.0.0.1:18080/api/key-partition?topic=orders&key=user-42&profile=prod"

# 扫描（与 CLI 等价）
curl -s -X POST http://127.0.0.1:18080/api/scan \
  -H 'Content-Type: application/json' \
  -d '{"profile":"prod","topic":"orders","last":500,"keyword":"1001"}'
```

---

## 9. 故障排查清单

1. **连不上 / “SASL missing”**
   - 确认 listener 类型：SASL_SSL 必须加 `tls` 块；SASL_PLAINTEXT 不要 tls（§3.4）。
   - `mechanism` 与服务端一致；端口是开了 SASL 的那个 listener。
   - `GET /api/profiles`（或 `kp profile show`）确认生效的 `sasl.mechanism`/密码正确。
   - 密码若是 DPAPI 密文且跨机器/用户 → 重新输入明文（§3.3）。

2. **“我配的 profile 不见了”**
   - 几乎总是 `KP_APPDATA` 目录不一致（§3.1）：serve 和 CLI 加载了不同的 `config.yaml`。统一目录即可。

3. **改了配置还是连不上**
   - 通过 UI/API/CLI 保存是即时生效的（§3.5）；只有手改文件才需 `kp restart`。
   - 确认是**新二进制**（`go build` 后来）在跑——旧进程内存可能被污染。

4. **服务莫名其妙没了**
   - 长驻进程被 shell 回收（§5.9）。改用后台任务/任务计划常驻。

5. **scan 结果 `total` 很大、`matched` 很小，是不是被截断了？**
   - 一般不是。`total`=各分区回扫总量，`matched`=过滤命中（§5.4 字段口径）。
   - 只有 `truncated=true`（安全上限 50000 或超时）才需担心丢数据，那时会出现 `matched_raw`。

6. **`--key-partition` 为什么漏数据？**
   - 只扫 murmur2 算出的单分区；生产端若用异源分区器/显式分区/发生过扩缩容，记录会在别的 partition（§5.4.1）。
   - 优先用 `--key`（精确 key 全量扫描定位）而非 `--key-partition`。

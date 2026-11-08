---
name: kp-cli
description: >-
  Kafka Pilot (kp) 命令行的使用指南。当需要操作 Kafka——
  查询/描述 topic 与分区、按关键字(key 表达式)/key/时间窗/offset 扫描消息、单条或批量生产消息、
  查看 consumer group 积压(lag)、管理审计与会话、选择/配置连接 profile、配置/迁移数据目录、
  启动/停止/重启本地服务——且目标是通过 kp 的 CLI 完成时，加载本 skill。
  涵盖 profile 选择策略、数据目录与一次性迁移、所有子命令与 flag、扫描结果字段口径、退出码。
---

# Kafka Pilot CLI 指南

Kafka Pilot（命令名 `kp`）是一个单二进制的 Kafka 数据工具，提供 **CLI 直接操作**。本指南供 AI Agent 在调用 kp 时参考，确保命令正确、避免已知的坑。

> 前提：`kp` 已安装并可用（命令名 `kp` / `kp.exe`）。本指南不涉及安装步骤。
> **本文件是 Agent 使用 kp 的契约：kp 暴露的任何命令 / flag / 退出码 / 输出字段 / 脚本语义发生变化时，必须同步更新本文件**（维护纪律见 `agents.md`）。

---

## 1. 何时使用本 skill

- 检查某个 topic 有几个分区、最新 offset、某分区存了什么消息。
- 按 key / 关键字 / 时间窗 / offset 范围在 Kafka 里翻找消息（排查数据是否写入、落哪个分区）。
- 向 topic 写一条/一批消息（含脚本化批量造数、dry-run 预览）。
- 查 consumer group 的积压（lag）排查消费延迟。
- 配置/修改 Kafka 连接（profile），查看/修改/迁移数据目录（`kp settings`），或启动/停止/重启本地 kp 服务。
- 审计“谁在什么时候对哪个 topic 做了什么”（kp 自带操作审计）。

> 注意：kp 是**只读/写数据的运维工具**，不负责创建 topic、改分区数、建 ACL 等集群管理动作（也**没有**相关命令）。

---

## 2. 连接 Profile 选择策略（先读，再执行任何读写命令）

kp 通过「连接 profile」区分不同的 Kafka 集群。执行任何读写命令（`topic`/`scan`/`produce`/`group`/`audit`）前，**先确定用哪个 profile**：

1. **列出当前 profile**：`kp profile list`
2. **按数量分支处理**：
   - **0 个 profile（首次使用）**：先引导用户配置一个，再继续。
     ```bash
     kp profile add --name prod \
       --bootstrap-servers "192.168.85.2:9092" \
       --sasl-mechanism plain --sasl-username appuser --sasl-password app-secret
     kp profile use prod          # 设为默认，后续可省略 --profile
     ```
     **在拿到至少一个可用 profile 之前，不要继续执行读写命令。**
   - **恰好 1 个 profile**：直接默认使用，**无需询问用户、也无需显式 `--profile`**。
   - **多个 profile**：引导/询问用户要操作哪一个，选定后用 `--profile <name>` **显式传参**（不要用 `default_profile` 去猜测用户意图）。
3. 之后的所有示例都以「已选定 profile」为前提：单 profile 时可省 `--profile`，多 profile 时务必带上。

> **会话与审计**：作为 AI Agent，每次任务开始前先 `kp session new` 拿到 6 位短 ID，之后**每条命令显式携带 `--session <id>` 与 `--intent "<你的意图>"`**，便于在观测区按会话追溯。未带 `--session` 的命令会归入名为 `default` 的兜底会话。

> **数据目录异常时**：`kp profile list` 为空或不是预期的 profile，先 `kp settings show` 检查 `active_data_dir`（服务生效目录）与 `data_dir`（配置值）是否一致——常见原因是改过数据目录却没 `kp restart`，或 `KP_APPDATA` 指向了别的目录。**不要盲目重新添加 profile**。

---

## 3. 快速开始

```bash
kp --help                     # 顶层命令列表
kp <command> --help            # 某命令的 flag 列表
kp schema                     # 机器可读：命令清单 + 退出码（可直接解析）
kp settings show               # 当前数据目录布局（profile / 审计数据在哪）
```

顶层命令：`profile` `topic` `group` `scan` `produce` `session` `audit` `key-partition` `schema` `settings`
`serve` `desktop` `stop` `restart`。

**数据目录（FR-16；排查「profile 突然不见了」先看这里）**：profile（`config.yaml`）与审计库（`audit.db`）默认在 **kp.exe 同目录 `kp-data/`**；`kp-settings.yaml` 固定放 kp.exe 旁，其 `data_dir` 可把数据目录改到任意位置（相对路径基于 exe 目录，绝对路径原样）。优先级：`KP_APPDATA` 环境变量 > `kp-settings.yaml` 的 `data_dir` > 默认 `kp-data`。首次启动会把旧位置（Windows `%APPDATA%/kafka-pilot`，Linux / macOS `~/.kafka-pilot`）的数据**一次性复制**过来：不删除源文件、不覆盖目标已有文件、`audit.db` 与 `-wal`/`-shm` 整组复制。**改目录或迁移后必须 `kp restart`**；`GET /api/settings` 的 `active_data_dir` 是服务实际生效目录，`pending_change=true` 表示配置已改但服务未重启。完整命令见 §5.11。

---

## 4. 全局 flags（所有命令通用）

| flag | 含义 |
|------|------|
| `--profile <name>` | 指定连接 profile；省略则用 `default_profile`（见 §2 选择策略） |
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
| `kp profile update <name>` | 更新已有 profile（bootstrap / SASL / TLS 整体覆盖） | `--bootstrap-servers`(必填) `--sasl-mechanism` `--sasl-username` `--sasl-password` `--tls` `--tls-ca` `--tls-skip-verify` |
| `kp profile remove <name>` | 删除 profile（若为默认则自动改指第一个） | — |

> `kp profile show` 仅查看（密码 mask 为 `****`），`kp profile use` 仅切换默认。SASL 密码经 Windows DPAPI（其它平台 base64）加密存储，CLI/UI 不感知明文。

### 5.2 `topic` — 浏览 topic
| 子命令 | 说明 | 关键 flag |
|--------|------|-----------|
| `kp topic list` | 列出 topic（**极简**：名称、分区数、各分区最新 offset） | `--filter <子串>` |
| `kp topic describe <topic>` | 描述单 topic；结构与 list 单项**同构**（`name` / `partitions_count` / `partitions[]`，每元素 `partition`+`latest_offset`），**不**含副本 / 清理策略 / HWM 等其它元数据 | — |

```bash
kp topic list --filter order
kp topic describe kp-multi-part --profile prod
```

### 5.3 `group` — consumer group
| 子命令 | 说明 | 关键 flag |
|--------|------|-----------|
| `kp group list` | 列出消费组 | `--topic <topic>` 按 topic 过滤 |
| `kp group lag <group>` | 查看某组的积压 lag（超阈值默认 10000 标 `highLag`） | `--topic <topic>` 按 topic 过滤 |

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
| `--last` | 100 | **每分区**回扫条数，CLI 上限 10000 |
| `--since` / `--until` | — | 时间窗，RFC3339（如 `2026-11-08T00:00:00+08:00`） |
| `--offset-from` / `--offset-to` | — | 精确 offset 范围（与 `--last` 互斥，设了后者归零） |
| `--key` | — | 精确 key 过滤 |
| `--key-regex` | — | key 正则过滤（RE2 语法） |
| `--key-partition` | false | **实验**：只扫 murmur2(key) 算出的单分区（见 5.4.1） |
| `--keyword` | — | value 关键字过滤（子串 + AND/OR/NOT 布尔组合；**不支持 `>` `<` `==` 等比较运算符**，按值比较请用 `--expr`） |
| `--ignore-case` | true | 关键字大小写不敏感（可关成 `false` 做大小写敏感匹配） |
| `--expr` | — | jq 表达式过滤（作用于消息 value 解析后的 JSON，见下方说明） |
| `--timeout` | 30 | 扫描超时（秒，1~300） |
| `--output <file>` | — | 结果写文件而非 stdout |

**`--keyword` 是布尔表达式，不只是子串**：除默认的大小写不敏感子串匹配外，支持一套小型布尔表达式语言：
- 运算符：`AND` / `OR` / `NOT`（大写；优先级 `NOT` > `AND` > `OR`，括号最高）。
- 词项：裸词（如 `ERROR`）或引号包裹的短语（如 `"error code"`）；每个词项都是对“key + value + headers 拼接文本”的大小写不敏感子串匹配。**词项永远是子串，不能用 `amount > 100` 这类比较运算符**；需要按数值/字段比较请改用 `--expr`（jq）。
- 若表达式语法非法，会**自动降级**为整串的普通子串匹配（不报错，只是失去组合能力）。

```bash
kp scan --topic orders --keyword "ERROR AND retry"
kp scan --topic orders --keyword "FAILED OR (timeout AND NOT retry)"
```

**`--expr` 是 jq 表达式（作用在消息 value 的解析结果上）**：接收标准 [jq](https://jqlang.github.io/jq/) 查询，作用对象是消息 **value 解析后的 JSON**（若 value 不是合法 JSON，则退化为原文字符串），命中条件为“表达式结果为真值（truthy）”。与 `--keyword` 的区别：`--keyword` 只在文本里做子串/布尔匹配；`--expr` 能按 JSON 结构做精确字段判断。表达式解析/编译失败会**直接报错退出**（不会降级）。

```bash
kp scan --topic orders --expr '.status == "FAILED"' --profile prod
kp scan --topic orders --expr '(.amount > 100) and (.currency == "USD")' --profile prod
```

**结果字段口径（很重要，避免误读）：**
- `opId`：本次操作 ID。
- `total`：本次**实际读取并检查**的原始消息数（≈ 分区数 × 每分区回扫量），**不是 topic 总消息数、也不是命中数**。
- `matched`：三层过滤后的**全部命中数**，各分区结果都保留、按时间倒序跨分区归并；常规场景 `matched ≤ 分区数 × last`。
- `matched_raw`：仅在被「安全上限 50000 条 / 超时」截断时出现，为截断前真实命中数。
- `truncated`：是否发生了截断（安全上限 `MaxResultMessages=50000`，或超时）。**正常小扫描不会截断**。
- `warnings`：机器码，如 `scan>1000` / `result-cap` / `timeout` / `key-partition`。
- `partition_ranges`：各分区实际扫描区间，前端据此生成「结果分区」筛选条。
- `messages[]`：命中消息列表（含 `partition` / `offset` / `key` / `value` / `timestamp` / `headers`）。
- `key_partition` / `key_partition_count`：**仅当使用 `--key-partition`** 时出现，标明实际只扫了哪个分区、总共几个分区。
- `skippedNonUtf8Key`：因 key 非 UTF-8 而未参与 key 过滤的条数。

> 例：`kp-multi-part` 有 12 个分区，`--last 100` → `total=1200`；若再加 `--keyword xxx` 命中 100 条，则 `matched=100`。这**不是截断**，而是 12 个分区各回扫 100 条共 1200 条被检查、其中 100 条匹配。没有任何“全局截断到 100 条”。

**5.4.1 `--key-partition` 实验开关（会静默漏数据！）**
- 开启后，kp 用标准 Kafka 默认分区器的 murmur2 哈希：`murmur2(key) & 0x7fffffff % 分区数`，只扫描算出的**那一个**分区，并忽略 `--partition` 显式选择。
- **风险**：只要生产端用了**非标准分区器**（如 `ManualPartitioner` 显式指定分区）或发生过**分区扩缩容**，相同 key 的记录可能落在别的 partition → **本开关会静默漏掉这些数据**。默认关闭，仅在你能 100% 确认生产端用默认分区器时再用。
- 强制约束：开启 `--key-partition` 时**必须**提供精确 `--key`，且**不能与 `--key-regex` 同用**，否则直接报错退出。

```bash
kp scan --topic kp-multi-part --last 500 --keyword "order-id-123" --profile prod
kp scan --topic orders --partition 0,2 --since 2026-11-01T00:00:00+08:00 --until 2026-11-08T00:00:00+08:00
kp scan --topic orders --key "user-42" --profile prod
kp scan --topic orders --offset-from 1000 --offset-to 2000 --profile prod   # 精确 offset 区间
```

### 5.5 `key-partition` — 独立查询 key 落点（只读，不扫描）
仅计算某 key 在标准默认分区器下会落到哪个分区，**不产生任何扫描**，用于在扫描前预判落点。

```bash
kp key-partition --topic orders --key "user-42" --profile prod
```
输出：`{ "topic", "key", "partition", "partition_count", "algorithm": "murmur2(key) & 0x7fffffff % partition_count" }`。

### 5.6 `produce` — 生产消息

**单条模式（默认）：**
| flag | 必填 | 说明 |
|------|------|------|
| `--topic` | ✅ | 目标 topic |
| `--key` 或 `--key-file` | ✅ | 消息 key；**key 决定分区**（默认分区器），不可手动指定分区 |
| `--value` 或 `--value-file` | ✅ | 消息 value |
| `--header key=value` | 可重复 | 消息头（可重复指定多个） |
| `--dry-run` | — | 仅 CLI 批量模式有；单条无 |

```bash
kp produce --topic orders --key "user-42" --value '{"amount":99}' --header src=cli --profile prod
```

**批量模式（JS 脚本）：** 加 `--script <file.js>` 即进入批量。
| flag | 默认 | 说明 |
|------|------|------|
| `--script` | — | 本地 `.js` 文件路径（批量模式必须） |
| `--count` | 100 | **总消息条数**（生成器被调用直到累计产出 N 条即停） |
| `--concurrency` | 8 | 并发（1–32） |
| `--rate` | 0 | 限速 msgs/sec（0=不限） |
| `--dry-run` | false | 仅预览不发送，返回前 5 条样例 + `generateCalls` / `messagesBuilt` |

脚本约定（goja JS 沙箱）：
- **必须导出** `function generate(ctx) { ... }`（返回**单条消息对象** / **消息数组** / `null` 表示结束流）。
- `ctx` 提供的变量（**有且仅有这 5 个**，没有 `now()` / `topic` / `count` 等）：
  - `ctx.index`：当前调用序号（int，从 0 开始）
  - `ctx.time`：当前毫秒时间戳（`time.Now().UnixMilli()`）
  - `ctx.random()`：返回 `[0,1)` 浮点随机数（每次调用独立取样）
  - `ctx.uuid()`：返回 v4 UUID 字符串
  - `ctx.pick(arr)`：从数组 / 对象随机取一个元素
- 返回消息对象字段：`{ key?, value, headers? }`
  - `value` **必须是字符串**（对象请用 `JSON.stringify`，否则会变成 `[object Object]`）
  - `key` 可省略，省略时工具自动补 `kp-auto-<时间戳>-<序号>`
  - `headers` 为 `{k:v}` 对象；**不支持 `partition`（分区永远由 key 判定）**
- 脚本**无需自己写循环**，`--count` 决定调用次数；一次 `generate` 返回数组即可一次造多条。
- 沙箱**无 IO**：无 `console` / `require` / `import` / `export` / `fetch` / `setTimeout` / `process`；想在脚本里看产出请用 `--dry-run`。
- 编译与每次 `generate(ctx)` 调用各享 **5s** 预算，超时经 `vm.Interrupt` 中断并返回明确错误；已写入的消息不回滚。

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

**返回字段口径：**
- **单条模式**返回 `{ "topic", "partition", "offset", "produced": 1 }`（produced 恒为 1）。
- **批量模式**返回 `BatchResult`：
  - `produced` / `failed`：成功 / 失败条数；`cancelled`：是否被限速或中断取消。
  - `tps`：实际吞吐（条/秒）；`elapsedMs`：耗时（毫秒）。
  - `generateCalls`：脚本 `generate(ctx)` 被调用的次数；`messagesBuilt`：实际构建出的消息数（`generate` 返回数组可一次造多条，故可能大于 `generateCalls`）。
  - `dryRun`：`true` 表示仅预览未发送；`samples[]`：预览样本（含 key、value、headers、partition）。

### 5.7 `session` — 审计会话
| 子命令 | 说明 | flag |
|--------|------|------|
| `kp session new` | 创建会话，返回 6 位短 `sessionId`（如 `K3M9XT`） | `--label` |
| `kp session end <id>` | 结束会话 | — |
| `kp session list` | 列出会话 | — |
| `kp session show <id>` | 查看会话详情 + 其下操作 | — |

> 会话的「新建 / 结束」是 **CLI 专属**能力（供 AI Agent 在任务前后开/收会话），Web UI 不提供对应入口，无需为“一致”而补 REST 端点。

### 5.8 `audit` — 操作审计
| 子命令 | 说明 | flag |
|--------|------|------|
| `kp audit list` | 列出操作 | `--session` `--source ai|human` `--since`(RFC3339) |
| `kp audit show <op-id>` | 操作详情 | — |
| `kp audit snapshot <op-id>` | 查看该操作的有界命中快照（与 list/show 互补，回看当时扫描到的消息样本） | — |
| `kp audit delete <op-id...>` | 删除指定操作（可一次多个） | — |
| `kp audit clear` | 清空审计（**含快照**） | `--before`(RFC3339) |

审计会记录每条 `scan`/`produce` 的调用者来源（ai/human）、意图（`--intent`）、参数摘要、结果。
> ⚠️ **作为 Agent 不要主动执行 `kp audit clear`**——它会抹除操作留痕。仅在用户明确要求时才执行。

### 5.9 `schema` — 命令自描述
`kp schema` 输出 JSON：所有命令名 + 退出码映射。Agent 可直接解析它来确认支持的命令与退出码，无需硬编码。

### 5.10 服务生命周期：`serve` / `desktop` / `stop` / `restart`
| 命令 | 说明 |
|------|------|
| `kp serve --port <n>` | 启动本地 HTTP 服务；`--port 0` 自动选空闲端口并打印 URL |
| `kp desktop` | 自动端口 + 自动打开浏览器 |
| `kp stop` | 读 `kp.pid` 杀掉运行中的服务并删 pid 文件 |
| `kp restart` | 停旧进程后**以相同端口**重启，配置重载（不依赖 Kafka 连接） |

- 服务**只监听 `127.0.0.1`**（本机），远程访问不到。
- `stop`/`restart` 通过 **kp.exe 同目录**的 `kp.pid` 定位实例（记录 `pid`/`port`/`started_at`），**不接收端口参数**。pid 文件与数据目录解耦，改了 `data_dir` 后仍能停止服务；读取时还会兼容查找旧版本写在数据目录 / `%APPDATA%` 下的 pid 文件。
- **修改数据目录后必须 `kp restart`**：服务只在启动时解析一次目录并持有 SQLite 连接；`GET /api/settings` 的 `active_data_dir`（生效目录）与 `pending_change=true` 表示配置已改但尚未重启。
- ⚠️ **常驻注意**：服务是前台长驻进程。若用 `./kp serve &` 在普通 shell 里起，shell 退出会被回收，表现就是“服务莫名其妙没了”。需要长期常驻时，请用工具/系统的后台任务机制拉起。

### 5.11 `settings` — 数据目录配置与一次性迁移（FR-16）

| 命令 | 说明 |
|------|------|
| `kp settings show` | 输出当前布局（JSON）：`data_dir` / `default_data_dir` / `legacy_dir` / `settings_path` / `config_path` / `audit_path` / `*_exists` / `migrated_from` / `migrated_at` / `migration_done` / `env_override` / `restart_required_on_change` |
| `kp settings set-data-dir <path> [--copy-current=false]` | 修改 `data_dir`；相对路径基于 exe 目录，绝对路径原样；默认把当前目录的 `config.yaml` + audit 三件套复制过去 |
| `kp settings migrate [--force]` | 从旧位置一次性复制数据；`--force` 忽略 `migration_done` 标记复查（仍不覆盖） |

- **纯本地管理命令**：不连 Kafka、不需要服务运行、不写审计记录；因此不需要 `--session` / `--intent`，也不要给它加 `--profile`。
- **优先级**：`KP_APPDATA` > `kp-settings.yaml` 的 `data_dir` > 默认 `<exe 目录>/kp-data`。设置 `KP_APPDATA` 时源=目标，自动迁移不触发。
- **迁移安全规则**（CLI / REST / UI 一致）：只复制不删除；目标已有文件绝不覆盖；`audit.db` + `audit.db-wal` + `audit.db-shm` 整组复制；`migration_done` 保证只做一次。建议先 `kp stop` 再迁移 / 切目录，避免 WAL 复制少最后几条记录。
- **重启语义**：改目录 / 迁移后必须 `kp restart`；运行中服务的目录不变。`GET /api/settings` 额外返回 `active_data_dir` 与 `pending_change`。
- **输出字段**：`set-data-dir` / `migrate` 返回 `status` / `data_dir` / `configured` / `previous_data_dir` / `settings_path` / `copied[]` / `skipped[]` / `already_done` / `restart_required` / `hint` / `note`（部分字段仅对应命令出现）。**`copied` / `skipped` 恒为数组（可能为空），不是 null**。
- **REST 对称**：`GET /api/settings`、`PUT /api/settings`（`{"data_dir":"...","copy_current":true}`）、`POST /api/settings/migrate`（`{"force":false}`）。
- **排查**：profile 列表为空 → 先 `kp settings show`；`env_override` 非空说明被 `KP_APPDATA` 覆盖；`pending_change` 为真说明需要 `kp restart`。

```bash
kp settings show
kp settings set-data-dir kp-data
kp settings set-data-dir D:/kafka-pilot-data --copy-current=false
kp settings migrate
```

---

## 6. 退出码（供 Agent 判错）

| 码 | 含义 | 典型原因 |
|----|------|----------|
| 0 | 成功 | — |
| 2 | 参数错误 | 缺必填 flag、格式错误（`--since` 非 RFC3339）、offset 范围非法、`--last > 10000` |
| 3 | 连接错误 | Kafka 不可达、SASL/TLS 配置错误、认证失败 |
| 4 | 超时 | 扫描/生产超过 `--timeout` |
| 5 | 内部错误 | 序列化失败、未预期异常 |

错误输出是 JSON 到 stderr：`{"code":N,"message":"...","hint":"..."}`（部分含 `hint` 排查建议）。

---

## 7. 典型工作流

**场景 A：确认数据有没有写入某个 topic、落在哪**
```bash
kp profile use prod                              # 单 profile 可省；多 profile 先选好
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

**场景 D：启动本地服务**
```bash
kp serve --port 18080        # 启动本地服务（仅监听 127.0.0.1）
```

**场景 E：确认数据目录 / 迁移旧数据 / 切换目录**
```bash
kp settings show                       # 先看生效目录与迁移标记
kp stop                                # 迁移 / 切目录前先停服务
kp settings migrate                    # 从旧 %APPDATA% 一次性复制（源保留）
kp settings set-data-dir D:/kp-data    # 或改到其它目录（默认复制现有数据）
kp restart                             # 让服务切到新目录
```

---

## 8. REST 端点速查（`kp serve` 时，仅 127.0.0.1）

| 端点 | 用途 |
|------|------|
| `GET /api/health`、`GET /api/schema` | 健康检查 / 命令契约 |
| `GET/POST/PUT/DELETE /api/profiles[/...]` | profile 管理 |
| `GET /api/topics[/{topic}]`、`GET /api/key-partition` | topic 浏览 / key 落点 |
| `POST /api/scan`、`POST /api/produce`、`POST /api/produce/batch` | 查询 / 注入 |
| `GET /api/groups[/{group}/lag]` | 消费组 lag |
| `GET /api/sessions[/{id}]`、`DELETE /api/sessions` | 会话（新建 / 结束为 CLI 专属） |
| `GET /api/audit/ops[/{op_id}][/snapshot]`、`DELETE /api/audit[/ops]`、`GET /api/audit/stream` | 审计 / SSE |
| `GET /api/settings`、`PUT /api/settings`、`POST /api/settings/migrate` | 数据目录布局 / 修改 / 一次性迁移 |

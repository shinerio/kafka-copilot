# agents.md — Kafka Pilot 开发指南

> 面向**开发者 / AI 协作者**的项目级开发指南。描述三大模块的职责边界、目录结构、构建与调试方式，以及「开发时必须同步更新技术文档与 skill」的纪律。
> 普通使用者（人类 / AI Agent）请直接看 `README.md` 与 `skill/SKILL.md`，本文不重复命令用法。

---

## 1. 项目概览

Kafka Pilot（`kp`）是一个**单二进制**的 Kafka 数据工具：查数、造数、只读观测，CLI 与本地 Web UI 双端共用同一套内核。

| 项 | 说明 |
|---|---|
| 语言 | Go（内核 + CLI）+ TypeScript/React（前端），前端经 `embed.FS` 打进同一个二进制 |
| 分发物 | 始终只有一个 `kp`（或 `kp.exe`） |
| 目标环境 | 开发 / 测试环境自用，弱权限、重效率；服务只监听 `127.0.0.1` |
| 设计基线 | `docs/kafka-pilot-设计文档.md`（含变更记录与 FR 编号），架构图在 `docs/figures/`（经 drawio MCP 维护） |

**三大模块（本文核心）**

| 模块 | 位置 | 职责 |
|---|---|---|
| **内核（Core）** | `internal/` | 全部 Kafka 能力：连接、扫描、过滤、注入、审计、配置。不含任何界面 / 协议解析逻辑 |
| **CLI 接入层** | `cmd/kp/` | cobra 命令解析、JSON 输出、稳定退出码。只做参数映射与渲染，**不碰业务逻辑、不碰 Kafka 凭证** |
| **前端 UI** | `web/` | React 前端，仅通过本地 `127.0.0.1` 的 REST/SSE 调用后端，**不直连 Kafka** |

> **双端一致性的落点**：`internal/app` 是 CLI 与 Web 服务**唯一**的用例入口。AI 走 CLI 得到的结果，与人在 Web 点同样操作得到的结果，来自同一段代码、产出同样的审计记录——一致性靠结构保证，不靠约定。

---

## 2. 模块职责与依赖方向

### 2.1 内核模块 `internal/`

| 包 | 职责 | 关键文件 |
|---|---|---|
| `config` | profile 增删改查；**数据目录解析与迁移**（默认 `kp.exe` 同目录 `kp-data`，`kp-settings.yaml` 可配置、`KP_APPDATA` 覆盖，一次性从旧 `%APPDATA%` 迁移）；凭据加解密（Windows DPAPI）；**Kafka 凭证仅在此模块内解密** | `config.go`、`settings.go`（含 `settings_test.go`）、`dpapi_windows.go` / `dpapi_other.go` |
| `kafka` | 连接池（按 profile 缓存 `kgo.Client`，支持 SASL / 单向 TLS 组合、多 broker 发现）、元数据、offset 定位、lag 计算；`KeyPartition` 用 murmur2 复刻默认分区器 | `client.go`、`keypartition.go`（含单测） |
| `scan` | 分区并行读取（direct assign）、时间 / 条数边界计算、跨分区归并排序、截断判定、快照边界 | `scanner.go` |
| `filter` | 三层过滤：key → keyword → jq 表达式；`Decoder` 插件接口（内置 `text` / `json`） | `filter.go`、`keyword.go` |
| `produce` | 单条注入 + 脚本生成器（goja 沙箱）+ 背压管道 + 限速 | `producer.go`、`batch.go`、`types.go` |
| `audit` | op / session 摘要与**有界命中快照**入 SQLite（WAL）；双阶段落库、崩溃恢复、保留策略清理 | `store.go` |

### 2.2 CLI 接入层 `cmd/kp/`

- `main.go`：cobra 根命令、全局 flag（`--profile` / `--session` / `--intent` / `--format`）、退出码分类、JSON 输出；含 `kp settings`（数据目录管理）。
- `serve.go` / `service.go`：服务生命周期（`serve` / `desktop` / `stop` / `restart`），PID 文件管理（`kp.pid` 放 exe 同目录，兼容查找旧位置）。

每个命令只做三件事：**解析参数 → 组装 `app.*Request` → 调 `internal/app` 方法 → 输出 JSON**。任何业务逻辑都不得在 CLI 层实现。例外：`settings` / `stop` / `restart` 是纯本地管理命令，直接调 `internal/config`（不经 `app`、不写审计、不连 Kafka），以 `PersistentPreRunE` no-op 跳过 `app.New()`。

### 2.3 前端 UI `web/`

React 19 + TypeScript + Vite + **Fluent UI React v9**。页面（`web/src/pages/`）：

| 页面 | 对应能力 |
|---|---|
| `Topics.tsx` | topic 浏览（极简：名称 / 分区数 / 最新 offset） |
| `Scan.tsx` | 查询构建器（定位维度 + 三层过滤 + 按 key 定位分区开关 + 结果分区筛选） |
| `Produce.tsx` | 单条 / 脚本批量注入（粘贴式 JS 编辑器 + dry-run + 进度） |
| `Groups.tsx` | consumer group lag 只读观测 |
| `Observe.tsx` | AI 操作审计（只读：会话 → 时间线 → op 详情 + 快照重放） |
| `Settings.tsx` | **数据目录卡**（查看 / 修改 / 复制 / 一次性迁移 / 重启提示）、profile 管理（含 SASL / TLS 表单）、保留策略、外观 |

前端所有数据均来自 `web/src/api.ts` 调用的本地 REST/SSE 端点（见 §3）。

---

## 3. 目录结构与关键文件

```
kafka-pilot/
├── cmd/kp/                 # CLI 接入层
│   ├── main.go             #   cobra 根、全局 flag、退出码、JSON 输出
│   ├── serve.go            #   serve / desktop 启动
│   └── service.go         #   stop / restart + PID 文件
├── kp.exe                  # 单二进制（构建产物）
├── kp-settings.yaml        # 固定：data_dir / 迁移标记（运行后生成，不进版本库）
├── kp.pid                  # 固定：服务 PID + 端口（运行后生成）
├── kp-data/                # 默认数据目录（运行后生成）：config.yaml / audit.db / cache.db
├── internal/
│   ├── app/app.go          # 用例层（Scan/Produce/Inspect/Audit/Session/Profile）+ 审计埋点
│   ├── config/             # profile、凭据、数据目录（settings.go）与一次性迁移
│   ├── kafka/              # 连接池、元数据、分区哈希
│   ├── scan/               # 扫描与归并
│   ├── filter/             # 三层过滤 + Decoder 接口
│   ├── produce/            # 注入 + 脚本沙箱 + 背压
│   ├── audit/              # SQLite 审计与快照
│   └── serve/              # 本地 HTTP/SSE 服务（//go:embed dist/*）
│       ├── server.go       #   路由表（/api/*）+ SPA 静态托管
│       └── dist/           #   前端构建产物（由 web/ 构建生成，勿手改）
├── web/                    # 前端 UI（React + Vite）
│   ├── src/pages/          #   各功能页
│   ├── src/api.ts          #   本地 REST/SSE 客户端
│   ├── vite.config.ts      #   outDir: ../internal/serve/dist
│   └── package.json
├── docs/                   # 技术文档（设计文档 + 架构图 PNG/drawio）
├── skill/SKILL.md          # AI Agent 使用 kp 的契约（必须随能力同步更新）
└── README.md               # 用户手册（命令参考）
```

**HTTP 端点（后端 `internal/serve/server.go`）**

```
GET  /api/health
GET/POST/PUT/DELETE /api/profiles[/...]       # profile 管理（含 /default、/{name}、PUT 更新已有 profile）
GET  /api/topics  GET /api/topics/{topic}
GET  /api/key-partition                          # 仅算 key 落点，不扫描
POST /api/scan                                   # 支持 last / 时间窗 / offset_from-offset_to / key / key_regex / keyword / expr / ignore_case / key_partition
POST /api/produce  POST /api/produce/batch
GET  /api/groups  GET /api/groups/{group}/lag
GET  /api/sessions[/{id}]  DELETE /api/sessions  # 浏览 / 详情 / 批量删除（会话「新建 / 结束」为 CLI 专属，见 §5.9）
GET  /api/audit/ops[/{op_id}][/snapshot]  DELETE /api/audit/ops  DELETE /api/audit
GET  /api/audit/stream                         # SSE 实时推送
GET  /api/schema                               # 机器可读：命令清单 + 退出码（对应 CLI 的 kp schema）
GET  /api/settings                             # 数据目录布局（与 kp settings show 同结构 + active_data_dir / pending_change）
PUT  /api/settings                             # 改数据目录（body: data_dir / copy_current，默认复制现有数据）
POST /api/settings/migrate                     # 一次性从旧 %APPDATA% 迁移（body: force，可选）
```

---

## 4. 开发前置与构建

### 4.1 依赖

- **Go**：以 `go.mod` 声明版本为准（当前 `go 1.26.0`）。`README.md` 写「1.22+」为早期文档，请以 `go.mod` 为准。
- **Node**：构建前端需要 Node 20+（Vite 8 / React 19）。
- 无需 CGO：`franz-go`、`modernc.org/sqlite`、`goja`、`gojq` 均为纯 Go，保证单二进制交叉编译。

### 4.2 构建流程（关键：先前端、后后端）

前端构建产物直接输出到 `internal/serve/dist`，再被 `go:embed` 打进二进制。**任何前端改动都必须先 `npm run build`，否则 `go build` 嵌入的是旧 `dist`。**

```bash
# 1) 构建前端 → 产出 internal/serve/dist
cd web
npm install
npm run build

# 2) 构建二进制（embed.FS 把 dist 打进去）
cd ..
go build -o kp.exe ./cmd/kp        # Windows
# go build -o kp ./cmd/kp          # Linux / macOS
```

> 只用 `go build` 而不先 build 前端，看到的会是上一次的前端产物——这是最常见的「改动看不到」原因。

### 4.3 开发期热更新（前端）

```bash
cd web && npm run dev     # Vite dev server，改动即时生效，走内存、不经过 Go embed
```

后端（Go）改动仍需重新 `go build` 并 `kp restart`。

### 4.4 运行 / 调试

```bash
# 默认数据目录就是 kp.exe 同目录的 kp-data/，通常无需任何环境变量
kp settings show          # 查看当前布局与迁移状态

# 用独立目录做测试（覆盖 kp-settings.yaml 的配置，且不触发旧目录迁移）
export KP_APPDATA=D:/workspace/proj/kafka-pilot/tmp-test-data   # 任意临时目录，按需创建

kp serve --port 18080     # 启动服务，浏览器开 http://127.0.0.1:18080
kp desktop                # 自动端口 + 自动开浏览器
kp restart                # 手改 config.yaml / 改数据目录 / 完成迁移后重载（API/UI 保存 profile 即时生效）
kp stop                   # 停服务
```

> 数据目录：`kp-settings.yaml` 固定放 exe 旁；改目录后需 `kp restart`；一次性迁移由 `config.DataDir()` 自动完成（或显式 `kp settings migrate`），源文件保留不删除。详见 `README.md` §2 与设计文档 FR-16。

> ⚠️ `kp serve` 是**前台常驻进程**。在普通交互式 shell 用 `./kp serve &` 起，shell 退出进程会被回收，表现为「服务莫名其妙没了」。长期运行请用后台任务机制拉起。

### 4.5 测试

```bash
go test ./internal/...                 # 内核单测；keypartition_test.go（murmur2 落点）、config/settings_test.go（数据目录 / 迁移 / SQLite 整组复制）
cd web && npm run lint                 # oxlint 前端静态检查
```

---

## 5. 改代码前必读的关键约定

1. **双端一致性**：新能力必须同时在 `internal/app`（用例层）+ CLI + REST 落地；前端只是调 API，不要在前后端各写一套逻辑。
2. **审计埋点**：每个 op 都必须经 `app.NewOpContext`（插入 `running` 行）→ 业务 → `app.FinishOp`（更新终态）。新增能力时漏埋点会导致该操作在观测区不可见。
3. **凭据隔离**：SASL 密码只在 `config` 模块解密使用；CLI / Web 层不持有明文，输出一律脱敏 `****`。
4. **扫描语义**：`--last N` 是**每分区**回扫 N 条原始消息；命中数 ≤ 分区数 × N，**各分区命中全部返回，不再全局截断**；CLI 上限 10000，单次结果安全上限 50000。
5. **单条注入**：`--key` 必填，**分区由 key 一致性 hash 决定，禁止手动指定分区**（RK-04）。
6. **脚本沙箱（goja）**：`ctx` 仅 `index / time / random() / uuid() / pick()` 五个成员；无 `console` / `require` / `fetch` / `setTimeout`；编译与每次 `generate(ctx)` 调用各享 5s 预算；`value` **必须是字符串**（对象需用 `JSON.stringify`）。
7. **本地服务无认证鉴权**：这是定位（本地单机工具），但 Kafka 连接的 SASL / TLS 仍按 profile 配置。
8. **架构图经 drawio MCP 维护**：`docs/figures/1~5` 与 `kafka-pilot-架构图.drawio` 的读取 / 修改必须走 drawio MCP，禁止手改 XML；改动后重新导出 PNG。
9. **明确「CLI 专属」能力，不算双端缺口**：以下能力刻意只走 CLI，不进 REST / Web UI，**不要为了「一致」而补 REST 端点**，也不要在 Web UI 加对应入口：
   - 会话的「新建 / 结束」（`kp session new` / `kp session end`）—— 供 AI Agent 在任务开始前开会话、结束时收尾，人类在 Web 观测区无需此操作。
   - 服务生命周期（`kp serve` / `desktop` / `stop` / `restart`）—— 进程管理，REST 服务本身就是被管理对象。
   - 其余能力（profile / topic / group / scan / produce / audit / key-partition / settings 等）必须 **CLI 与 REST 完全对称**：任一侧有的能力，另一侧也要有。
10. **数据目录与 PID 文件（FR-16）**：
    - 数据目录默认 `<exe 目录>/kp-data`，由 `kp-settings.yaml` 的 `data_dir` 配置（相对路径基于 exe 目录），`KP_APPDATA` 仅作测试覆盖；`kp-settings.yaml` 与 `kp.pid` 固定放 exe 旁，**不要放进数据目录**。
    - 迁移 / 切换目录必须走 `internal/config` 的 `MigrateData` / `SetDataDir` / `CopyDataDir`：只复制、不删除、不覆盖，`audit.db` + `-wal` + `-shm` 整组处理；`migration_done` 保证一次性。
    - 服务只在启动时解析目录，改目录 / 迁移后必须 `kp restart`；`GET /api/settings` 用 `active_data_dir` / `pending_change` 区分生效值与配置值。
    - `settings` 命令与端点是纯本地文件管理，**不写审计埋点**（不经 `NewOpContext` / `FinishOp`），但仍须 CLI + REST 对称。

---

## 6. 新增 / 修改能力的工作流（Checklist）

1. `internal/` 内实现内核逻辑，并补单测。
2. 在 `internal/app/app.go` 暴露方法，并接入审计埋点（`NewOpContext` / `FinishOp`）。
3. 在 `cmd/kp/` 增加 / 调整 cobra 命令（JSON 输出 + 正确的退出码）。
4. 在 `internal/serve/server.go` 增加 / 调整 REST 路由（若新能力需要前端入口）。
5. 在 `web/src/pages/` 增加 / 调整页面，经 `web/src/api.ts` 调后端。
6. 若新增 / 修改了命令，同步更新 `kp schema` 的逻辑与设计文档中的命令清单。
7. **更新技术文档**：`docs/kafka-pilot-设计文档.md` 对应的 FR + 在「变更记录」追加一行（版本号 / 日期 / 变更摘要）；`README.md` 命令参考同步。
8. **若变更影响了 kp 对用户 / Agent 暴露的能力**（新增 / 移除 / 修改命令、flag、退出码、输出字段、REST 端点、脚本语义），必须同步修订 `skill/SKILL.md`（见 §7）。

---

## 7. 文档与 skill 同步纪律（核心要求）

> 本纪律是本项目开发的第一红线：**开发即要更新文档；能力变化即要更新 skill。**

- **CLI 与 Web（REST）能力必须双向对称（第一红线前的硬约束）**：新增或改动任何「非 CLI 专属」（见 §5.9）的能力时，**必须同时在 `internal/app` + CLI(`cmd/kp`) + REST(`internal/serve`) 三处落地**，并保证三者语义、字段、退出码一致；Web UI 只是 REST 的消费者，自然随之具备同等能力。任何一侧独有的能力都视为缺陷，需补齐，不得任其漂移。
- **技术文档必须随代码同步更新**。功能新增、行为变更、bug 修复落地后，不得只改代码不更文档。
  - 设计文档遵循既有「变更记录」表格，新增一行：`版本号 · 日期 · 变更摘要 · 备注（用户反馈 / 自查）`。
  - 架构图变更走 drawio MCP 重新导出 PNG，保证图文一致。
  - `README.md` 的命令参考（§5）与 CLI 实际行为保持一致。
- **`skill/SKILL.md` 是 AI Agent 使用 kp 的契约**，必须随「kp 暴露的能力」同步更新：
  - 触发时机：只要 Agent 可调用的命令 / flag / 退出码 / 输出字段 / REST 端点 / 脚本 `ctx` 语义发生新增、移除或修改，立即修订 `skill/SKILL.md`。
  - 原因：Agent 基于该 skill 的契约调用 kp；过时的 skill 会导致 Agent 传错参数、误读字段、漏掉能力。
  - 反向兜底：若发现 `skill/SKILL.md` 与设计文档 FR / CLI 实际行为不一致，**以设计文档 FR 与 CLI 实际行为为准，并立刻修正 skill**，不要让 Agent 继续读错契约。
- 建议在 PR / 提交说明里显式标注「已同步更新设计文档 / README / SKILL」。

---

## 8. 常见坑

| 现象 | 根因 | 处理 |
|---|---|---|
| 前端改动看不到 | 没先 `npm run build`，`go build` 嵌了旧 `dist` | 改前端后先 `cd web && npm run build` 再 `go build` |
| 服务莫名消失 | `serve` 前台进程被 shell 退出回收 | 用后台任务拉起，或 `kp desktop` |
| `--last` 误解为结果上限 | 它是**每分区**回扫量，命中 = 分区数 × N | 见 §5.4，设计文档 FR-03/05 |
| 单条注入想指定分区 | 分区由 key 自动判定，不支持手动指定 | 设计文档 FR-06，不要加 `--partition` |
| 脚本里 `console.log` 报未定义 | goja 沙箱未注入 `console` | 用 `kp produce --dry-run` 看前 5 条样例 |
| 扫描结果被「截断」 | 实为 12 分区 × 100 = 1200 条检查、命中 100，并非截断 | 设计文档 FR-05 统计语义 |
| 多 broker / TLS 连通性问题 | mechanism 与服务端不一致，或 SASL_SSL 漏配 `--tls` | 设计文档 FR-13 + README §4 FAQ |
| 改了数据目录但 / 审计记录「不见了」 | 服务只在启动时解析一次目录，未重启；或 `KP_APPDATA` 指向了别的目录 | `kp settings show` 看 `active_data_dir` / `data_dir` / `env_override`，再 `kp restart` |
| 迁移后 audit.db 打不开 / 内容异常 | `audit.db-wal` / `-shm` 没和 `audit.db` 整组复制，混入了不匹配的 WAL | 只走 `config.MigrateData` / `CopyDataDir`；先 `kp stop`，必要时 `kp settings migrate --force` 复查 |

# Kafka Pilot 本地消息缓存详细设计（FR-15）

> 独立详细设计文档。本文是 `docs/kafka-pilot-设计文档.md` 中 **FR-15 本地查询结果缓存** 的唯一详细设计基线；与本文件冲突处，以本文件为准。
> 本轮为**设计阶段交付，不改任何代码**；`internal/cache/cache.go` 与 `cache_test.go` 为既有原型草稿，实现期按本文对齐。

## 0. 文档信息

| 项 | 内容 |
|---|---|
| 文档版本 | v1.0（FR-15 独立详设首版） |
| 日期 | 2026-11-08 |
| 状态 | 设计评审中（5 项关键决策已由用户拍板，见 §1） |
| 上游基线 | `docs/kafka-pilot-设计文档.md` v1.4（FR-15 草稿）、`AGENTS.md` / `agents.md` 开发纪律 |
| 关联原型 | `internal/cache/cache.go`、`internal/cache/cache_test.go`（未接入 scan 链路） |
| 适用范围 | 仅本地查询结果缓存；不改变 `scan` / `produce` / `audit` 的既有语义 |
| 交付边界 | 本轮只产出设计文档；主设计文档仅追加变更记录与 FR-15 摘要索引，其余章节同步、架构图 `figures/6` 补图留待实现期 |

---

## 1. 本轮拍板结论（决策记录）

| # | 决策项 | 用户结论 | 对设计的约束 |
|---|---|---|---|
| DC-1 | 交付形态 | **只出独立详设文档**；主设计文档仅追加一行变更记录与 FR-15 摘要，其余章节留待实现阶段同步 | 本文自成体系，须完整覆盖数据模型、协议、契约、验收；不重写主文档 §7~§18 |
| DC-2 | 空间上限 | **用户可自定义，默认 500 MB**（本文统一为 500 MiB = 524,288,000 B） | `max_bytes` 为可配置项；条目上限 `max_entries` 同样可配，默认 2,000,000 |
| DC-3 | 上限口径 | **逻辑载荷预算 + 物理兜底** | `max_bytes` 约束 `Σ(len(key)+len(value)+Σ(len(hdrKey)+len(hdrVal)))`；物理占用单独展示，另设物理硬兜底（§6.4） |
| DC-4 | 默认开启策略 | **默认开 + 文件权限 0600 + 一键清除** | 首次安装即有加速收益；必须在设置页与 CLI 提供即时清除并回收磁盘 |
| DC-5 | 正确性边界 | **严格模式**：仅「非压缩 topic + Topic ID 可用 + 集群指纹一致」才缓存，否则自动回退直连并给出 warning | 压缩 topic 首版明确不支持缓存；Topic ID / 清理策略不可得时保守不缓存（§3） |

**本轮设计默认采用的工程决策**（非用户新拍板，如需调整请指出）：

| # | 决策 | 理由 |
|---|---|---|
| DE-1 | 缓存管理能力按项目第一红线**双端对称**：CLI `kp cache stats\|limits\|clear` ↔ REST `GET /api/cache`、`PUT /api/cache/config`、`DELETE /api/cache` | 缓存管理不属于「CLI 专属」白名单（会话新建/结束、服务生命周期），必须对称 |
| DE-2 | `kp scan --no-cache` ↔ `POST /api/scan` 的 `no_cache`，语义为「本次既不读也不写缓存」 | 与 DE-1 同理 |
| DE-3 | 结果新增 `served_from_cache`（本次是否至少 1 条被检查消息来自缓存）与 `cache_stats`；**`total` / `matched` / `partition_ranges` / `truncated` 口径完全不变** | 「零语义破坏」：与同刻直连逐项等价，唯一允许变化的是 `elapsedMs` |
| DE-4 | 覆盖区间改用显式 `cache_segments` 表，替代草稿中「单一 `[cachedStart, cachedEnd)` 前缀」 | 按分区筛选 / 历史 offset 回填会产生区间空洞，单一前缀无法表达「已确认读过的空区间」 |
| DE-5 | 淘汰后**必须收缩/重建受影响分区的 segments**，保持「segments 覆盖 ⇒ 数据一定可取」不变式 |  prototype 中 `enforce()` 只删 `cache_messages`，不维护区间，淘汰后会返回缺失数据 |
| DE-6 | `cache_meta` 计数不再常驻进程内存，全部走事务内 SQL 原子增减；`Open()` 时做一次计数校正 | CLI 与 `kp serve` 双进程共享 `cache.db`，内存副本会漂移导致 `enforce()` 失效 |
| DE-7 | 存储路径按**代码现状**修正为数据目录下的 `cache.db`（默认 `<exe>/kp-data/`，`KP_APPDATA` 可覆盖），而非主文档 §11.1 旧写的 `%APPDATA%/kafka-pilot/` | 主文档与 `config/settings.go` 已漂移，详设不得沿用失效路径 |
| DE-8 | 缓存层任何错误一律 **fail-open 到直连**（结果照常返回 + `warnings:["cache-degraded"]`） | 只读加速层不得成为可用性瓶颈 |
| DE-9 | 本轮不动 drawio 画布；`figures/6 缓存层位置图 / 对账时序图` 标注为待补，遵守「图表必须经 drawio MCP 维护」纪律 | 用户指定本轮只出设计文档 |

---

## 2. 背景、目标与非目标

### 2.1 现状问题

现有查询链路（主文档 §12.1）：每次 `scan` 都经 `scan.Scanner` → `client.PollFetches()` 从 broker 拉取目标窗口全量消息。Web UI 五档回扫（100/500/1000/5000/10000）意味着同一批消息在反复点查时被重复传输，12 分区 × 10000 档位单次最多检查 12 万条（受 50000 结果安全上限约束），重复查询带来带宽浪费与大对象内存抖动。

审计已有「有界命中快照」（每 op ≤100 条 / ≤1MB）用于**回放**，但它不缓存原始窗口、也无法加速下一次查询，二者职责不同、不可互相替代。

### 2.2 设计目标

| 目标 | 口径 | 验收指向 |
|---|---|---|
| 有界 | 双上限 + 物理兜底，磁盘占用绝不无限增长 | §12 G3/G4 |
| 正确 | 缓存结果 = 同刻直连 broker 扫描结果（逐项等价） | §12 G2/G9/G11 |
| 零语义破坏 | 三种扫描模式、`total` 统计、截断、超时、审计字段行为不变 | §12 G11 |
| 可观测可清除 | 占用 / 命中 / 淘汰 / 降级原因可查，一键彻底清除 | §12 G14/G15 |
| 不降低可用性 | 缓存故障、不支持的 topic、权限不足均自动直连 | §12 G13 |

### 2.3 明确不做（首版）

1. 压缩（compacted）topic 的缓存 —— DC-5 严格模式排除。
2. `cache.db` 内容加密（DPAPI / SQLCipher）—— 首版用 0600 + 数据目录隔离，加密列为实现期评估项（§10）。
3. 跨机器 / 多用户共享缓存、缓存预热、后台自动全量同步 topic —— 超出本地单机工具定位。
4. 缓存 produce 写入（write-through）—— 注入后靠对账补拉新增，不做双写。
5. 缓存 schema / 元数据（topic 列表、group lag）—— 只缓存消息记录本身。
6. 修改 `web/src`、`cmd/kp`、`internal/serve`、`internal/scan` 的现有代码 —— 本轮不实施。

### 2.4 成功指标

| 指标 | 目标 |
|---|---|
| 重复查询命中率 | 同一 topic / 同一窗口、期间无新增时，第二次 `scan` 的 broker 消息拉取条数 = 0（仅元数据 RPC） |
| 结果等价性 | 缓存路径与 `--no-cache` 直连路径的 `messages[]` 逐项 diff = 0 差异 |
| 磁盘有界 | 任意压力场景下 `PhysicalBytes ≤ 1.6 × max_bytes + WAL 常量` |
| 可用性 | 缓存层故障注入下 `scan` 成功率不下降 |

---

## 3. 正确性前提与严格模式守卫

### 3.1 四个前提（主文档 A7~A10 在本文件中闭环）

| # | 原草稿假设 | 本设计结论 | 依据 / 加固 |
|---|---|---|---|
| A7 | offset 单调 + 值不可变 | ✅ **确认，唯一不可妥协的根基** | Kafka 分区日志只追加、消息写入后不可变 ⇒ 同一 `(分区, offset)` 的 `key/value/headers/timestamp` 与同刻 broker 读取逐项等价 |
| A8 | topic 不变 | ⚠️ **修正为「用 Topic ID 锚定，不可得则不缓存」** | 同名 topic 删除重建会让 offset 从 0 重来；Kafka 2.8+ MetadataResponse 带 `TopicID`（UUID）。DC-5：Topic ID 全零 / broker 不支持 / 权限不足 ⇒ 直连 + warning，**不靠 topic 名兜底** |
| A9 | broker 节点不变 | ✅ **弱化，无需追踪 broker 身份** | leader 迁移 / 副本切换不改变逻辑 offset 空间；每次扫描以 `logStart` 与 `HWM` 为锚即可。取而代之的是「集群指纹」（§5.2），防止 profile 改指向另一集群 |
| A10 | 数据只会增删、不会更新 | ❌ **草稿判断不完整，首版收紧** | ① 非压缩 topic 段淘汰 / `DeleteRecords` → `logStart` 前移，可感知、可驱逐；② **压缩 topic 压实会物理删除中间 offset 的旧值，`logStart` 不变，缓存无法感知** → 会返回 broker 已不存在的「幽灵消息」。DC-5：首版排除压缩 topic |

**结论**：物化缓存成立的前提不是「四个假设都成立」，而是「只对能证明成立的作用域启用」。这正是 DC-5 严格模式。

### 3.2 失效场景矩阵

| 场景 | 能否被缓存感知 | 守卫手段 | 结果 |
|---|---|---|---|
| 新消息追加（HWM 前移） | ✅ 能 | 对账补拉 `[cachedEnd, HWM)` | 等价 |
| 段淘汰 / `DeleteRecords`（logStart 前移） | ✅ 能 | `EvictBelow(logStart)` + segments 收缩 | 等价 |
| topic 删除重建 | ✅ 能（Topic ID 变化） | Topic ID 进缓存键；旧 ID 成为孤立分组待 GC | 不串数据 |
| profile 改名 | ✅ 无需感知 | 缓存键用集群指纹而非 profile 名 | 缓存继续有效 |
| profile 改 brokers / SASL 用户 | ✅ 能 | 集群指纹变化 ⇒ 新键 | 不串数据 |
| 分区数增加 | ✅ 能 | 既有分区 offset 不变；新分区独立建段 | 等价（`KeyPartition` 哈希映射变化属既有行为） |
| 压缩 topic 压实删除中间 offset | ❌ **不能** | **首版不缓存**（DescribeConfigs 判 `cleanup.policy`） | 直连，无偏差 |
| 事务控制批次造成的 offset 空洞 | ✅ 能 | segments 记录「区间已完整读取」，而非「每个 offset 都有消息」；`GetRange` 允许稀疏 offset | 等价（见 §5.4 I2） |
| Topic ID 不可用 / 清理策略不可读 | ✅ 能 | 保守判定为不可缓存 | 直连 + warning |
| 同地址集群整体重建且 Topic ID 巧合相同 | ❌ 残余风险 | 见 §13 RK-C06；提供「清除缓存」兜底 | 文档明示 |

### 3.3 严格模式准入判定（每次 scan 执行一次）

按序判定，命中任一条即回退直连；`reason` 写入 `cache_stats.reason`，`warning` 按映射表输出（§9.5）：

```
1. config.cache.enabled == false            → 直连，reason="disabled"，无 warning
2. req.no_cache == true                     → 直连，reason="bypassed"，无 warning（不读不写缓存）
3. 缓存 Store 打开失败 / 上次标记损坏       → 直连，reason="unavailable"，warning=cache-degraded
4. 集群指纹计算失败（profile 无法解析）      → 直连，reason="no-cluster-key"，warning=cache-degraded
5. Metadata 无有效 TopicID                 → 直连，reason="no-topic-id"，warning=cache-unsupported-topic-id
6. DescribeConfigs 失败 / 权限不足          → 直连，reason="policy-unknown"，warning=cache-policy-unknown
7. cleanup.policy 含 "compact"             → 直连，reason="compacted-topic"，warning=cache-compacted-topic
8. 以上均通过                              → 准入，进入对账协议（§7）
```

**元数据成本**：严格模式每次 scan 增加 1 次 `DescribeConfigs`（清理策略）+ 2 次 `ListOffsets`（logStart / HWM，其中时间窗模式另需时间戳定位，属既有成本）。首版**不做 TTL 缓存**（配置可被管理员随时改成 `compact`，缓存 TTL 内会漏判）；后续若要优化，可加 ≤60s TTL 并在 profile 变更时失效，须同步更新本表。

### 3.4 降级原则（DE-8）

准入之后的任何缓存错误（`Append` 失败、SQLite busy、磁盘满、`GetRange` 报错、segments 不一致）**当场放弃缓存路径**，本次查询完整走既有 `scan.Scanner` 直连链路，结果照常返回，`warnings` 追加 `cache-degraded`，`cache_stats.reason="degraded"`，`served_from_cache=false`。缓存层不得抛出异常给调用方、不得让查询失败。

---

## 4. 架构位置与模块边界

### 4.1 分层（文字描述，figures/6 待 drawio MCP 补图）

```
AI Agent (Skill over CLI)  /  人类 (Web UI)
        │                            │
   cmd/kp (cobra)            internal/serve (HTTP/SSE, 127.0.0.1)
        └──────────────┬─────────────┘
                       ▼
              internal/app  ← 唯一用例入口（Scan / Cache / 审计埋点）
                       │
        ┌──────────────┼───────────────┐
        ▼              ▼               ▼
   internal/scan   internal/cache   internal/audit
   (reconcile +    (有界物化视图)   (op / 快照)
    PollFetches)         │
        │                ▼
        └──────►  kgo.Client ──► Kafka 集群
                         │
                         ▼
            数据目录/cache.db (SQLite WAL, 0600)
```

- `cache` 是**内核层新增第七个模块**，与 `scan` 平级，二者只经 `internal/app` 组合；`cache` 不 import `scan`、`app`、`serve`，也不持有 Kafka 客户端与凭证。
- `scan.Scanner` 增加**可选** `Cache` 接口字段（nil = 完全走现有直连路径），保证零侵入、可回退。
- Kafka 凭证仍只在 `config` 模块内解密；集群指纹只取**非敏感**字段（brokers / mechanism / username / TLS 模式与 CA 文件指纹），不含密码明文。
- 依赖方向仍为 `cmd/kp → internal/app → internal/{config,kafka,scan,filter,produce,audit,cache} → 第三方库`；实现期需同步主文档 §7 末尾的依赖方向约束行（当前漏写 `cache`）。

### 4.2 职责切分

| 层 | 新增职责 | 明确不做 |
|---|---|---|
| `internal/cache` | 存储、覆盖区间、双上限淘汰、物理兜底、计数校正、清除与压缩 | 不连 Kafka、不解析过滤条件、不感知 HTTP / CLI |
| `internal/scan` | 对账编排（§7）、缺口补拉、缓存/直连结果合流、过滤重写执行 | 不做淘汰策略、不直接读写 SQLite |
| `internal/app` | 准入编排、配置注入、`Cache` 用例（stats/limits/clear）、审计埋点、结果字段拼装 | 不写业务过滤逻辑 |
| `cmd/kp` | `kp cache` 命令组、`scan --no-cache`、JSON 渲染、退出码 | 不碰缓存文件、不碰 Kafka 凭证 |
| `internal/serve` | `/api/cache*` 路由、`no_cache` 入参透传 | 不实现缓存逻辑 |
| `web/src` | 设置页「本地缓存」卡、Scan 结果缓存命中标记 | 不直连 Kafka / 不直读 cache.db |

### 4.3 审计埋点（项目纪律）

1. `kp cache limits` / `kp cache clear` / `PUT /api/cache/config` / `DELETE /api/cache` 属配置变更类 op，须经 `app.NewOpContext`（插入 `running`）→ 执行 → `app.FinishOp`（终态），`command` 只记参数不含消息体；`source` 按调用方为 `ai` / `human`。
2. `kp cache stats` / `GET /api/cache` 为只读查询，按现有只读端点惯例不新增 op（若实现期决定统一埋点，需 CLI/REST 同时落地）。
3. `scan` 的缓存信息只写进既有 op 的 `result_summary`（`served_from_cache` / `hit_messages` / `fetched_messages` / `reason`）与 `warnings`，**不新增 op、不落任何消息体**。

---

## 5. 数据模型

### 5.1 存储布局（按代码现状修正，DE-7）

```
<exe>/kp-settings.yaml          # 运行设置（现有 Settings.DataDir；本版新增 cache 配置段，§6.5）
<数据目录>/                     # 默认 <exe>/kp-data/，或 KP_APPDATA 指定
├── config.yaml                 # profiles（凭据 DPAPI 加密）
├── audit.db                    # 审计摘要 + 有界命中快照（WAL）
├── cache.db                    # 本版新增：消息缓存（WAL，权限 0600）
├── cache.db-wal / -shm         # SQLite 伴生文件，计入 PhysicalBytes
└── logs/
```

> 主设计文档 §11.1 仍写 `%APPDATA%/kafka-pilot/`，与 `config/settings.go` 的 `DefaultDataDir()` / `MigrateFromLegacy()` 已不一致；实现期同步主文档时须一并修正（§14 待办清单）。

### 5.2 缓存键：集群指纹 + Topic ID

```
cluster_key = hex( sha256(
      join(sorted(bootstrap_servers), ",") + "|" +
      sasl_mechanism + "|" + sasl_username + "|" +
      tls_mode + "|" + ca_file_sha256_prefix
  )[:16] )

cache_key   = (cluster_key, topic_id, partition, offset)
```

| 规则 | 说明 |
|---|---|
| 不含 profile 名 | profile 改名不丢缓存；profile 改 brokers / SASL 用户 ⇒ 指纹变化 ⇒ 新键，旧数据成为孤立分组 |
| 不含 SASL 密码 | 避免密码轮换导致全量缓存失效；密码不参与指纹计算，也不落任何日志 |
| Topic ID 必非空 | DC-5 严格模式：为空即不缓存（不退化成 topic 名） |
| topic 名 | 仅作展示与诊断字段冗余存储，**不参与主键与命中判定** |
| 旧分组 GC | `cache_segments.last_hit_at` 连续 7 天未命中的 `(cluster_key, topic_id)` 分组，由 `kp serve` 启动时的每日清理任务回收；双上限淘汰仍为主要回收手段 |

### 5.3 表结构

**`cache_messages`** —— 一条消息的原始字节镜像

| 字段 | 类型 | 说明 |
|---|---|---|
| cluster_key | TEXT NOT NULL | §5.2 集群指纹 |
| topic_id | TEXT NOT NULL | Topic ID（UUID 字符串），严格模式下必非空 |
| topic | TEXT NOT NULL | 仅展示 / 诊断 |
| partition | INTEGER NOT NULL | |
| offset | INTEGER NOT NULL | |
| ts | INTEGER NOT NULL | 消息时间戳（UnixMilli），与直连 `filter.Result` 一致 |
| key / value / headers | BLOB | 原始字节；headers 以 `map[string][]byte` JSON 序列化后存 BLOB |
| size | INTEGER NOT NULL | 逻辑载荷字节数（§6.1） |
| **主键** | `(cluster_key, topic_id, partition, offset)` | `INSERT OR IGNORE` 幂等；同 offset 重写为空操作 |

索引：`(cluster_key, topic_id, partition, offset)` 即主键聚簇顺序，天然支持 `GetRange` 与「按最旧 offset 淘汰」；另建 `(cluster_key, topic_id)` 索引服务分组 GC。

> 与原型差异：原型主键为 `(profile, topic, partition, offset)`、`topic_id` 只是普通字段且不参与守卫判定。实现期必须按本表调整，否则 DC-5 无法落地。

**`cache_segments`** —— 已完整读取过的 offset 区间（DE-4）

| 字段 | 类型 | 说明 |
|---|---|---|
| cluster_key / topic_id / partition | TEXT / TEXT / INTEGER | 与 messages 对齐 |
| start_offset / end_offset | INTEGER | 半开区间 `[start, end)`；相邻或重叠在写入时合并 |
| last_hit_at | INTEGER | 最近一次被查询命中时间（UnixMilli），供分组 GC 与后续 LRU 演进 |
| **主键** | `(cluster_key, topic_id, partition, start_offset)` | |

语义：一个 segment 表示「该区间已从 broker 完整读取并镜像」——**不是**「每个 offset 都有一条消息」。事务控制批次、被压实排除前的空洞、保留边界等造成的稀疏 offset 均合法。

**`cache_meta`** —— 全局计数（DE-6）

| 字段 | 类型 | 说明 |
|---|---|---|
| id | INTEGER PK CHECK(id=1) | 单行 |
| total_entries / total_bytes | INTEGER | 逻辑条目数 / 逻辑字节数 |
| total_hits / total_fetches / total_evicted | INTEGER | 累计命中 / 补拉 / 淘汰条数，用于设置页展示 |

所有增减在**写事务内**用 SQL 原子完成（`UPDATE ... SET total_entries = total_entries + ?`），进程内不缓存副本；`Open()` 时执行一次 `reconcileMeta()`（以 `SELECT COUNT(*), COALESCE(SUM(size),0)` 校正），修复崩溃或多进程异常导致的计数漂移。WAL 模式下写事务天然串行化，配合 `busy_timeout(5000)` 即满足 CLI 与 `kp serve` 双进程安全，**不需要「单写者」假设**。

### 5.4 不变式（实现期必须单测覆盖）

| # | 不变式 | 破坏后果 | 保证手段 |
|---|---|---|---|
| I1 | `cache_meta.total_*` == `cache_messages` 实时聚合值 | 淘汰失效、预算被突破 | 事务内原子增减 + `Open()` 校正 |
| I2 | segment 覆盖 `[a,b)` ⇒ 该区间内**所有 broker 可见消息**都在 `cache_messages` 中 | 返回缺失数据（静默漏消息） | `Append` 与 segment 扩展在**同一事务**内提交 |
| I3 | 任何删除（`enforce` / `EvictBelow` / `EvictPartition` / GC / `Clear`）后，受影响的 segment 必须同步收缩或重建 | 淘汰后 segment 仍声称覆盖 ⇒ 返回幽灵缺口 | 删除后对受影响分区执行 `rebuildSegments()`：由现存消息 offset 重新生成连续段（单批 ≤5000，仅针对被删分区） |
| I4 | segments 之间不重叠、不相邻（始终合并） | 缺口计算重复 / 漏算 | 写入时事务内合并 |
| I5 | `total_bytes` ≤ `max_bytes` 且 `total_entries` ≤ `max_entries`（`enforce` 返回后） | 磁盘无界 | §6.3 |

> I3 是原型 `cache.go` 当前缺失的关键一环：其 `enforce()` / `EvictBelow()` 只删 `cache_messages`，若直接接入「单一 cachedStart 前缀」模型，淘汰后 `cachedStart` 不推进，查询会静默丢数据。

---

## 6. 容量上限与淘汰策略

### 6.1 口径定义（DC-2 / DC-3）

| 配置项 | 默认值 | 含义 | 校验 |
|---|---|---|---|
| `cache.enabled` | `true` | 总开关（DC-4） | bool |
| `cache.max_entries` | `2,000,000` | 全局缓存消息**条数**上限 | ≥ 1000；0 → 回退默认值 |
| `cache.max_bytes` | `524,288,000`（500 MiB） | 全局**逻辑载荷**字节上限 | ≥ 16 MiB；0 → 回退默认值 |

- **逻辑载荷** `size = len(key) + len(value) + Σ(len(hdrKey) + len(hdrVal))`，不含 SQLite 页 / 索引 / WAL 开销。
- **物理占用** `PhysicalBytes = stat(cache.db) + stat(cache.db-wal) + stat(cache.db-shm)`，仅用于展示与物理兜底，不作为常态淘汰依据。
- 两者关系：稳态下 `PhysicalBytes ≈ 1.3~1.6 × TotalBytes`（主文档 v1.4 已给出倍率）；设置页必须同时显示「逻辑预算 500 MiB / 当前逻辑 X / 当前物理 Y」并附倍率说明，避免「上限 500MB 就只占 500MB」的误判。
- **首版取消「0 = 不限」语义**：DC-5 的硬约束是「绝不无限占用本地磁盘」，0 一律回退默认值；不开放无限模式（原型注释需实现期修正）。

### 6.2 配置项存储与跨进程生效

- 落在现有 `<exe>/kp-settings.yaml` 的 `cache:` 段，复用 `config.LoadSettings` / `SaveSettings`；CLI 与 `kp serve` 读同一文件。
- `PUT /api/cache/config` / `kp cache limits`：写文件 + 更新 serve 进程内存 + 立即 `SetLimits()` 触发 `enforce()`（无需重启，与 profile 动态更新体验一致）。
- 跨进程一致性：serve 在每次 scan 前按文件 mtime（≥1s 粒度）懒加载配置，保证 CLI 侧改配置后 serve 下次扫描即生效；CLI 每次启动重新读文件。

### 6.3 淘汰策略

`enforce()` 在每次 `Append`、`SetLimits`、`Open`（含校正后）结束时调用，循环直至同时满足 I5：

1. 计算超限量：`over_entries = total_entries - max_entries`，`over_bytes = total_bytes - max_bytes`。
2. 按 `(cluster_key, topic_id, partition, offset ASC)` 顺序删除**最旧**（offset 最小）的条目；单批上限 5000 条，按 `over_entries` 与 `over_bytes` 取需要的批量，循环至两侧均不超限。
3. 保留「最近尾部」——正是 `--last N` 实际命中的区间；被淘汰项随时可从 broker 按需重建，缓存只是加速层，不丢真数据。
4. 每批删除后：事务内更新 `cache_meta`（含 `total_evicted`）→ 提交 → 对受影响分区执行 `rebuildSegments()`（I3）。
5. `SetLimits()` 调小上限时立即 trim 至新预算（验收 G6）。

**已知局限**：全局按 `cluster_key/topic_id` 字典序淘汰，可能优先清空某个字母序靠前、但仍在热用的 topic；首版接受该行为（正确性不受影响，仅命中率），后续可引入 `last_hit_at` 做 LRU 加权（§13 RK-C07）。

### 6.4 物理兜底（DC-3 的「兜底」）

设 `physical_guard = ceil(max_bytes × 1.6)`（默认 800 MiB）。每次 `Append` 提交后检查 `PhysicalBytes`：

1. 超阈值 → 先 `PRAGMA wal_checkpoint(TRUNCATE)` 回收 WAL；
2. 仍超 → 在 `enforce()` 基础上额外淘汰最旧 10% 条目（同样遵守 I1/I3）；
3. 仍超 → 触发 `Compact()`；并在日志与设置页给出 `warnings:["cache-trimmed"]` 提示。

建库时启用 `PRAGMA auto_vacuum=INCREMENTAL`，淘汰后调用 `PRAGMA incremental_vacuum(1024)` 渐进回收页；完整 `VACUUM` 只在 `Clear()` 或「累计删除 > 20% 条目」后执行，避免每次写入触发独占锁。`Compact()` 期间缓存写入串行等待，读路径仍可用；若 `VACUUM` 失败，仅记日志，不降级查询结果。

### 6.5 清除语义（DC-4）

`kp cache clear` / `DELETE /api/cache`：

1. 事务内 `DELETE FROM cache_messages` + `DELETE FROM cache_segments` + `cache_meta` 归零（I1/I3 自动满足）；
2. `PRAGMA wal_checkpoint(TRUNCATE)` + `VACUUM`，回收物理空间；
3. 返回清除前后的 `PhysicalBytes`，供 UI 显示「已释放 X MiB」；
4. 不删除 `cache.db` 文件本身（避免重建时的并发窗口），文件内容已无任何消息字节；
5. 该操作记审计 op，但 `command` 与 `result_summary` 不含消息体。

---

## 7. 对账协议（reconcile）

每次准入的 `scan`，在 `PollFetches` 之前执行；全程受 `req.Timeout`（默认 30s）约束，缓存侧耗时计入 `elapsedMs`。

| 步 | 动作 | 说明 |
|---|---|---|
| S0 | 准入判定 | §3.3；不通过 ⇒ 直接走既有直连路径 |
| S1 | 取元数据 | `ListOffsets(earliest=-2)` 得 `logStart`、`ListOffsets(latest=-1)` 得 `HWM`；`Metadata` 取 `TopicID`；`DescribeConfigs` 取 `cleanup.policy` |
| S2 | 驱逐已消失数据 | 对每个目标分区 `EvictBelow(logStart)`，并按 I3 收缩 segments |
| S3 | 计算目标窗口 | 按既有 `calculateRanges` 语义，三种模式分别见 §8；`partition_ranges` 输出与直连完全一致 |
| S4 | 计算缺口 | 用 `cache_segments` 求 `[from,to)` 的未覆盖子区间集合；已覆盖部分记 `hit_messages` |
| S5 | broker 补缺口 | 对缺口区间复用现有 direct assign + `PollFetches` 拉取（可并行多分区），记 `fetched_messages` |
| S6 | 落缓存 | 缺口数据与新 segments 在**同一事务** `Append` + 合并区间（I2/I4）；随后 `enforce()`（§6.3）并刷新物理兜底（§6.4） |
| S7 | 取全窗口 | `GetRange(from,to)` 取已覆盖部分 + S5 拉取的缺口数据，合成完整窗口的 `filter.Message` 序列 |
| S8 | 既有流水线 | 三层过滤（key → keyword → jq）→ 跨分区归并排序（timestamp desc，tie-break: partition asc, offset asc）→ 50000 安全上限 / 超时截断 → `Result` 组装 |

**关键约束**

- 过滤**在缓存原始字节上重写执行**，绝不缓存「过滤后的结果」——否则过滤条件一变缓存即失效。
- S6 失败（磁盘满 / busy / 文件损坏）不回滚 S7：结果已由 S5 的 broker 数据 + 已覆盖的缓存数据组成，仍正确返回；标记 `cache-degraded`（DE-8）。
- S7 合成时若发现 I2 被破坏（segment 声称覆盖但 `GetRange` 缺行），**立即整体降级直连**并记 `cache-degraded`，同时后台标记该分区 segments 待重建。
- `HWM` 异常回退（低于已缓存 end）：视为异常，对该分区 `EvictPartition` + 全量重拉 + `warnings:["cache-degraded"]`。
- 只扫部分分区（`--partition` / `--key-partition` 开关）时，S2~S6 仅作用于选中分区，其余分区 segments 原样保留，不推进、不驱逐。

**时序（文字版，figures/6 待 drawio MCP 补图）**

```
CLI/UI → app.Scan → 准入判定 ──否──→ 直连路径 ─────────────┐
                       │是                                  │
                       ├─ ListOffsets(logStart/HWM)         │
                       ├─ Metadata(TopicID)                 │
                       ├─ DescribeConfigs(cleanup.policy)   │
                       ├─ EvictBelow(logStart)              │
                       ├─ 目标窗口 → segments 求缺口         │
                       ├─ 缺口 → kgo.PollFetches (broker)    │
                       ├─ Append + 合并 segments + enforce   │
                       ├─ GetRange(全窗口)                   │
                       └─ 任一步失败 ─────→ 直连路径 ───────┤
                                                            ▼
                              三层过滤 → 归并排序 → Result(+缓存字段) → 审计
```

---

## 8. 与既有扫描模式的兼容矩阵

| 模式 | 目标窗口算法（不变） | 缓存行为 | 收益 |
|---|---|---|---|
| `--last N`（每分区回扫 N 条） | `to=HWM`，`from=max(logStart, HWM-N)` | 首次建段；之后只补 `[cachedEnd, HWM)`；窗口完全落入缓存时**零消息传输**，仅元数据 RPC | 最高 |
| `--offset-from / --offset-to` | 精确 offset 区间（与 `--last` 互斥） | 已覆盖段命中，只补缺失段；早于 `cachedStart` 的历史段可回填并合并为独立 segment（DE-4 多区间） | 中 |
| `--since / --until` 时间窗 | 仍需 `ListOffsets(timestamp)` 定位起点；`until` 逐条时间判断 | 起点定位 RPC 保留，数据从缓存出；**逐条时间过滤逻辑必须沿用直连实现**，不得因缓存跳过 | 窗越大收益越明显 |
| `--key-partition`（实验开关） | 只扫 murmur2 落点单分区 | 仅该分区参与对账；结果区既有「本次实际扫描 Pn」标注不变 | 有（但风险同既有） |
| 注入后立刻查询 | — | 下一次对账补拉 `[cachedEnd, HWM)` 新增，新消息可见 | — |

**语义不变清单**（验收 G11 逐项 diff）：`total`（实际检查的原始消息数，含从缓存读出的条数）、`matched` / `matched_raw`、`truncated` / `warnings` 既有码、`messages[]` 字段与排序、`partition_ranges`、`skippedNonUtf8Key`、`key_partition` / `key_partition_count`、审计 `result_summary` 既有字段、超时与安全上限行为。**唯一允许变化的是 `elapsedMs` 与新增字段。**

> 注意：`total` 不因「broker 拉取 0 条」而变成 0 —— 它统计的是被检查的原始消息，缓存读出的条数照样计入，否则会破坏 SKILL.md 已约定的口径。

---

## 9. 对外契约（CLI / REST 双端对称，DE-1 / DE-2）

### 9.1 CLI

```
kp cache stats                                   # 占用 / 命中 / 上限 / 物理占用
kp cache limits [--enabled true|false]
                [--max-entries <n>] [--max-bytes <n>]   # 即时生效，落 kp-settings.yaml
kp cache clear                                   # 清空 + VACUUM，返回释放量

kp scan ... [--no-cache]                         # 本次既不读也不写缓存
```

- 输出均为单对象 JSON（沿用 `--format json|table`）；参数错误 exit 2，缓存文件不可写等内部错误按既有分类映射（连接类 3 / 内部 5），但**缓存故障在 scan 场景一律降级、不改变退出码**。
- `kp schema` 须新增：`cache` 命令组、三个子命令参数、`scan --no-cache`、`served_from_cache` / `cache_stats` 输出字段、新增 warning 码。

### 9.2 REST

```
GET    /api/cache                 # = kp cache stats
PUT    /api/cache/config          # = kp cache limits；body {enabled,max_entries,max_bytes}
DELETE /api/cache                 # = kp cache clear
POST   /api/scan                  # body 新增 no_cache: bool（默认 false）
```

- CLI 与 REST 的字段、语义、错误码、审计埋点必须一一对应（项目第一红线）。
- 设置页改动即时生效，无需 `kp restart`（与 profile 动态更新一致）。

### 9.3 `scan` 结果新增字段

```json
{
  "opId": "op_01H...",
  "topic": "orders",
  "total": 1200,
  "matched": 37,
  "elapsedMs": 84,
  "served_from_cache": true,
  "cache_stats": {
    "enabled": true,
    "eligible": true,
    "reason": "",
    "hit_messages": 1180,
    "fetched_messages": 20,
    "evicted": 0,
    "total_entries": 152340,
    "total_bytes": 48213091,
    "physical_bytes": 61000000,
    "max_entries": 2000000,
    "max_bytes": 524288000
  }
}
```

| 字段 | 类型 | 定义 |
|---|---|---|
| `served_from_cache` | bool | 本次**至少有 1 条被检查消息**来自缓存（`hit_messages > 0`）即为 true；全部来自 broker 或降级直连时为 false。**始终输出**，不省略 |
| `cache_stats` | object | **始终输出**；未准入时仅含 `enabled` / `eligible:false` / `reason`，其余字段省略（不输出 0 值以免误读） |
| `hit_messages` | int | 从缓存读出并被检查的原始消息条数 |
| `fetched_messages` | int | 本次从 broker 补拉的原始消息条数 |
| `evicted` | int | 本次 `enforce()` 淘汰的条数 |
| `total_entries` / `total_bytes` | int | 缓存当前逻辑条目数 / 逻辑字节数 |
| `physical_bytes` | int | `cache.db + -wal + -shm` 实际占用 |
| `max_entries` / `max_bytes` | int | 当前生效上限 |

### 9.4 `kp cache stats` / `GET /api/cache` 响应

```json
{
  "enabled": true,
  "max_entries": 2000000,
  "max_bytes": 524288000,
  "total_entries": 152340,
  "total_bytes": 48213091,
  "physical_bytes": 61000000,
  "physical_guard_bytes": 838860800,
  "total_hits": 98400,
  "total_fetches": 12100,
  "total_evicted": 3200,
  "groups": 7,
  "db_path": "D:/.../kp-data/cache.db"
}
```

`db_path` 为本地绝对路径，仅用于排障展示；不暴露任何 Kafka 凭证。

### 9.5 新增 warning 码与中文映射

| 机器码 | 触发条件 | UI 中文提示 |
|---|---|---|
| `cache-degraded` | 缓存不可用 / 对账失败 / 完整性校验失败 | 「本地缓存暂不可用，本次已直连 Kafka 查询，结果不受影响」 |
| `cache-compacted-topic` | 清理策略含 `compact` | 「该 topic 为压缩主题，为保证结果准确已跳过本地缓存」 |
| `cache-unsupported-topic-id` | broker 不支持 / 未返回 Topic ID | 「Kafka 版本不支持 Topic ID，已跳过本地缓存」 |
| `cache-policy-unknown` | DescribeConfigs 失败 / 权限不足 | 「无法确认主题清理策略，已跳过本地缓存」 |
| `cache-trimmed` | 物理兜底触发额外淘汰 | 「本地缓存已达物理空间兜底阈值，已自动清理最旧数据」 |

既有 warning 码（`scan>1000` / `result-cap` / `timeout` / `key-partition`）行为不变；未知码原样透出（沿用 v1.3c 约定）。

### 9.6 Web UI

| 位置 | 内容 |
|---|---|
| 设置页新增「本地缓存」卡 | 开关（默认开）、最大条目输入、逻辑预算 MB 输入（默认 500）、实时占用（逻辑 / 物理 / 倍率说明）、累计命中 / 补拉 / 淘汰统计、「清除缓存」按钮（二次确认，显示释放量） |
| 同卡常驻安全说明 | 「缓存会在本机保存消息的原始 key / value / headers，与 Kafka 中等同敏感；文件权限 0600，可随时一键清除并回收磁盘」 |
| Scan 结果统计条 | 显示「命中缓存 X 条 / 补拉 Y 条」与 `served_from_cache` 标记；warning 按 §9.5 中文化展示 |
| 不做 | 不在 UI 提供按 topic 手动失效、不展示消息内容预览（避免二次暴露） |

### 9.7 实现期必须同步的文档/契约清单

| 文件 | 同步内容 |
|---|---|
| `docs/kafka-pilot-设计文档.md` | FR-15 正文按本文重写；§7 依赖方向补 `cache`；§9.1 命令清单补 `kp cache` / `--no-cache`；§9.2 输出契约补新字段；§9.3 SKILL 要点；§10 设置页；§11 存储布局 / 表 / 保留策略（含旧路径修正）；§12.1 查询链路；§13 非功能；§14 技术选型；§15 风险；§16 版本规划；§17 RTM；§18 补 A7~A10；变更记录追加版本行 |
| `README.md` | §5 命令参考新增 `kp cache` 与 `--no-cache`；§2 配置目录补 `cache.db`；§7 FAQ 补「缓存会不会占满磁盘 / 如何彻底清除」 |
| `skill/SKILL.md` | 顶层命令清单、§5.4 `scan` flags 与结果字段口径、新增 warning 码、`kp cache` 用法、Never do 增补「不要主动 `kp cache clear`（除非用户明确要求）」 |
| `kp schema` | §9.1 所述机器可读契约 |
| `docs/figures/` | 经 **drawio MCP** 新增「缓存层位置图」与「对账时序图」（figures/6），重新导出 PNG；禁止手改 `.drawio` XML |

---

## 10. 安全与隐私

| 项 | 设计 |
|---|---|
| 落盘内容 | 原始 `key` / `value` / `headers` 字节，与 broker 同等敏感；不含 SASL 密码、不含审计命令明文 |
| 文件权限 | `cache.db` 及伴生文件创建时设 `0600`；数据目录沿用 `kp-data` 隔离（Windows 上继承用户目录 ACL） |
| 与审计的关系 | 审计 `operations` 行仍不落消息体；命中快照仍 ≤100 条 / ≤1MB 且随 30 天策略滚动清理。缓存是**独立**的第二份消息副本，设置页文案须明确提示「本机存在两份数据副本」 |
| 清除 | §6.5：DELETE + checkpoint + VACUUM；不残留消息字节在可复用页中（VACUUM 后文件被重写） |
| 加密 | 首版不做；实现期评估 Windows DPAPI 整库加密（会破坏 WAL / 多进程读，需专项设计），不阻塞首版 |
| 日志 | slog 结构化日志只记计数、reason、耗时、错误类别；**禁止**打印 key / value / headers 内容 |
| 服务边界 | REST 仍只监听 127.0.0.1、无认证鉴权（D14）；`/api/cache*` 不返回任何消息内容，只返回统计与配置 |

---

## 11. 并发、可靠性与异常矩阵

| 场景 | 行为 |
|---|---|
| CLI 与 `kp serve` 同时扫描同一 topic | WAL + 写事务串行化 + `busy_timeout(5000)`；`INSERT OR IGNORE` 幂等；计数走 SQL 原子增减（DE-6），无内存副本漂移 |
| `Open()` 发现历史计数漂移 | `reconcileMeta()` 校正后 `enforce()` |
| `cache.db` 损坏 / 打不开 | 隔离为 `cache.db.corrupt-<ts>`（不自动删除，留待排障）+ 本次直连 + `cache-degraded`；下次启动重建空库 |
| 磁盘满 / SQLite busy 超时 | 结果仍由直连返回 + `cache-degraded`；后台触发 trim 与物理兜底 |
| 进程崩溃 | WAL 保证已提交事务完整；未提交事务回滚；下次 `Open()` 校正计数与 segments（I1/I2） |
| 淘汰与查询并发 | 淘汰只在 `Append` / `SetLimits` 事务内发生；S7 取数在 S6 之后同一请求内，若跨请求并发淘汰命中窗口，I3 保证 segments 已收缩 ⇒ 缺口会被重新计算并补拉，不会静默漏消息 |
| `VACUUM` 期间 | 写串行等待、读不阻塞；失败仅记日志 |
| 配置跨进程生效 | §6.2 mtime 懒加载 |
| 超时 | 缓存路径同样受 `req.Timeout` 约束；补拉超时按既有 `truncated` / `timeout` 语义处理，不新增截断类型 |

---

## 12. 验收标准（GWT）

| # | Given | When | Then |
|---|---|---|---|
| G1 | 默认配置、非压缩 topic、有 Topic ID | 首次 `kp scan --last 1000` | 触发 broker 拉取并写入各分区 segments；结果 `served_from_cache=false`、`fetched_messages>0` |
| G2 | 同上，期间无新增 | 第二次同样查询 | `served_from_cache=true`、`fetched_messages≈0`；`messages[]` 与首次逐项一致（含 timestamp / headers / 排序） |
| G3 | 设 `max_entries=5` | 向同一分区 Append 10 条（offset 0..9） | 存活 5 条且为 offset 5..9，`total_entries=5`，segments 收缩为 `[5,10)` |
| G4 | 设 `max_bytes=8`（单条逻辑 4B） | Append 5 条 | 仅最大 offset 两条存活；`physical_bytes ≥ total_bytes` |
| G5 | 默认 `max_bytes` | 读取生效配置 | `= 524288000`（500 MiB）；设为 0 时回退默认而非「不限」 |
| G6 | 已有 20 条缓存 | `kp cache limits --max-entries 7` | 立即 trim 至 7 条；REST `PUT /api/cache/config` 结果一致；无需重启 |
| G7 | 持续写入直至物理占用超 `physical_guard` | 触发兜底 | 执行 checkpoint + 额外淘汰 + Compact，`physical_bytes` 回落至守卫内，`warnings:["cache-trimmed"]` |
| G8 | `cleanup.policy=compact` 的 topic | `kp scan` | 自动直连，`eligible=false` / `reason="compacted-topic"`，`warnings:["cache-compacted-topic"]`，**不写任何缓存行** |
| G9 | broker 不返回 Topic ID / DescribeConfigs 权限不足 | `kp scan` | 自动直连 + 对应 warning；结果与 `--no-cache` 完全一致 |
| G10 | 同名 topic 重建（新 Topic ID）、profile 改 brokers | 再次扫描 | 两组数据键不同、互不串数据；旧分组待 GC |
| G11 | 三种扫描模式 + `--partition` + `--key-partition` | 分别用缓存路径与 `--no-cache` 执行 | `total` / `matched` / `partition_ranges` / `messages[]` / `truncated` / 既有 warnings 逐项 diff = 0；仅 `elapsedMs` 与新增字段不同 |
| G12 | `logStart` 前移至 N | 下次对账 | offset < N 的缓存被驱逐、segments 收缩，`GetRange` 不再返回；结果与直连一致 |
| G13 | 模拟 `cache.db` 只读 / SQLite busy / 文件损坏 | `kp scan` | 查询成功且结果正确，`warnings` 含 `cache-degraded`，退出码与直连一致 |
| G14 | 任一扫描路径 | `kp cache stats` / `GET /api/cache` | 字段与 §9.4 一致；CLI 与 REST 数值同源 |
| G15 | 已有缓存数据 | `kp cache clear` / `DELETE /api/cache` | `total_entries=0`、`physical_bytes` 显著回落、返回释放量；审计 op 可查且不含消息体 |
| G16 | `kp scan --no-cache` / `no_cache:true` | 执行扫描 | 既不读也不写缓存，`cache_stats` 前后不变，`reason="bypassed"` |
| G17 | 默认安装 | 检查 `cache.db` 权限 | 文件权限 0600，`enabled=true`；设置页可见安全说明与清除按钮 |
| G18 | 准入后的 scan | 检查审计 | `result_summary` 含 `served_from_cache` / `hit_messages` / `fetched_messages` / `reason`；`operations` 行不含任何消息体 |

> 实现期回归要求沿用项目惯例：真实 Kafka（含 12 分区多分区 topic、压缩 topic、Topic ID 可用性差异）+ CLI/REST 双端对照 + Web UI 无头浏览器验证；`go test ./internal/...` 覆盖 I1~I5 不变式。

---

## 13. 风险与遗留问题

| 编号 | 风险 | 等级 | 应对 |
|---|---|---|---|
| RK-C01 | 缓存落盘原始消息带来第二份敏感数据副本 | 中 | 0600 + 数据目录隔离 + 设置页明示 + 一键彻底清除（§10）；加密列为实现期评估项 |
| RK-C02 | 压缩 topic 压实删除中间 offset 导致「幽灵消息」 | 高（已规避） | DC-5 严格模式首版排除；每次 scan 查 `DescribeConfigs`，不做 TTL |
| RK-C03 | 淘汰后 segments 未收缩 → 静默漏消息 | 高（已规避） | I3 + `rebuildSegments()` 单测（G3/G12） |
| RK-C04 | 多进程计数漂移导致预算失效 | 中（已规避） | DE-6：计数事务内原子增减 + `Open()` 校正 |
| RK-C05 | `DescribeConfigs` 增加一次元数据 RPC / 权限不足时大面积不缓存 | 低 | 首版接受；权限不足保守直连并给可读 warning；后续可加 ≤60s TTL（须同步 §3.3） |
| RK-C06 | 同地址集群整体重建且 Topic ID 巧合相同，理论上仍可能串数据 | 低 | 集群指纹 + Topic ID 双重锚定已覆盖绝大多数场景；残余风险写入 README FAQ，提供「清除缓存」兜底 |
| RK-C07 | 全局按 offset 字典序淘汰，可能误伤热 topic 命中率 | 低 | 仅影响性能不影响正确性；后续可用 `last_hit_at` 做 LRU 加权 |
| RK-C08 | 物理兜底 `VACUUM` 在大库上持锁较久 | 低 | `auto_vacuum=INCREMENTAL` 渐进回收；完整 VACUUM 只在 Clear / 大删后；读路径不阻塞 |
| RK-C09 | 缓存字段 / 命令上线后 SKILL.md 未同步，Agent 误读契约 | 中 | §9.7 列为实现期阻断项；按项目纪律「能力变化即更新 skill」验收 |
| RK-C10 | 主设计文档 §11.1 存储路径与代码漂移，读者按错路径找 cache.db | 低 | DE-7 已在本文修正；实现期同步主文档时一并修正 |

---

## 14. 实现落点与顺序（本轮不执行）

| 阶段 | 落点 | 要点 |
|---|---|---|
| P0 | `internal/cache` | 按 §5 重建键与三表、segments 合并 / 重建、计数事务化、`enforce` + 物理兜底、`Compact` / `Clear`、`reconcileMeta`；补 I1~I5 单测 |
| P1 | `internal/config` | `kp-settings.yaml` 增 `cache:` 段；默认值、校验、跨进程 mtime 懒加载 |
| P2 | `internal/scan` | 可选 `Cache` 接口字段；§7 对账编排；失败降级；结果字段回填 |
| P3 | `internal/app` | 准入编排、`Cache` 用例方法、`NewOpContext` / `FinishOp` 埋点、`ScanResult` 新字段 |
| P4 | `cmd/kp` + `internal/serve` | 双端对称的命令组 / 路由 / `no_cache` / `kp schema` |
| P5 | `web/src` | 设置页「本地缓存」卡、Scan 结果缓存标记、warning 中文映射 |
| P6 | 文档与图 | §9.7 全量同步；figures/6 经 drawio MCP 补图并导出 PNG |
| P7 | 验收 | §12 G1~G18；真实 Kafka + CLI/REST 对照 + 无头浏览器；`go test ./internal/...` |

**实现期开工前需再次确认的开放项**（不阻塞本设计定稿）：

1. `cache.db` 是否需要 Windows DPAPI 整库加密（涉及 WAL / 多进程读，需专项设计）。
2. `DescribeConfigs` 在目标开发/测试集群的授权情况（若普遍无权限，需评估「按 topic 白名单显式标记非压缩」的替代准入）。
3. 清理策略 TTL 优化是否纳入首版（当前结论：不做，正确性优先）。
4. 主设计文档其余章节与 figures/6 的同步时点（本轮按 DC-1 留待实现阶段）。
5. `kp cache stats` 是否需要像 `scan` 一样产生审计 op（当前 DE 结论：只读不新增 op）。

---

## 15. 变更记录

| 版本 | 日期 | 变更 | 备注 |
|---|---|---|---|
| v1.0 | 2026-11-08 | FR-15 独立详设首版：闭环 A7~A10、确立严格模式准入（DC-5）、集群指纹 + Topic ID 缓存键、三表模型与 I1~I5 不变式、双上限（逻辑载荷默认 500 MiB 可配 + 物理兜底）、对账协议、CLI/REST 双端契约与结果字段、安全与并发、G1~G18 验收、风险与实施顺序 | 用户拍板 5 项决策（交付形态 / 上限默认值 / 口径 / 默认开启 / 正确性边界）；本轮仅出设计文档，未改任何代码 |

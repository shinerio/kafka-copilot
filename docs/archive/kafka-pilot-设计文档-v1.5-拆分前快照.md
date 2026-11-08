# Kafka Pilot 设计文档

> 面向 AI Agent 与人类双端的 Kafka 数据查看 / 造数工具

## 一、文档信息

| 项 | 内容 |
|---|---|
| 文档版本 | v1.5（新增 FR-16 数据目录可配置 + 一次性迁移：默认 `kp.exe` 同目录 `kp-data`，`kp-settings.yaml` 固定置于 exe 旁，CLI / REST / Web UI 三端对称，SQLite 三件套整组安全迁移；v1.4 的缓存 FR-15 仍为设计稿） |
| 作者 | 司南（产品经理 Agent）× 用户共创 |
| 日期 | 2026-11-07 |
| 状态 | 设计评审中 |

**变更记录**

| 版本 | 日期 | 变更 | 备注 |
|---|---|---|---|
| v1.0 | 2026-11-07 | 初稿 | 基于 3 轮需求澄清（13 项决策）产出 |
| v1.1 | 2026-11-07 | 评审反馈修订：① 审计只存最终结果、不落任何消息体；② CLI / 客户端取消认证鉴权，Kafka 凭证仅内核持有；③ session 改为 CLI 随机生成短 ID；④ jq 为默认解码插件并预留扩展接口；⑤ Windows UI 采用 Fluent Design；⑥ 假设 A1~A6 全部确认闭环 | 用户反馈 7 条，全部闭环 |
| v1.1a | 2026-11-07 | 补充客户端视觉与布局基线（6 个页面要点，见 §10.4；v1.3d 起以业务前端为唯一基线） | 补充客户端视觉与布局基线 |
| v1.1b | 2026-11-07 | ① FR-01 Topic 列表信息极简：仅名称 / 分区数 / 各分区最新 offset，去掉 HWM、清理策略、消息总量、时间等其他元数据（业务前端同步修订）；② 新增图表维护约定：架构图经 drawio MCP 生成与维护，读取 / 修改必须走 MCP 并同步更新图片 | 用户反馈 2 条 |
| v1.2 | 2026-11-07 | ① **Kafka 连接认证显式化（用户需求）**：SASL 用户名密码与 TLS 均为可选、互相独立，支持明文 / SASL / TLS / SASL over TLS 四种组合，FR-13 重写为字段级配置（D18）；② 评审四决策落地：`--last` 改回扫量语义并取消 `--limit`（FR-03/05）、dry-run 提前至 v0.3（§16）、重放改为有界命中快照纯本地回放（D7/FR-09/FR-12/图5）、客户端支持完整 profile 管理（FR-11）；③ 矛盾修复：内存口径两档化（§13）、command 字段 2KB 截断（FR-09）、op 双阶段落库 + running 状态 + serve 轮询实时机制（FR-09/12）、审计完整率口径修订（§2.3）；④ 边界补全：topic describe 补 FR（FR-01）、scan 超时 30s（FR-05）、`--partition` 多值与归并 tie-break（FR-03）、二进制 header / 非 UTF-8 key（FR-04）、session 24h 自动结束（FR-10）、SKILL Never do 增补 audit clear（§9.3） | 本轮评审闭环，用户 5 项决策 |
| v1.2a | 2026-11-08 | ① **TLS 收窄为单向**（用户反馈）：移除 mTLS 双向认证，`tls` 仅保留 `ca_cert`（客户端校验服务端）+ `skip_verify`（直接跳过校验），取消 `client_cert` / `client_key` 字段及 `--tls-client-cert` / `--tls-client-key` 参数（FR-13、D18、§13/14 同步）；② **bootstrap_servers 显式支持多 broker 地址**：YAML 数组 / CLI 逗号分隔 / UI 多行输入，kgo 自动发现全集群（FR-13） | 用户反馈 2 条 |
| v1.2b | 2026-11-08 | ① **单条注入收敛**（用户反馈）：`--key` / Key 改为必填，取消指定分区能力（CLI `--partition`、`POST /api/produce` 的 `partition`、UI 分区输入全部移除），目标分区由 key 一致性 hash 自动判定（FR-06/07、§9.1、FR-11）；② **所有表格列宽可拖拽调整**、双击按内容自适应，宽度按页面持久化（FR-11）；③ UI 缺陷修复：时间控件输出标准 RFC3339（`+08:00` 带秒，`step=1`），后端 `parseFlexibleTime` 兼容 `+0800` / 无时区格式；Fluent Dropdown 换原生 select + 全局 ErrorBoundary 消除白屏；下拉面板改 Portal 渲染并网格化以适配多分区；Topics 表格固定布局让 offset 列占 62% 宽度；AI 观测区会话 / op 支持多选删除（新增 `DELETE /api/audit/ops`、`DELETE /api/sessions`）；④ 附带修复：空 topic 扫描快速返回（不再等 30s 超时）、扫描响应补 `partition_ranges`、列表接口空结果由 `null` 统一为 `[]` | 用户反馈 9 条（6 + 3） |
| v1.2c | 2026-11-08 | 对照 FR-07 验收实现时补齐脚本注入语义：① `generate(ctx)` 返回**数组**真正落地（此前仅支持单对象），`--count N` 明确为**总消息条数**而非生成器调用次数；② `ctx.random()` 由「纳秒时间戳取模」改为 `math/rand/v2` 独立均匀取样（同一毫秒内连续调用不再退化为同一值），`ctx.pick(arr)` 由按 index 轮询改为真随机，`ctx.uuid()` 改为 UUID v4；③ 清除 `genMessage.Partition` 死字段，与 v1.2b「分区只由 key 判定」保持一致。真实 Kafka 回归：数组脚本 dry-run 5 条样例的 amount/status/uuid 全部互异，注入 20 条 produced=20 failed=0，回读 40 条全部带唯一 key 且散列至 12 个分区 | 实现自查闭环，非用户新增需求 |
| v1.2d | 2026-11-08 | ① **修复 `POST /api/produce` headers 类型契约**（用户反馈）：Web UI 发送 `{"k":"v"}` 对象，后端却声明 `[]string`，导致 `cannot unmarshal object into Go struct field produceRequest.headers of type []string`。改为 `json.RawMessage` + `parseProduceHeaders`，同时支持对象（UI，推荐）与 `["k=v"]` 数组（CLI 风格，向后兼容）；对象按 key 排序、校验空名 / 含 `=` 的 key、非字符串值转 JSON 文本（FR-06）；② **补齐查询结果 header 展示**：查询页原本即应含 headers 列与展开区 Headers 区块，实现此前遗漏。Scan 结果表格新增 Headers 列（前 2 个 `k=v` 摘要 + 余量计数、hover 显示全量、空值显示 `""`、无 header 显示 `—`），展开详情拆为「消息内容」与「Headers（N 项）」两块，无 header 时给出明确占位（FR-04/11）；③ 真实 Kafka + 无头浏览器 CDP 双重回归：对象/数组/非字符串值/错误格式全部符合预期，UI 表头出现 Headers 列、展开区含 header 值、JS 异常 0 | 用户反馈 2 条 |
| v1.2e | 2026-11-08 | ① **回扫量改为单选框**（用户反馈）：Web 查询页由 `SpinButton` 自由输入改为**固定 5 档单选** 100 / 500 / 1000 / 5000 / 10000，默认 100；② **上限 5000 → 10000**：`scan.MaxLast`、`ValidateRequest`、CLI help、`POST /api/scan` 统一放宽，10001 仍 exit 2 / HTTP 400；③ >1000 档位改为**内联性能提示**并落地 CLI `warnings:["scan>1000"]`，替代原 Fluent Dialog 确认框（避免每次扫描被打断）；④ D9、FR-03/05、§10.2/10.3/10.4、§11、§12.3、§13、业务前端查询页 / 审计页、架构图 3 全部同步 | 用户反馈 1 条 |
| v1.2f | 2026-11-08 | ① **修复 Web UI 脚本批量注入契约**（实现自查发现）：前端用 `FileReader` 读取用户选择的 `.js` 文件并把**源码文本**放进 `script`，后端 `BatchRequest.ScriptPath` 却按**服务端文件路径** `os.ReadFile`，导致 UI 的 Dry-run / 执行 100% 报 `read script: open function generate(ctx){...}: The filename...`。新增 `ScriptContent` 字段与 HTTP `script_content`，Web UI 改传内联源码；`script` 保留路径语义供 CLI / 脚本调用（向后兼容），两者至少其一、`script_content` 优先；② **落地 FR-07 沙箱超时**：脚本编译与每次 `generate(ctx)` 调用各享 5s 预算（goja `Interrupt`），死循环返回明确超时错误；③ **生成器错误不再静默吞掉**：非 dry-run 返回 `batch aborted (produced=N failed=M): ...`，已写入消息不回滚（FR-08）；④ `--count` / HTTP `count` 默认由 0 改为 100（原默认 0 会产出 0 条）；⑤ 真实 Kafka + 无头浏览器 CDP 双重验证：UI 选文件 → Dry-run 请求体含 `script_content`（344B）、返回 5 条样本、JS 异常 0 | 实现自查闭环，非用户新增需求 |
| v1.3 | 2026-11-08 | ① **Web UI 取消文件选择**（用户反馈）：批量注入改为**等宽 textarea 直接粘贴 / 编辑 JS**，默认载入可运行示例，并提供「每次 1 条」「每次 2 条」两个示例模板；② **新增「调用规则」卡**：常驻说明 generate 的调用时机、每次返回量、总构造量公式、ctx 与状态语义，并随「计划总条数」实时估算调用次数；③ **FR-07 详细设计**：给出调用模型伪代码、三种返回值对照表、总量公式 `min(count, 脚本累计产出)`、完整可运行示例与常见错误表；④ 边界加固：返回**空数组**原会导致 generator 无限循环，现改为明确报错；结果新增 `generateCalls` / `messagesBuilt`，UI 结果条直接回报「调用 N 次、构造 M 条」；⑤ 业务前端批量注入页重画（含修正 goja 不兼容的 `export function` / `ctx.now()` 示例代码、Dry-run 5 条、规则卡）；§10.2/10.3/10.4 同步 | 用户反馈 1 条（含 3 项详细设计诉求） |
| v1.3a | 2026-11-08 | ① **Web UI 补齐 ctx 完整说明**（用户反馈）：批量注入页新增常驻「ctx 速查」卡，逐条列出运行时注入的全部 5 个成员（成员名 / 类型 / 作用与边界 / 写法示例），补充返回消息对象字段规格（`value` 必填字符串、`key` 可省略自动补、`headers` 对象、不支持 `partition`）、沙箱可用 / 不可用全局对象清单，以及 3 个可一键载入编辑器的可运行示例（全成员 / 顶层状态 + 提前结束 / 数组 + headers）；② **FR-07 新增「ctx 运行时契约」详细设计**：以 dry-run 实测确认 `Object.keys(ctx) = index,time,random,uuid,pick`，标准内置 `Math`/`JSON`/`Date`/`Promise`/`RegExp`/`Map` 可用，`console`/`require`/`fetch`/`setTimeout`/`process` 不可用；③ 说明表纳入全局列宽拖拽 / 双击自适应；④ §10.1/10.3/10.4 与业务前端（新增 ctx 速查卡）同步 | 用户反馈 1 条 |
| v1.3b | 2026-11-08 | ① **数字参数改为可键入输入框**（用户反馈）：「计划总条数 / Concurrency / Rate」由只能 ±1 步进的 `SpinButton` 改为 `Input type=number`，支持直接键入 / 粘贴大数值；前端按整数范围校验（count ≥ 1、并发 1~32、速率 ≥ 0），非法时红字提示并禁用 Dry-run / 执行，`runBatch` 内二次防御；估算文案在非法输入时给出明确提示；② **修复按钮不可见**（用户反馈）：v1.3a 的两张长说明卡（合计约 1.1k px）把 Dry-run / 执行挤出首屏，现将按钮组移到脚本编辑器正下方并 `position:sticky; top:0` 滚动贴顶常驻（最初误用 `bottom:0`，对位于内容上方的元素不生效，CDP 实测后修正），「调用规则」与「ctx 速查」卡整体移到进度 / 结果区之后，并加「参考说明」分隔；③ **修复页面无法滚动**（用户反馈）：根因是 `.kp-fill` 固定 `height:100%` + Fluent Card 运行时注入的 `overflow:hidden`，Card 被压到 720px 却含约 1970px 内容，多出的说明卡与按钮被直接裁剪且 `main.scrollHeight == clientHeight` 无滚动条。现为注入页单独设 `.kp-fill--produce{height:auto;min-height:100%}`、`.kp-card--batch{overflow:visible !important}`，主区域恢复滚动；④ §10.1/10.3/10.4、FR-07 规则 9 与验收、业务前端（参数卡标签与说明文案）同步 | 用户反馈 3 条 |
| v1.3c | 2026-11-08 | ① **定位「total 1200 / matched 100 / last-truncated」困惑**（用户反馈）：审计记录复现为 `kp-multi-part`（12 分区），`--last N` 由 `calculateRanges` 逐分区计算 `start = HWM - N`，故 `total = 分区数 × N = 1200`；旧实现的第 6 步再**全局截断至 N 条**，把 12 个分区的命中砍成 100 条（多数分区结果被整体丢弃），并给出无解释的内部码 `last-truncated`；`total` 实为**实际读取并检查的原始消息数**，既非 topic 总消息数、也非命中数；② **取消全局 `--last` 截断（用户决策）**：每分区已各读 ≤ N 条原始消息、命中自然 ≤ N 条/分区，跨分区归并后**全部返回**，不再为了凑 N 丢分区；`matched` 即真实命中总数（≤ 分区数 × N）。新增 `MaxResultMessages = 50000` 安全上限，仅极端 topic 触发，置 `truncated:true` / `warnings:["result-cap"]`；`last-truncated` 码保留兼容读取旧结果但不再产生；③ **结果区新增分区筛选**：结果表上方常驻「结果分区」chip 条，默认「全部」（展示全部命中），列出本次有命中的每个分区及条数，点击即本地筛出该分区并重置分页，无需重新扫描；分页与计数随筛选联动；④ **统计与告警中文化**：`Total/Matched/Elapsed (ms)` 改为「已扫描 / 命中 / 耗时 (ms)」，新增 `matched_raw`（`json:"matched_raw,omitempty"`，截断时回报截断前命中数）支撑「共 X / 返回 Y」；统计条下常驻口径说明（已扫描 ≠ topic 总量、覆盖分区数、分区数 × N、命中按时间倒序且各分区全保留），`warnings` 内部码译成中文人话（`last-truncated` / `result-cap` / `scan>1000` / `timeout`，未知码原样透出）；回扫量单选下方标注乘算示例；⑤ D9、FR-03/05（新增「结果统计与截断语义」小节与验收）、§10.2/10.3/10.4、§11 JSON、§13 内存口径、业务前端（统计条标签、回扫量口径行、结果分区筛选条）同步；⑥ 真实 Kafka + 无头浏览器 CDP 回归 | 用户反馈 2 条 |
| v1.3d | 2026-11-08 | ① **界面基线收敛**（用户反馈）：界面设计与交互统一以 `web/src` 业务前端为唯一基线，删除此前独立的静态页面文件，后续设计变更直接修改业务前端并同步本文档；② §10.4 改写为「界面实现基线」，保留 6 个页面实现要点表，全文不再引用任何静态页面文件；图表维护约定、FR-04 与全部历史变更记录中的相关引用一并清理；架构图 1~5 与 drawio 源文件保留不变；③ 延续 v1.3c：真实 Kafka 与无头浏览器 CDP 回归 | 用户反馈 1 条 |
| v1.3e | 2026-11-08 | ① **消息查询新增「按 key 定位分区」显式开关**（用户反馈）：查询条件卡下新增常驻开关，默认**关闭**（仍扫描全部分区），仅当已填 Topic 且填了**精确 `--key`** 时可启用；启用后实时调用 `GET /api/key-partition?topic=&key=` 用 `murmur2(key) & 0x7fffffff % 分区数` 复刻 franz-go 默认分区器算出目标分区，并把分区多选**预置**为该分区（用户可随时改回全部分区），结果区标注「本次实际扫描：Pn（共 m 个分区，其余 m-1 个未检查）」并提示其它分区可能仍有相同 key 的消息；② **反向实测澄清「不扩缩容也不保证不漏」**（用户追问）：用 `ManualPartitioner` 向 `kp-multi-part`（12 分区、从未扩缩容）写入同 key `kp-routing-probe` 到 P0（murmur2 落点）与 P1（自定义/显式分区器落点），全分区扫描命中 4 条、仅扫 P0 命中 2 条且静默漏掉 P1 的 2 条 —— 证明只要生产者未用标准分区器（或曾显式指定分区），即便分区数从未变化也会漏；开关文案明确列出「全部满足才不漏」的 6 个前提并建议不确定时关闭重扫；CLI 新增 `--key-partition` 与 `KeyPartition` API；③ **修复消息查询页无法滚动**（与 v1.3b 同根因）：扫描页根容器 `.kp-fill` 沿用 `height:100%` + 内联 `overflow:hidden`，把查询卡下方的开关区、风险提示区与结果区整体裁剪、且 `main.scrollHeight == clientHeight` 无滚动条；新增 `.kp-fill--scan{height:auto;min-height:100%}` 后 `main.scrollHeight` 由 ≈434 恢复为 ≈1577，全部内容可滚到；④ FR-05 新增规则 10 与「按 key 定位分区」契约 + 验收，§10.4 消息查询行、§11/`types` 与业务前端（开关、状态卡、风险清单、分区预置、结果标注）同步；⑤ 真实 Kafka + 无头浏览器 CDP 回归（开关启用→预置 P0→扫描命中 2 条且结果标注只扫 P0；关闭→全分区命中 4 条；页面可滚动到底） | 用户反馈 2 条 |

| v1.4 | 2026-11-08 | ① **新增本地查询结果缓存 FR-15**（用户需求）：在"broker 节点不变、topic 不变、offset 单调递增、数据只会增删不会更新"前提下，引入有界物化前缀缓存作为只读加速层；硬约束**双上限**——全局最大条目 `MaxEntries`（默认 200 万）+ 全局最大逻辑载荷 `MaxBytes`（默认 2GiB），超限按 offset 升序淘汰最旧条目；Topic ID 重建守卫 + logStart 前移驱逐 + 每次扫描前对账到 `[logStart, HWM)` 再出结果，保证与同刻直连逐项等价；新增设置项（开关 / 上限 / 清除）+ CLI `--no-cache` + 结果 `served_from_cache` 透明标记；`internal/cache` 已有原型草稿，待评审后按本文对齐实现并同步 README / kp schema / SKILL.md | 用户需求 + 设计先行（先出文档，后改代码） |
| v1.5 | 2026-11-08 | ① **数据目录可配置（FR-16）**：profile（`config.yaml`）与审计 / 缓存（`audit.db`）默认落在 `kp.exe` 同目录 `kp-data/`，不再写入 `%APPDATA%`；`kp-settings.yaml` 固定置于 exe 旁记录 `data_dir`（相对路径基于 exe 目录），`KP_APPDATA` 作为测试覆盖仍保留；优先级 `KP_APPDATA > kp-settings.yaml > 默认 kp-data`；② **一次性、非破坏性迁移**：从旧位置复制 `config.yaml` 与 `audit.db` + `-wal` + `-shm` **整组**（目标已存在绝不覆盖、源文件保留可回滚），`migration_done` 标记保证只做一次，`--force` / `POST /api/settings/migrate` 仅复查；切换目录默认把现有数据一并复制，避免 profile 凭空消失；③ **三端对称**：`internal/config`（`DataLayout` / `SetDataDir` / `MigrateData` / `CopyDataDir` 单一用例）+ CLI `kp settings show / set-data-dir / migrate` + REST `GET/PUT /api/settings`、`POST /api/settings/migrate` + Web 设置页「数据目录」卡片，`kp schema` 命令清单同步；④ **PID 文件与数据目录解耦**：`kp.pid` 改放 exe 同目录并兼容查找旧位置，保证改了 `data_dir` 后 `stop` / `restart` 仍能定位服务；⑤ 新增 `internal/config/settings_test.go`（路径解析 / settings 往返 / SQLite 整组复制不串 WAL / 一次性标记 / 强制复查不覆盖 / 布局）；顺带修复 `internal/cache` 草稿的字节上限淘汰 bug（原实现一次删整批导致 `TestByteCapEvictsOldest` 失败）；改目录后需 `kp restart` | 用户需求：profile / data 可自定义 + 本机一次性迁移 |

**图表维护约定（重要）**

- 本文档全部架构图 / 流程图 / 时序图（`figures/1~5`，PNG 与多页源文件 `docs/figures/kafka-pilot-架构图.drawio`）均**通过 drawio MCP 工具生成**；
- 任何对图表的**读取或修改必须通过 drawio MCP 进行**（查看图元、编辑页面、增删页），禁止手工编辑 `.drawio` XML 文件或使用其他工具改图；
- 文档中**涉及图表内容的设计变更**（架构、模块、链路、流程等）发生时，必须**调用 drawio MCP 同步修改对应页面并重新导出 PNG**，保证图文一致，不允许图文脱节；
- 界面设计与交互以 `web/src` 业务前端为唯一基线，不属 drawio 管辖；界面变更直接修改业务前端并同步本文档。

**已确认的关键决策（澄清结论）**

| # | 决策项 | 结论 |
|---|---|---|
| D1 | 内核技术栈 | Go，单二进制，无运行时依赖 |
| D2 | 客户端形态 | 本地 Web UI：内核起 127.0.0.1 HTTP/SSE 服务，前端窗口呈现 |
| D3 | 使用环境 | 开发 / 测试环境自用，弱权限、重效率 |
| D4 | 表达式语法 | jq 风格（gojq 沙箱） |
| D5 | 消息格式 | 纯文本 / JSON，不做 Schema Registry |
| D6 | 注入脚本 | JavaScript 内嵌（goja 沙箱） |
| D7 | 审计粒度 | 调用级摘要 + 最终结果 + 任务会话时间线 + **有界命中快照**（v1.2 修订：不落原始扫描全量消息；scan 命中结果落快照每 op ≤100 条 / ≤1MB，供纯本地重放；produce 不落快照） |
| D8 | 功能边界 | 查数据 + 造数据 + 只读观测（topic 元数据、consumer lag）；不做 topic 增删改 |
| D9 | 扫描边界 | 默认回扫最近 100 条，**上限 10000**（v1.2e 由 5000 放宽），超 1000 提示性能风险；Web 客户端固定 5 档单选 100/500/1000/5000/10000，CLI 接受 ≤10000 任意值（`--last` 为**每分区**回扫 N 条原始消息，命中数 ≤ 分区数 × N；`--limit` 取消）。**v1.3c：各分区命中全部返回，不再全局截断至 N**，结果区可按分区筛选；另设 50000 条单次结果安全上限 |
| D10 | 人类权限 | 客户端可写（与 AI 同能力），AI 轨迹区纯只读 |
| D11 | 连接配置 | 多集群 profile |
| D12 | 注入规模 | 万级（1 万～50 万条），背压流式管道，速率 / 并发可调 |
| D13 | 审计存储 | SQLite 单库存摘要与最终结果，保留 30 天 / 500MB（v1.1 修订：取消 JSONL 原文文件） |
| D14 | 认证鉴权 | CLI 与客户端均不做认证鉴权（本地单机工具定位）；Kafka 连接凭证仅由内核 config 模块持有（v1.1 新增） |
| D15 | 会话标识 | CLI 提供 `kp session new` 随机生成短 session_id（6 位数字+字母）；Agent 先生成、后每条命令显式携带 `--session`（v1.1 新增） |
| D16 | 解码扩展 | jq 为默认解析 / 过滤插件，预留 Decoder 扩展接口供后续自定义插件（v1.1 新增） |
| D17 | UI 设计语言 | Fluent Design（Fluent UI React v9，Mica/Acrylic 材质、跟随系统主题与强调色），现代简洁（v1.1 新增） |
| D18 | 连接认证 | SASL 用户名密码认证与 TLS 均为**可选**、互相独立，支持明文 / SASL / TLS / SASL over TLS 四种组合；TLS 为**单向**（客户端校验服务端，仅 `ca_cert` + `skip_verify`，不支持 mTLS 双向认证）；`bootstrap_servers` 支持配置多个 broker 地址（v1.2 新增，v1.2a 收窄 TLS 为单向 + 显式多 broker） |

## 二、背景与目标

### 2.1 背景

开发测试阶段排查 Kafka 数据问题时，现有方式效率低：`kafka-console-consumer` 参数繁琐、无过滤能力；自写脚本每次重复造轮子；AI Agent（Claude / WorkBuddy 等）无法安全、结构化地访问 Kafka——要么没有入口，要么输出不可控、操作不可追溯。

同时，AI 操作存在信任问题：**AI 到底查了什么、往 topic 里灌了什么，人需要一个只读窗口去核对**。

### 2.2 目标

| 目标 | 衡量口径 |
|---|---|
| 让 AI Agent 一条命令完成 Kafka 查数 / 造数 | 单命令完成，输出为稳定结构的 JSON，无需二次解析处理 |
| 让人类零门槛操作 | 客户端图形界面完成全部查询与注入，无命令行依赖 |
| AI 操作全程留痕、人类可只读回放 | 每次调用产生 op 记录，客户端按会话时间线 100% 还原 |
| 双端行为一致 | CLI 与客户端共用同一内核用例层，功能行为无分叉 |

### 2.3 成功指标（口径）

| 指标 | 口径 | 目标值 |
|---|---|---|
| 查询命中准确率 | 给定 key / 关键词 / 表达式，命中结果与预期一致的比例 | 人工抽检 ≥ 99% |
| 查询响应时间 | 默认 last 100 条查询，从发起至结果返回 | P95 ≤ 3s（局域网集群） |
| 注入吞吐 | 速率参数放开、并发 8 时单机生产 TPS | ≥ 5000 条/s（估算，依赖 broker 配置） |
| 审计完整率 | CLI / 客户端操作被完整记录（含参数与最终结果统计）的比例；崩溃 op 经启动扫描标记 crashed 收敛 | 正常退出路径 100% |
| 审计体积 | 单条 operations 记录平均大小（命中快照独立存储，每 op ≤1MB，不计入本口径） | < 4KB（不含消息体） |
| 双端一致性 | 同一操作 CLI 与客户端产生相同结果与相同审计字段 | 100% |

## 三、名词与术语

| 术语 | 定义 |
|---|---|
| 内核（core） | `internal/` 下的 Go 包集合，承载全部 Kafka 能力，不含任何界面逻辑 |
| 用例层（app） | `internal/app`，将内核模块编排为 Scan / Produce / Inspect / Audit 四类用例，CLI 与本地服务唯一入口 |
| op（操作） | 一次 CLI 调用或一次客户端操作，审计的最小单位，有唯一 op_id |
| session（会话） | AI 一轮任务（如"排查订单丢失"）的聚合单位，包含 1..N 个 op；由 `kp session new` 生成 6 位短 ID |
| 观测区 | 客户端中只读展示 AI 操作轨迹的功能区 |
| 快照（snapshot） | scan 类 op 落盘的有界命中结果：仅命中消息（过滤后，非全量扫描），每 op ≤100 条且序列化 ≤1MB，存 audit.db 独立表，供本地重放（v1.2 新增） |
| 重放（replay） | 读取 scan 类 op 的命中快照做**纯本地展示**，不发起任何 Kafka 读写，用于核对 AI 当时查到的内容（v1.2 修订：由"重新执行查询"改为"读快照"） |
| HWM | High Water Mark，分区最新可见 offset |
| 背压 | 有界队列写满时阻塞上游生成器，防止内存随总条数增长 |
| direct assign | 消费者直接指定分区与 offset 读取，不加入 consumer group、不提交 offset |

## 四、用户角色与核心场景

| 角色 | 入口 | 典型场景 | 能力 |
|---|---|---|---|
| AI Agent | Skill over CLI | 排查"订单消息丢了"；为压测注入 10 万条模拟订单 | 全部命令；每次调用自动留痕 |
| 人类开发者 | Windows 客户端 | 日常查数、造数；检查 AI 刚才干了什么 | 操作区可写 + 观测区只读 |

**核心场景 1：AI 排查消息丢失**
AI 执行 `kp session new` 取得 6 位短 ID → 之后每条命令携带 `--session`：按时间窗 + key 扫描上游 topic → 按 jq 表达式过滤异常记录 → 扫下游 topic 对比 → 输出结论。人类在客户端观测区按会话回放全程，点开任一 op 查看查询参数与最终结果统计，需要看内容时点「重放」实时复现。

**核心场景 2：人类造测试数据**
客户端选择 topic → 注入面板选"脚本"或"模板" → 设置条数 / 并发 / 速率 → dry-run 预览 → 执行并观察实时进度。

**核心场景 3：AI 批量造数**
AI 调 `kp produce --script gen.js --count 100000`，CLI 输出进度行，完成后返回 JSON 汇总；客户端观测区实时显示进度与最终结果统计（成功 / 失败 / TPS）。

## 五、需求范围

**包含**

1. Topic 浏览（名称、分区数、各分区最新 offset）——只读，信息极简，不含清理策略 / HWM 等其他元数据（v1.1b）
2. Consumer Group 列表、成员、分区分配与 lag 观测——只读
3. 消息查询（key / value 关键词 / jq 表达式 / 时间段 / 最近 N 条 / 指定分区）
4. 消息注入（单条 + JS 脚本批量并发）
5. AI 操作审计与会话时间线，客户端只读回放（最终结果 + 有界命中快照，纯本地重放）
6. 多集群 profile 管理（含可选 SASL 用户名密码认证与可选 TLS，v1.2）
7. Skill（SKILL.md）+ CLI 分发
8. 本地查询结果缓存（可选、有界）：对 broker 拉取结果做本地物化前缀缓存以加速重复查询，受最大条目 + 最大物理空间双上限约束（FR-15）
9. 数据目录可配置（FR-16）：profile / 审计 / 缓存默认随二进制放在同目录 `kp-data/`，可通过 `kp-settings.yaml` / CLI / REST / Web UI 改到任意目录，并提供从旧 `%APPDATA%` 位置的一次性安全迁移

**不包含（本期不做）**

| 项 | 说明 |
|---|---|
| Topic 增删改、分区扩容、配置修改 | 误操作风险高，开发测试环境可用原生工具；记入后续版本池 |
| Schema Registry / Avro / Protobuf 解码 | D5 决策不做；非 JSON 消息按字节 + hex 展示；已预留 Decoder 插件接口（D16） |
| 消息删除 / offset 重置 | 危险写操作，明确不做 |
| 认证鉴权（CLI / UI 层） | 本地单机工具定位，明确不做（D14）；注：此处指**工具自身接口**，Kafka 连接的 SASL / TLS 认证属 FR-13 范围（v1.2 澄清） |
| 跨机器部署 / 多用户 Web 服务 | 单机本地工具定位，服务只绑定 127.0.0.1 |
| 消息生产端埋点采集（业务侧 tracing） | 超出工具边界 |

## 六、核心功能设计

### FR-01 Topic 浏览（v1.1b 修订：信息极简）

- 规则：
  1. topic 列表**仅输出三项信息——topic 名称、分区数、各分区最新 offset**（只读）。不展示清理策略、保留配置、消息总量、最早 offset、时间范围等任何其他元数据。
  2. `kp topic describe <topic>` 输出与列表单项**同构**（name / partitions_count / 逐分区 latest_offset），不引入任何其他元数据（v1.2：补齐该命令的 FR 定义）。
- 验收（GWT）：Given 已配置可用 profile，When 执行 `kp topic list --filter order`，Then 输出名称含 "order" 的 topic 列表 JSON，每项**仅含** `name`、`partitions_count`、`partitions` 数组（每元素为 `partition` + `latest_offset`），无其他字段；When `kp topic describe orders`，Then 输出结构与列表单项一致且不含额外字段。

### FR-02 Consumer Group 观测（只读）

- 规则：列出 group、state、成员与分区分配；按 topic 维度输出各 group 的 committed offset、HWM、lag；lag 超阈值（默认 10000，可配）标记 `highLag=true`。
- 验收：Given group "order-consumer" 订阅 topic "orders" 且 lag=15200，When `kp group lag order-consumer --topic orders`，Then 结果含 `lag:15200` 且 `highLag:true`。

### FR-03 消息查询——定位维度

| 参数 | 语义 | 默认 |
|---|---|---|
| `--partition` | 只扫指定分区，支持逗号分隔多值（如 `0,2`，v1.2）；不指定则扫全部分区并行 | 全部 |
| `--last N` | **每分区回扫量语义（v1.3c）**：从**每个**分区尾部各回扫 N 条原始消息（`total ≈ 分区数 × N`），跨分区按时间戳倒序归并；三层过滤后各分区命中**全部返回**，命中数 ≤ 分区数 × N，不再全局截断至 N。上限 10000（v1.2e），单次结果安全上限 50000（v1.3c）。**Web 端为 5 档单选**：100 / 500 / 1000 / 5000 / 10000，不可自由输入 | 100 |
| `--since / --until` | 时间窗，按 CreateTime 用 offsetsForTimes 定位起始 offset | 无 |
| `--offset-from / --offset-to` | 精确 offset 区间；未指定 `--partition` 时作用于全部分区（v1.2 澄清） | 无 |

- 规则：
  1. 不指定定位参数时等价 `--last 100`；扫描采用 direct assign，不加入 consumer group、不提交 offset，不影响业务消费。
  2. 参数组合（v1.2 明确）：`--since/--until` 可与 `--last` 组合（时间窗内自窗尾回扫 N 条）；`--offset-from/--offset-to` 与 `--last` 互斥，同时给出报参数错误（exit 2）。
  3. 归并排序 tie-break（v1.2）：timestamp 相同时按 partition、offset 升序。
- 验收：When `kp scan --topic t --since 2026-11-07T10:00:00+08:00 --until 2026-11-07T11:00:00+08:00 --last 100`，Then 在该时间窗内自窗尾回扫 100 条原始消息并返回命中结果，且结果含每分区实际扫描区间。

### FR-04 消息查询——三层过滤

| 层 | 参数 | 语义 |
|---|---|---|
| key 过滤 | `--key`（精确）/ `--key-regex`（正则） | key 序列化为 UTF-8 字符串后匹配；正则用 RE2 语法 |
| value 关键词 | `--keyword` | value 的 UTF-8 文本做子串包含匹配；`--ignore-case` 默认开启 |
| 表达式 | `--expr '<jq>'` | jq 表达式作用于上下文对象 `{key, value, headers, partition, offset, timestamp, size}`；value 为合法 JSON 时自动解析为对象，解析失败退化为字符串；表达式结果为真值即命中 |

- 表达式示例：
  - `.value.orderId == "A1001"`
  - `.value.amount > 100 and .value.status == "FAILED"`（jq 语法）
  - `.headers.traceId != null`
- **结果展示（v1.2d，补齐查询页既有基线）**：查询结果表格固定含 `Headers` 列，展示前 2 个 `k=v` 摘要 + 余量计数（`+N`），hover 显示全量；空值渲染为 `""`、无 header 渲染为 `—`；展开行拆为「消息内容」与「Headers（N 项）」两块，后者独立展示全部 header（二进制值以 `hex:` 前缀），无 header 时给出明确占位。keyword 检索范围包含 header 名与值。
- 安全：gojq 在沙箱内运行，无 IO 能力；表达式编译失败立即返回参数错误（exit 2），不发起扫描。
- 边界（v1.2）：key 为非 UTF-8 字节串时该消息不参与 key 过滤、计为不匹配并在结果 `skippedNonUtf8Key` 计数；header 值为二进制时以 hex 展示；`--ignore-case` 可显式关闭。
- **解码插件扩展点（v1.1 新增）**：value 解码抽象为 `filter.Decoder` 插件接口，内置 `text` 与 `json` 两个实现，`json` 为默认。后续用户可按接口注册自定义解码插件（如 Avro / Protobuf / 自定义报文）：插件只需实现 `Decode([]byte) (any, error)`，表达式上下文结构不变；本期预留接口与注册配置，不实现第三方动态加载。
- 验收：When `--expr '.value.amount > 100'`，Then 仅返回 amount>100 的记录，且每条记录附带原始 offset 供回查。

### FR-05 扫描边界（性能底线）

- 规则：
  1. 单次回扫条数上限 **10000**（v1.2e；`--last` 为唯一上限参数，v1.2 取消 `--limit`，避免两个上限语义打架；`--last > 10000` 拒绝执行 exit 2 / HTTP 400）；
  2. 默认 `--last 100`；
  3. **Web 客户端回扫量固定 5 档单选** 100 / 500 / 1000 / 5000 / 10000（v1.2e），不接受自由输入；选中 >1000 档位时在扫描按钮旁显示**内联性能提示**但不阻断（替代原 Fluent Dialog 确认框，避免每次扫描被打断）；CLI 输出 `warnings:["scan>1000"]` 并继续，双端均不中断扫描；
  4. 单条消息 > 1MB 时 value 截断至 64KB 展示（仅存在于本次响应与命中快照中，不落审计 operations 行；`--output` 写文件属用户显式行为，不受审计约束——v1.2 措辞澄清），标记 `truncated:true`；
  5. 因边界提前终止时，结果标记 `truncated:true` 并附实际扫描区间；
  6. 扫描超时（v1.2 新增）：默认 30s，`--timeout` 可配（1~300s）；超时返回已扫描部分并标记 `truncated:true`，exit 4。
  7. **不再全局截断（v1.3c，用户决策）**：`--last N` 的 N 是**每分区回扫量**，不是「结果最多 N 条」。旧版在归并后执行 `allMessages[:N]`，把 12 分区 topic 的 1200 条命中砍成 100 条、部分分区结果整体消失并报难懂的 `last-truncated`；现移除该步骤，各分区命中**全部返回**，`matched` 为真实命中总数。
  8. **单次结果安全上限（v1.3c）**：`MaxResultMessages = 50000`，仅当命中总数超过该值时触发；为避免按时间全局截断再次抹掉整个分区，改为**按分区均摊保留**（每分区最多 `50000 / 分区数` 条、各取该分区最新命中），保证所有分区仍有结果。置 `truncated:true`、`warnings:["result-cap"]` 并回填 `matched_raw`（截断前命中数）。常规场景（如 12 分区 × 100 = 1200 条）不触发。
  9. **结果分区筛选（v1.3c）**：Web 结果表上方常驻「结果分区」chip 条，默认「全部」（展示全部命中），列出本次有命中的每个分区及条数；点击分区在**前端本地过滤**、重置分页与行展开，**不重新发起扫描**；分页总数与底部计数随筛选联动。
  10. **按 key 定位分区（v1.3e，用户反馈的显式开关）**：基于「有精确 key 时能否先算出落在哪个分区」——**可行**，但默认**不开启**，做成可见开关，避免静默漏数据（见下方契约与反例）。
      - **开关位置与默认**：查询条件卡下方常驻「按 key 定位分区（实验性）」`Switch`，**默认关闭**，此时扫描**全部分区**；仅当已填 `Topic` 且填了**精确 `--key`**（正则 / keyword / jq 不满足）时才可启用（`disabled` 否则），开关提示「需先填写 Topic 与精确 --key 才能启用 · 默认关闭，扫描全部分区」。
      - **计算与预置**：启用后前端实时（250ms 防抖）调用 `GET /api/key-partition?topic=<t>&key=<k>`，用 `murmur2(key) & 0x7fffffff % partition_count` 复刻 franz-go 默认分区器（本工具 v1.22.2 `UniformBytesPartitioner` 对**有 key** 的记录走该算法），把分区多选**自动预置**为算出的那一个（用户可随时手动改回「全部分区」）；分区字段 label 同步变「分区（已按 key 定位为 Pn）」。
      - **结果标注与风险**：结果统计条下方常驻「本次实际扫描：Pn（共 m 个分区，其余 m-1 个未检查）。命中数只代表该分区，其它分区可能仍有相同 key 的消息。」；开关下方风险卡列出「**分区数没变，也不代表一定不漏数据**」并给出全部满足才不漏的前置条件（见契约 §）。
      - **为什么默认关闭 / 不是自动**：精确 key 哈希只能覆盖「生产者用标准一致性 hash 且 topic 从未扩缩容」的情形；一旦生产者用自定义分区器、显式指定分区、或 topic 曾扩缩容，按当前分区数算出的分区会**漏掉**历史 / 异源消息。显式开关让用户**可见且可撤销**地缩小扫描范围，而非悄悄丢数据。

#### FR-05 按 key 定位分区的契约与「不扩缩容也会漏」反例（v1.3e）

- **算法与权威实现**：`internal/kafka/keypartition.go` 的 `KeyPartition(topic, key)` 通过 `app.DescribeTopic` 取分区数 `n`，返回 `murmur2(key) & 0x7fffffff % n`；`murmur2` 为 franz-go 默认分区器所用的 32 位 MurmurHash2（小端、seed `0x9747b28c`）。单元测试 `keypartition_test.go` 已对 `order-9999 / A1001 / 中文key` 等逐 key、逐分区数（2/3/12/50）与 franz-go 实际落点**逐条比对一致**。
- **实测反例（证明「没扩缩容」不足以保证不漏）**：`kp-multi-part` 为 12 分区、自创建以来**从未扩缩容**。用 `kgo.ManualPartitioner()`（代表任何显式指定分区 / 非标准分区器的生产者）写入同 key `kp-routing-probe`：
  - 写入 P0（= murmur2 计算落点）与 P1（= 异源 / 自定义分区器落点）各 2 条；
  - **全分区扫描**（`partitions=[]`）：命中 **4** 条（P0 与 P1 都在）；
  - **按 key 定位只扫 P0**（`partitions=[0]`）：命中 **2** 条，**静默漏掉 P1 的 2 条**；
  - 结论：即便分区数恒定，只要存在异源 / 显式分区写入，按 key 哈希缩小扫描就会漏数据。开关文案把这一点显式告知用户。
- **全部满足才不漏**的前提（开关风险卡列出）：① 该 key 全部由使用标准一致性 hash 的生产者写入；② 生产者未用自定义分区器、未显式指定分区；③ topic 自该 key 写入以来从未扩缩容；④ 集群未发生过跨分区数据迁移 / 重平衡导致的副本重建（不影响 hash，但属于运维边界）；⑤ 该 key 未被其它语言客户端以不同 hash（Sarama FNV-1a、librdkafka CRC32 等）写入；⑥ 用户确认本次只需查该分区近期数据。任一不满足 → 关闭开关、扫描全部分区。
- **API / CLI**：`GET /api/key-partition?topic=&key=` 返回 `{algorithm, key, partition, partition_count, topic}`；CLI `kp scan --key <k> --key-partition` 在请求里加 `KeyPartition: true`，后端先算分区再扫描并在结果回填 `key_partition` / `key_partition_count`。
- 验收：When 未填 key 打开扫描页，Then 开关 `disabled`；When 填 `kp-multi-part` + 精确 key `order-9999` 并启用开关，Then 分区多选被预置为 P4、请求体含 `partitions:[4]`、`key_partition:true`，结果标注「本次实际扫描：P4（共 12 个分区，其余 11 个未检查）」；When 同 topic + key `kp-routing-probe` 启用开关只扫 P0，Then 命中 2 条且漏掉 P1 的 2 条（反例成立）；When 关闭开关，Then 分区多选清空、扫描全部分区、命中 4 条；When 打开开关但把分区改回「全部分区」，Then 仍扫描全部、不漏。

#### FR-05 结果统计与截断语义（v1.3c）

| 字段 | 含义 | 示例（12 分区 topic、`--last 100`、无过滤） |
|---|---|---|
| `total` | 实际读取并检查的原始消息数，≈ 分区数 × N；**既不是 topic 总消息数，也不是命中数** | 1200 |
| `matched` | 三层过滤后的命中数，各分区全部保留、按时间倒序跨分区归并 | 1200 |
| `matched_raw` | 仅截断时出现，截断前的真实命中数 | 不出现（未截断） |
| `truncated` | 是否因安全上限或超时而截断 | false |
| `warnings` | 机器码，供 CLI / 审计消费；UI 译为中文人话 | `[]`（>1000 档时为 `["scan>1000"]`） |

- `last-truncated` 为 v1.3c 之前的遗留码，新版不再产生；UI 仍保留兼容文案，方便查看历史快照 / 旧服务结果。
- UI 口径：统计条标签「已扫描 / 命中 / 耗时 (ms)」，其下常驻说明「已扫描 = 实际读取并检查的原始消息数；回扫量是每分区各 N 条，总量约 分区数 × N；命中按时间倒序、各分区结果都保留，可在结果区按分区筛选」；`warnings` 内部码不直接透出，统一译为中文（`last-truncated` / `result-cap` / `scan>1000` / `timeout`），未知码原样显示。
- 验收：When `--last 10001`，Then exit 2，错误信息说明上限 10000；When `--last 10000`，Then 正常返回且 `warnings` 含 `scan>1000`；When Web 查询页加载，Then 回扫量渲染为 5 个 radio，默认选中 100，点击 10000 后请求体为 `{"last":10000}` 且出现内联提示；When `--timeout 1` 且集群无响应，Then exit 4 且返回已扫描部分与 `truncated:true`；**When 12 分区 topic 且 `--last 100`（无过滤），Then `total=1200`、`matched=1200`、`truncated=false`、无 `last-truncated`，结果分区筛选条列出 12 个分区且默认「全部」；When 点选 P3，Then 表格只剩该分区消息、分页计数同步、不发起新扫描**。

### FR-06 单条注入（v1.2b 修订：key 必选、分区自动判定）

- 规则：`kp produce --topic t --key k (--value '...' | --value-file <f>) [--header k=v ...]`；**`--key` 为必填**（或 `--key-file` 从 UTF-8 文件读取），CLI / `POST /api/produce` 均在入口校验，缺 key 直接返回参数错误；**不提供 `--partition` / partition 字段，用户不能指定目标分区**——由 franz-go 默认分区器按 key 一致性 hash 决定，相同 key 恒落入同一分区；`--value-file` 支持从文件读取；value 不合法 JSON 时不做校验直接按字节发送（工具不做格式警察，但输出 `valueParsed:false` 提示）。
- **headers 契约（v1.2d）**：CLI 用 `--header k=v`（可重复）；`POST /api/produce` 的 `headers` **同时接受两种形状**——对象 `{"traceId":"abc","source":"ui"}`（Web UI 默认，推荐，无 `=` 歧义）与数组 `["traceId=abc"]`（CLI 风格，向后兼容）。对象形式按 key 字典序归一化；header 名不得为空或含 `=`，否则 exit 2（HTTP 400）并给出修正提示；值允许数字 / 布尔 / null（分别转为 `7` / `true` / 空串）；值内含 `=` 由 `SplitN("=", 2)` 保证无损。
- 验收：
  - Given topic "orders" 3 分区，When 指定 `--key A1001`，Then 消息按 key hash 落入固定分区，重复发送分区一致；不同 key 可落入不同分区；
  - Given 未传 `--key` / `--key-file`，Then exit 2（HTTP 400）并提示"key is required"，不发起任何 Kafka 写入；
  - Given `POST /api/produce` 携带 `partition` 字段，Then 该字段被忽略，分区仍由 key 决定；
  - Given `POST /api/produce` 携带 `{"headers":{"traceId":"t1","eq":"a=b=c"}}`，When 注入后按 key 扫描，Then headers 为 `{"eq":"a=b=c","traceId":"t1"}`（中文 / 等号 / 多值均无损）；传 `["traceId=t1"]` 结果一致；header 名含 `=` 或传字符串 / 数字时返回 400 且提示正确形状。

### FR-07 脚本批量注入

- 规则：
  1. 脚本为 JavaScript（goja），须导出 `generate(ctx)`，返回消息对象 `{key, value, headers?}` 或其数组（**`value` 必须是字符串**，对象请用 `JSON.stringify` 序列化，否则会被 `.String()` 转成 `[object Object]`，v1.2f 澄清）；返回 `null/undefined` 表示结束；**批量消息同样不指定 partition，分区由各消息 key 自动判定**（v1.2b）；脚本未返回 `key` 时工具自动生成唯一 key（`kp-auto-<毫秒时间戳>-<序号>`），保证每条消息都有 key；
  2. `ctx` 提供（v1.2c 精确化）：
     - `index`：从 0 递增的 `generate` **调用序号**；一次调用返回数组时，该批消息共享同一个 `index`；
     - `time`：当前毫秒时间戳；
     - `random()`：返回 `[0,1)` 独立均匀随机浮点数，**每次调用重新取样**（不得以时间戳取模实现，避免同一毫秒内连续调用得到相同值）；
     - `uuid()`：返回 UUID v4 字符串；
     - `pick(arr)`：从数组 / 对象中**随机**取一个元素（非按 index 轮询）；
  3. `--count N`（UI 称「计划总条数」）控制**总消息条数**（v1.2c）：生成器被调用直到累计产出 N 条消息即停止；脚本返回数组时调用次数可少于 N，数组展开后不会超额写入；**默认 100**（v1.2f，CLI / HTTP 一致）。结果回报 `generateCalls`（调用次数）与 `messagesBuilt`（实际构造条数），UI 结果条直接展示「调用 N 次、构造 M 条」（v1.3）；`--concurrency` 默认 8（1~32）；`--rate` 每秒条数上限（0 = 不限速）；以上参数及队列容量、进度上报间隔均支持 profile 默认值 + 命令行覆盖（A1 确认）；
  4. 沙箱（v1.2f 落地）：禁 IO（goja 未注入 `require` / 网络 / 文件 / 进程能力）；脚本**编译**与**每次 `generate(ctx)` 调用**各享 5s 预算，超时经 `vm.Interrupt` 终止并返回明确错误；生成器抛错或超时不再被静默吞掉——dry-run 返回 `generate call N: ...`，实际执行返回 `batch aborted (produced=N failed=M): ...`，已确认写入的消息不回滚（FR-08 口径）；
  5. `--dry-run`：只运行生成器并输出**前 5 条消息**样例（数组展开后计数），不发送；
  6. **脚本来源契约（v1.3 修订）**：CLI `--script <path>` 与 `POST /api/produce/batch` 的 `script` 字段是**服务端本地文件路径**；Web UI **不提供文件选择器**，改为等宽 `<textarea>` 让用户**直接粘贴 / 编辑 JS**，默认载入可运行示例并提供两个示例模板按钮，提交时以 **`script_content`** 字段发送**内联 JS 源码**（浏览器本地路径对服务端无意义）。两者至少提供一个、`script_content` 优先；审计 `command` 只记录 `topic` / `count`，不落脚本文本。
  7. **直观性要求（v1.3）**：批量注入页常驻「调用规则」卡，说明调用时机、每次返回量、总量公式与 ctx 语义，并随「计划总条数」实时估算调用次数；Dry-run 与正式执行结果均回报调用次数与构造条数，用户无需猜测脚本被调了几次；
  8. **ctx 可发现性要求（v1.3a）**：批量注入页另设常驻「ctx 速查」卡，逐条列出 `ctx` 的**全部** 5 个成员（成员名 / 类型或返回值 / 作用与边界 / 写法示例），并说明：每次调用都注入全新 ctx、脚本顶层变量跨调用保留、单次编译与调用各 5s 超时；同时给出返回消息对象的字段规格（`value` 必填字符串、`key` 可省略自动补、`headers` 对象、不支持 `partition`）、沙箱可用与不可用的全局对象清单，以及 ≥3 个可一键载入编辑器的可运行示例。**说明必须与 `callGenerate` 实际注入的字段严格一致，不得出现未实现的成员**（如 `ctx.now()`），避免用户照抄后报错。卡片默认展开、可收起且收起状态按浏览器持久化，收起态仍以速览行列出全部 5 个成员名；编辑器下方提供「查看 ctx 全部成员与用法 ↓」直达入口，避免长说明卡挡住 Dry-run / 执行按钮；
  9. **参数输入与操作可达性（v1.3b）**：「计划总条数 / 并发 / 速率」均为**可直接键入 / 粘贴**的数字输入框，不得使用只能 ±1 步进的 SpinButton；前端整数范围校验（count ≥ 1、并发 1~32、速率 ≥ 0）+ 禁用按钮 + 明确错误提示，提交时再校验一次；Dry-run / 执行按钮必须**紧跟脚本编辑器**且滚动时保持可达（`position:sticky; top:0` 贴顶常驻），「调用规则」与「ctx 速查」两张说明卡放在操作与结果区之后，不得把主操作按钮挤出首屏；**页面必须可滚动到底**：长内容不得被 Card 的 `overflow:hidden` 或固定高度容器裁剪，`main.scrollHeight` 必须大于 `clientHeight`，下方说明卡与示例可正常滚动查看。
#### FR-07 详细设计：`generate` 调用模型、返回量与总构造量（v1.3）

**1）调用模型（正式执行；dry-run 见第 4 点）**

工具只创建**一个 goja VM**、只跑**一个 generator goroutine**，`generate(ctx)` 被串行反复调用；构造出的消息进入有界队列，再由 N 个 worker 并发写入 Kafka。脚本顶层变量在多次调用之间**保留**，可用于维护自增 ID、状态机等。

```text
calls = 0; built = 0
loop:
  if built >= count            -> 正常停止（计划总条数已凑满）
  if cancelled / ctx 超时       -> 停止，标记 cancelled
  calls += 1
  out = generate({ index: calls-1, time, random, uuid, pick })   # 单次 5s 超时
  if out 是 null / undefined    -> 停止（脚本主动结束流）
  if out 是空数组               -> 报错终止（否则 built 永不增长会死循环）
  msgs = (out 是 Array) ? out : [out]
  for m in msgs:
      if built >= count        -> 停止（最后一批截断，绝不多写）
      enqueue(m); built += 1
```

**2）每次应该返回多少：三种合法返回值**

| `generate` 返回 | 本次构造条数 | 之后是否继续调用 | 典型用途 |
|---|---|---|---|
| 一个消息对象 `{key?, value, headers?}` | 1 | 继续 | **最常用**，一条一条造 |
| 非空数组 `[msg, msg, ...]` | 数组长度 K（超出 count 的部分丢弃） | 继续 | 一次造主记录 + 明细、批量事件 |
| `null` / `undefined` | 0 | **停止** | 数据造完 / 满足条件提前收尾 |
| 空数组 `[]` | — | **报错终止** | 非法，避免无限循环 |
| 抛异常 / 死循环 | — | **报错终止** | 5s 超时经 `vm.Interrupt` 打断 |

数组返回时同一批消息共享同一个 `ctx.index`（index 是**调用序号**，不是消息序号）。

**3）总构造数据量如何确定**

> **实际构造条数 = min(计划总条数 count, 脚本累计产出的消息条数)**
> **调用次数 ≈ ceil(count / 平均每次返回条数)**

| 计划总条数 | 脚本每次返回 | 实际调用次数 | 实际构造条数 |
|---|---|---|---|
| 100 | 1 个对象 | 100 | 100 |
| 100 | 2 条数组 | 50 | 100 |
| 100 | 3 条数组 | 34（第 34 次只取 1 条，其余截断） | 100 |
| 100 | 每次 1 条，但第 10 次返回 `null` | 10 | 9 |
| 100 | 空数组 | 1 | 0（报错） |

结论：**用户只需在界面上填「计划总条数」，不需要按脚本返回条数自己去换算**。脚本每次返回 1 条最直观；若每次返回多条，工具自动少调几次并在最后一批截断；结果中的 `generateCalls` / `messagesBuilt` 给出实际值。

**4）dry-run 的差异**：不发送任何消息，只调用 `generate` 直到凑满 **5 条样例**（或达到 count、或脚本返回 null），返回前 5 条样例以及本次的 `generateCalls` / `messagesBuilt`。因此 dry-run 的调用次数不能代表正式执行的调用次数，正式执行以 `count` 为准。

**5）可运行示例（UI 默认预填第一个）**

```js
// 每次返回 1 条：计划总条数 100 -> generate 调用 100 次
function generate(ctx) {
  return {
    key: "order-" + ctx.index,
    value: JSON.stringify({
      orderId: "ORD" + String(ctx.index).padStart(5, "0"),
      amount: Math.floor(ctx.random() * 1000) + 1,
      status: ctx.pick(["CREATED", "PAID", "FAILED"]),
      traceId: ctx.uuid(),
      ts: ctx.time
    }),
    headers: { source: "kp-script" }
  };
}
```

```js
// 每次返回 2 条：计划总条数 100 -> 约调用 50 次
// 第 2 条不写 key，工具自动补 kp-auto-*，分区仍由 key 决定
function generate(ctx) {
  return [
    { key: "order-" + ctx.index,
      value: JSON.stringify({ type: "ORDER", i: ctx.index, ts: ctx.time }) },
    { value: JSON.stringify({ type: "ITEM", order: ctx.index,
                              qty: Math.floor(ctx.random() * 5) + 1 }) }
  ];
}
```

**6）用户操作路径与常见错误**

路径：数据注入 → 脚本批量注入 → （编辑器已预填示例，可直接改或点示例模板）→ 选 Topic、填计划总条数 → **Dry-run 预览**（不写 Kafka，看 5 条样例 + 调用/构造计数）→ 执行 → 进度条与结果统计。

| 现象 | 原因 | 修正 |
|---|---|---|
| `script compile error: SyntaxError` | 写了 `export function` / `import` / `require` | goja 无 ES Module 与 IO，改 `function generate(ctx) {...}` |
| 消息 value 变成 `[object Object]` | `value` 直接返回了对象 | 用 `JSON.stringify(obj)`；`value` 必须是字符串 |
| `generate must return an object with a 'value' field` | 漏写 value 或返回裸字符串 | 返回带 `value` 的对象或对象数组 |
| `returned an empty array` | 返回 `[]` | 至少返回 1 条，或返回 `null` 表示结束 |
| 实际写入少于计划总条数 | 脚本提前 `return null` | 去掉或调整提前结束条件 |
| `script execution timeout` | 单次 generate 超过 5s | 简化脚本 / 去掉死循环 |
| `ReferenceError: console is not defined` | 脚本里写了 `console.log` | goja 沙箱未注入 `console`；改用 **Dry-run 预览** 查看前 5 条产出（v1.3a 实测） |
| 数量总是对的但分区集中 | 所有消息用了同一个 key | 让 key 随 `ctx.index` 变化；分区按 key hash |

**7）ctx 运行时契约与 Web UI「ctx 速查」卡（v1.3a）**

`ctx` 由 `internal/produce/batch.go:callGenerate` 在**每次调用前新建**；v1.3a 以真实 dry-run 实测 `Object.keys(ctx)` 恒为 `index,time,random,uuid,pick`。除此之外没有 `topic` / `count` / `profile` / `now()` 等成员，需要跨调用状态就用脚本顶层变量，需要外部信息就用标准 JS 表达。

| 成员 | 类型 | 语义 | 写法示例 |
|---|---|---|---|
| `ctx.index` | number | 调用序号，从 0 起每次 +1；一次返回数组时整批共享，**不是消息序号** | `key: "order-" + ctx.index` |
| `ctx.time` | number | 服务端当前 Unix 毫秒时间戳，同一次调用内固定 | `ts: ctx.time` |
| `ctx.random()` | number `[0,1)` | 独立均匀取样；同一毫秒内连续调用也互异 | `Math.floor(ctx.random()*1000)+1` |
| `ctx.uuid()` | string | UUID v4 字符串 | `traceId: ctx.uuid()` |
| `ctx.pick(arr)` | any | 数组随机取元素 / 对象随机取属性值；空数组与 `undefined` 返回 `undefined` | `ctx.pick(["PAID","FAILED"])` |

脚本环境实测（goja，内核未注入任何 IO 能力）：

| 可用 | 不可用 |
|---|---|
| `Math` `JSON` `Date` `String` `Array` `Object` `RegExp` `Map` `Promise` `globalThis`；`let/const`、箭头函数、模板字符串、`String.prototype.padStart` 等 ES2015+ 语法；脚本顶层变量跨 `generate` 调用保留 | `console` `require` `import` `export` `fetch` `setTimeout` `process` `module`；文件 / 网络 / 进程 IO；读取 Kafka 或 profile 配置 |

Web UI 速查卡 = 上述成员表 + 返回消息对象字段规格（`value` 必填字符串、`key` 可省略自动补 `kp-auto-*`、`headers` 对象、不支持 `partition`）+ 沙箱可用 / 不可用清单 + 3 个可一键载入编辑器的可运行示例（全成员 / 顶层状态 + `return null` 提前结束 / 数组 + headers）；说明表纳入全局「列宽可拖拽、双击按内容自适应」能力。想查看脚本产出只能走 Dry-run（返回 5 条样例），不能在脚本里 `console.log`。

- 验收：Given 脚本返回 `{value: JSON.stringify({orderId:"T"+ctx.index})}`，When `--count 3 --dry-run`，Then 输出 3 条样例、value 解析后 orderId 为 T0/T1/T2，且 Kafka 无新消息。
- 验收（v1.2c）：Given 脚本每次返回 2 条消息（其中 1 条无 `key`），When `--count 20 --dry-run`，Then 输出 5 条样例、无 `key` 的样例被补为 `kp-auto-*`、且各样例 `random()` / `pick()` / `uuid()` 结果互异；When `--count 20` 实际执行，Then `produced=20`（不因数组返回而超额）、回读每条消息均有唯一 key 且分区按 key 分散。
- 验收（v1.2f）：Given Web UI 提交脚本文本并点 Dry-run，Then 请求体含 `script_content`（源码文本）而非文件路径、返回 5 条样本且 Kafka 无新消息；Given `script` 与 `script_content` 均缺失，Then HTTP 400 并提示两种合法形状；Given 脚本 `while(true){}`，Then 约 5s 返回超时错误且不挂死；Given `generate()` 抛错且 `dry_run:false`，Then 返回含 `produced` / `failed` 的明确错误而非静默返回短批次。
- 验收（v1.3a）：Given 打开批量注入页，Then 「ctx 速查」卡默认展开、列出且仅列出 5 个成员（无 `ctx.now()` 等未实现字段），返回字段规格与沙箱清单齐全，说明表支持列宽拖拽 / 双击自适应；Given 点「收起说明」并刷新页面，Then 仍保持收起且速览行列出 5 个成员名，编辑器下方「查看 ctx 全部成员与用法 ↓」可重新展开并滚动定位；Given 点任一「载入此脚本」按钮，Then 编辑器内容替换为对应可运行脚本且 Dry-run 成功返回样例；Given 脚本内 `Object.keys(ctx)`，Then 输出恰为 `index,time,random,uuid,pick`；Given 脚本写 `console.log(...)`，Then 报 `console is not defined`（沙箱未提供）而非静默执行。
- 验收（v1.3）：Given 打开批量注入页，Then 编辑器已预填可运行示例、无文件选择控件、常驻调用规则卡且估算文案随计划总条数变化；Given 点两个示例模板，Then 编辑器内容切换为对应脚本且均可直接 Dry-run；Given `count=100` 且脚本每次返回 1 条，Then 结果 `generateCalls=100`、`messagesBuilt=100`、`produced=100`；Given 每次返回 2 条，Then `generateCalls=50`、`messagesBuilt=100`；Given 脚本返回空数组，Then 立即返回明确错误而非无限循环；Given 脚本第 10 次返回 `null`，Then `messagesBuilt=9` 且正常结束。
- 验收（v1.3b）：Given 打开批量注入页，Then 计划总条数 / 并发 / 速率均为可直接键入的 number 输入框（无 SpinButton 步进器），且 Dry-run / 执行按钮在脚本编辑器下方首屏可见、滚动查看说明卡时仍贴底可达；Given 计划总条数直接键入 `12345`，Then 估算文案实时显示「12,345」且请求 `count=12345`；Given 输入 `0` / `abc` / 并发 `99`，Then 显示红字校验信息且 Dry-run / 执行均禁用，不发请求；Given 点 Dry-run，Then 结果区出现在按钮下方、说明卡之前；Given 打开批量注入页并向下滚动，Then 主区域 `scrollHeight > clientHeight`（有滚动条）、「ctx 速查」卡与 3 个示例可见，且 Dry-run / 执行按钮在滚动后仍贴顶可见（rect.top ≥ 0、rect.bottom ≤ 视口高度），不被 Card 裁剪。

### FR-08 注入进度与中断

- 规则：每 500 条或每 500ms 上报一次进度（CLI 写 stderr 进度行；客户端经 SSE 推送；间隔可配置）；`Ctrl+C` 或客户端"停止"按钮触发 context 取消：停止生成新消息，等待在途批次完成（最长 10s），结果如实回报 `produced`（已确认写入条数）、`failed`、`cancelled:true`。已写入条目不回滚。
- 验收：When `--count 100000` 执行 3s 后 Ctrl+C，Then 输出 `produced` 为实际已确认条数且 `cancelled:true`，进程 10s 内退出。

### FR-09 操作审计（v1.2 修订：最终结果 + 有界命中快照 + 双阶段落库）

- 规则：
  1. 每次 op 记录：op_id、session_id、source（ai/human）、actor（agent 名 / 用户名）、intent（AI 声明的意图，可选）、命令（含完整参数，**>2KB 截断并标记 `commandTruncated:true`**——v1.2 新增，守住单条 <4KB 口径）、started_at / duration_ms、status（**running** / ok / error / truncated / cancelled / **crashed**——v1.2 新增 running 与 crashed）、result_summary、warnings、error；
  2. **双阶段落库（v1.2）**：op 启动即插入 `status=running` 行（含 op_id / session / 完整命令 / started_at），终态在完成 / 失败 / 取消时更新；`kp serve` 启动时将 started_at 超过 10 分钟仍为 running 的 op 标记 `error(crashed)`，硬崩溃不留悬挂记录；
  3. **result_summary 只存最终结果统计**：扫描条数 / 命中条数 / 注入成功数 / 失败数 / TPS / 耗时等数字与状态；produce 进行中经 result_summary 周期性原地更新（每 500 条 / 500ms），承接观测区实时进度（核心场景 3）；
  4. **有界命中快照（v1.2，替代"重新执行式重放"）**：scan 类 op 将**命中消息**（过滤后，非全量扫描）落一份快照——每 op ≤100 条且序列化 ≤1MB，先到为准，超界截断标记 `snapshotTruncated:true`；produce 类 op 不落快照；快照存 audit.db 独立 snapshots 表，计入 30 天 / 500MB 保留池；设置页提供「命中快照留证」开关（默认开，关闭后 scan 不落快照、重放入口置灰）；
  5. 重放 = 读取快照的纯本地展示：**不发起任何 Kafka 读写、不产生新 op**、不污染时间线（审计完整率口径注明"不含重放"）；
  6. `kp audit clear` 一并清理快照。
- 验收：Given AI 执行一次命中 42 条的 scan，When 在观测区点「重放」，Then 展示快照内容且期间未建立任何 Kafka 连接、审计无新增 op；When 查看 `kp audit show <op_id>`，Then operations 行 <4KB 且不含消息体；When `--value` 携带 1MB JSON 执行单条 produce，Then command 字段被截断至 2KB 且 `commandTruncated:true`。

### FR-10 会话管理（v1.1 修订：短 ID 显式携带）

- 规则：
  1. `kp session new [--label <l>]`：随机生成 6 位短 session_id（Crockford Base32，仅数字与无易混字母，如 `K3M9XT`），即生成即生效；32^6 ≈ 10 亿组合，30 天保留窗口内碰撞概率可忽略，写入时校验唯一、冲突自动重生成；
  2. SKILL 要求 Agent 使用能力前先调用 `kp session new`，其后**每条命令显式携带 `--session <id>`**——不依赖环境变量，命令自包含、可追溯；
  3. `kp session end <id>` 关闭会话；会话 24h 无新 op 自动结束并标记 `auto:true`（v1.2，防 AI 遗忘 end 导致会话永远 open）；未携带 `--session` 的命令作为独立 op（session_id 为空，按自身 started_at 参与保留滚动删除——v1.2 澄清）。
- 验收：Given 连续 3 次带 `--session K3M9XT` 的 scan，When `kp audit list --session K3M9XT`，Then 返回 3 条按时间排序的 op。

### FR-11 客户端操作区（人类可写）

- 规则：提供与 CLI 等价的可视化能力：**profile 完整管理（v1.2：新增 / 编辑 / 删除表单，字段含可选 SASL 认证与 TLS 分组，见 FR-13；凭据经内核 config 以 DPAPI 加密持久化，UI 层不落明文——首次使用零终端，对齐 §2.2"无命令行依赖"目标）**、topic 浏览、查询构建器（定位参数 + 三层过滤表单 + 表达式编辑器）、消息详情（含 hex 视图）、注入面板（单条 / 模板 / 脚本编辑器 + dry-run + 进度条 + 停止按钮）；人类操作同样写入审计（source=human）。**所有表格的列宽支持用户拖拽调整（双击表头手柄按内容自适应），宽度按「页面 + 表格 + 列」持久化到 localStorage**（v1.2b）；单条注入 Key 为必填项、界面不提供分区选择（FR-06）；topic 与分区均改为从已有数据下拉选择，时间条件使用带秒的时间控件，下拉面板经 Portal 渲染避免被滚动容器裁切（v1.2b）。
- 验收：客户端执行一次关键词查询后，`kp audit list` 能查到对应 op；在 UI 新建含 SASL+TLS 的 profile 后，`kp topic list --profile <新名>` 可用该连接信息成功连接。

### FR-12 客户端观测区（只读，AI 轨迹）

- 规则：仅提供只读接口（GET /api/audit/*、SSE /api/audit/stream）；UI 功能：会话列表 → 会话时间线（多步操作时序）→ op 详情（命令、参数、耗时、状态、**最终结果统计**、重放入口）；运行中的 op 实时流入；无任何写按钮；重放仅对 scan 类 op 开放，**读取命中快照做纯本地展示（GET /api/audit/{op_id}/snapshot），不发起任何 Kafka 读写、不产生新 op**（v1.2 修订）。
- 实时机制（v1.2 明确）：CLI 与 serve 为独立进程，serve 以 500ms 间隔轮询 SQLite（WAL 模式，读不阻塞写）感知新 op、状态迁移与 produce 进度变更，经 SSE 批量推送前端——观测区对 CLI 发起的 op 同样实时可见（含 running 徽标与注入进度，承接核心场景 3）。
- 验收：观测区页面发起的全部 HTTP 请求方法均为 GET；SSE 断开后自动重连且不丢 op（按 op_id 增量补拉）；CLI 发起 op 后 ≤1s 内出现在观测区并带 running 徽标；重放请求不产生新审计记录、期间无 Kafka 连接建立。

### FR-13 Profile 管理（v1.2 修订：连接认证显式化——SASL 与 TLS 独立可选）

- 规则：
  1. `kp profile add/list/use/show/remove`；字段：name、bootstrap_servers、**sasl（可选）**、**tls（可选）**；默认 profile 可设置；CLI `--profile` 覆盖默认值；客户端顶栏下拉切换；
  2. **bootstrap_servers 支持配置多个 broker 地址**：YAML 中为字符串数组（`["broker1:9092", "broker2:9092", "broker3:9092"]`），CLI `--bootstrap-servers` 接受逗号分隔（`broker1:9092,broker2:9092,broker3:9092`）；kgo 用该列表做初始连接发现全集群，至少配置一个可达地址即可；UI 表单为多行输入或逗号分隔文本框；
  3. **用户名密码认证（SASL）与 TLS 均为可选、互相独立**（D18），支持四种组合：

     | 组合 | sasl | tls | 典型场景 |
     |---|---|---|---|
     | 明文 | ✗ | ✗ | 本地开发集群（默认） |
     | SASL 认证 | ✓ | ✗ | 内网测试集群 |
     | TLS 加密 | ✗ | ✓ | 云集群（客户端校验服务端证书） |
     | SASL over TLS | ✓ | ✓ | 传输加密 + 身份认证双开 |

     > v1.2a 收窄：TLS 仅做**单向**——客户端用 CA 证书校验服务端身份，**不支持 mTLS 双向认证**（暂不需要服务端认证客户端证书）。

  4. profile 字段结构（YAML，v1.2a）：

     ```yaml
     name: dev
     bootstrap_servers: ["broker1:9092", "broker2:9092", "broker3:9092"]
     sasl:                      # 可选，缺省 = 不启用用户名密码认证
       mechanism: plain | scram-sha-256 | scram-sha-512
       username: dev-user
       password: <DPAPI 加密存储>
     tls:                       # 可选，缺省 = 不启用 TLS
       ca_cert: <PEM 路径，可选，校验服务端证书；缺省走系统信任链>
       skip_verify: false       # 直接跳过服务端证书校验，默认 false；true 时不校验证书、无需 ca_cert
     ```

     > v1.2a 收窄：`tls` 仅含 `ca_cert` 与 `skip_verify` 两字段——客户端校验服务端身份（单向），不保留 mTLS 客户端证书 / 私钥字段。

  5. CLI 参数（v1.2a）：`kp profile add` 支持 `--sasl-mechanism / --sasl-username / --sasl-password / --tls / --tls-ca / --tls-skip-verify`，均不传即不启用对应能力；`kp profile show` 输出密码外的敏感值脱敏（`****`）；
  6. SASL 密码经 Windows DPAPI 加密存 config.yaml，**只在内核 config 模块内解密使用，CLI / UI 层不感知**（D14 不变；UI 表单提交的凭据直接透传内核，不落明文）；`ca_cert` 为证书文件路径，非敏感数据，明文存储即可；
  7. `tls.skip_verify=true` 时直接跳过服务端证书校验、无需 `ca_cert`，连接结果附 `warnings:["tls-insecure"]`，客户端连接状态黄标提示；
  8. 连接失败错误按层归因（TCP 不通 / SASL 认证失败 / 证书校验失败），exit 3，错误 JSON 的 hint 给出对应排查建议；
  9. kgo 原生支持 SASL（PLAIN / SCRAM-SHA-256/512）与单向 TLS 组合，无额外依赖（§14）。
- 验收（GWT）：
  - Given profile 仅配 bootstrap_servers（多地址，无 sasl / tls），When `kp topic list --profile dev`，Then 以明文方式连接成功（kgo 自动从任一可达地址发现全集群）；
  - Given profile 配置 sasl(plain, 正确用户名密码) + tls(ca_cert)，When `kp topic list --profile secure`，Then 连接成功，输出含 `auth:"sasl+tls"` 且不含密码明文；
  - Given profile 密码错误，When `kp topic list --profile secure`，Then exit 3，message 含"认证失败"且 hint 指向凭证排查；
  - Given `tls.skip_verify=true`（不配 ca_cert），When 连接成功，Then 不校验证书且结果含 `warnings:["tls-insecure"]`。

### FR-14 机器可读命令契约

- 规则：`kp schema` 输出全部命令、参数、输出 JSON 结构、错误码的机器可读描述（JSON），供 SKILL.md 引用与 Agent 自省；`--help` 同样支持 `--format json`。
- 验收：`kp schema` 输出可被 jq 解析且覆盖 §9.1 命令清单**全部命令**（v1.2：口径由"FR-01~10"修正为命令清单全集，含 describe / profile / session / audit / serve）。

### FR-15 本地查询结果缓存（v1.4 新增）

- 背景与动机：现状（§12.1）每次查询都直接由 `scan.Scanner` 经 `client.PollFetches()` 从 broker 全量拉取；Web UI 五档回扫（100/500/1000/5000/10000）每次点击都是对 `min(分区数×N, 50000)` 条消息的 broker 重拉，重复查询浪费带宽且带来大对象内存抖动。在"broker 节点不变、topic 不变、offset 单调递增、数据只会增删不会更新"的前提下，可引入本地缓存作为**只读加速层**，把"同一时刻直连扫描的结果"物化为可复用的本地前缀，避免重复网络拉取。

- 前提假设与验证（缓存正确性的基础，需用户确认闭环，见 §18 的 A7~A10）：
  1. **offset 单调 + 值不可变（Kafka 天性，真正不可妥协）**：每分区 offset 只追加、消息体写入后永不修改 ⇒ 缓存值与同刻 broker 读取逐项等价，这是物化缓存成立的根基。
  2. **topic 不变 ⚠️ 需加固**：同名 topic 删后重建会让 offset 从 0 重来、旧数据无关；用 **Topic ID**（Kafka 2.8+）进缓存键，重建即视为新分区、绝不串数据。
  3. **broker 节点不变 ❌ 可弱化**：副本切换 / leader 迁移不改变逻辑 offset 空间；失效锚定每分区 `logStart` 与 `HWM` 即可，无需追踪 broker 身份。
  4. **数据只会增删、不会更新**：区分两类删除——非压缩 topic 段淘汰 / `DeleteRecords`（`logStart` 前移，缓存必须同步驱逐）；压缩 topic 墓碑（压实前旧值仍在，缓存忠实镜像、不比直连更错，但 UI/文档需说明"删除≠立即消失"）。

- 设计目标：① 有界——硬上限双约束，绝不无限占用本地磁盘（用户硬约束）；② 正确——缓存返回内容 = 同刻直连 broker 扫描结果；③ 零语义破坏——三种扫描模式行为不变，过滤在缓存原始字节上重写执行；④ 可观测可清除——占用 / 命中 / 淘汰可查，支持一键清除。

- 缓存模型——日志的**物化前缀**：对每个 `(profile, topicID, partition)` 维护区间 `[cachedStart, cachedEnd)`，始终是 broker `[logStart, HWM)` 的子集；键含 `topicID`（非仅 topic 名），防同名重建串数据；存储为 `cache_messages`（主键 `(profile, topic, partition, offset)`，幂等 `ON CONFLICT DO NOTHING`）+ `cache_meta`（全局 `total_entries / total_bytes` 计数），复用 modernc.org/sqlite、独立 `cache.db` + WAL（§11.4）。

- 对账协议（reconcile）——**保证与直连逐项等价**：每次 `Scan()` 在 `PollFetches` 前先对账：① `ListOffsets` 取每分区当前 `logStart` 与 `HWM`；② `EvictBelow(logStart)` 驱逐 offset < logStart 的缓存（段淘汰 / DeleteRecords 真实消失的数据）；③ 计算本次目标区间 `[from, to)`（`--last N`：`to=HWM, from=max(logStart, HWM-N)`；时间窗 / offset 区间按既有逻辑）；④ 缓存已覆盖 `[from, to)` 的部分从 `GetRange` 直取，仅对未覆盖缺口（含 `[cachedEnd, to)` 新增 + 必要时的 `[from, cachedStart)` 回填）调 broker；⑤ `Append` 写回新增缺口并推进 `cachedEnd`；⑥ 结果经现有三层过滤 / 归并 / 排序出，与直连路径完全一致。

- **双上限（本 FR 重点，用户硬约束）**：
  1. `MaxEntries`：全局缓存消息条数硬上限（默认 2,000,000，0 = 不限）。
  2. `MaxBytes`：全局**逻辑载荷**字节硬上限（默认 2 GiB，0 = 不限）；逻辑载荷 = Σ(`len(key)` + `len(value)` + Σ(`len(hdrKey)` + `len(hdrVal)`))，不含 SQLite 索引 / 页开销。
  3. 超限淘汰策略：`enforce()` 在每次 `Append` / `SetLimits` 后触发，按 `(profile, topic, partition, offset)` 升序**删除最旧（offset 最小）的条目**，单批 5000、循环至两侧均不超限；保留"最近尾部"——正是 `--last N` 实际命中的区间，且任何被淘汰项随时可从 broker 按需重建，不丢真数据（缓存只是加速层）。
  4. **物理空间口径**：`MaxBytes` 为逻辑预算；SQLite 索引 + 页 + WAL 使真实磁盘占用约为逻辑预算的 **1.3~1.6 倍**；设置上限时应留 headroom；`Compact()`（VACUUM）在 `Clear()` 或大删后回收空间；物理占用经 `Stats.PhysicalBytes`（db + wal + shm 之和）对外展示，避免"2GiB 上限 = 只占 2GiB"的误判。

- 守卫与失效：① Topic 重建守卫——`Append` 时若非空前 stored `topicID` ≠ 入参 `topicID`，先 `DELETE` 该 `(profile, topic)` 全量再写，杜绝旧 offset 空间串入；② 分区数增加——已有分区 offset 不受影响、缓存按原 partition key 继续有效，仅 `KeyPartition` 哈希映射会变（现有行为已如此）；③ 多进程并发——CLI 与 `kp serve` 共享 `appdata`，cache.db 走 SQLite 锁 + 单写者（典型仅 `kp serve` 写、CLI 只读），读路径 WAL 不阻塞写。

- 与三种扫描模式的收益（均兼容、不改语义）：

  | 模式 | 直连成本 | 加缓存后 |
  |---|---|---|
  | `--last N` 回扫 | 每次重拉 `min(分区数×N, 50000)` 条 | 首次建缓存；之后只拉 `[cachedEnd, HWM)` 新增；窗口完全落入缓存时**零数据传输**，仅 2 次 `ListOffsets` |
  | `--offset-from/-to` | 拉整段 | 已缓存段命中，只补缺失段 |
  | `--since/--until` 时间窗 | 仍需 `ListOffsets(时间戳)` 定位起点 | 起点定位 RPC 保留，数据从缓存出，窗越大收益越明显 |

- 设置项与透明标记：① 设置页新增「本地缓存」开关（默认开）、「最大条目」「最大物理空间(MB)」上限输入、「清除缓存」按钮 → 经 config + `SetLimits` 即时生效（无需重启）；② CLI 透传 `--no-cache` 临时绕过，`kp cache clear` 维护命令（可选，实现期确认）；③ 扫描结果新增 `served_from_cache: bool` 与 `cache_stats`（命中条数 / 补拉条数 / 占用），透明告知本次是否命中缓存，便于排障与信任建立。

- 安全：缓存载荷为原始消息字节（与 broker 同等敏感），落 `appdata/cache.db` 本地文件；建议沿用 `appdata` 目录权限隔离（与 config.yaml 同级），后续可选对该文件做 DPAPI 加密（实现期评估，不阻塞首版）；「清除缓存」与 `kp cache clear` 彻底抹除。

- 验收（GWT）：
  1. Given 已开启缓存，When 首次 `kp scan --last 10000`，Then 触发 broker 拉取并向 cache.db 写入该 topic 各分区前缀；When 第二次同样查询（期间无新增），Then 结果 `served_from_cache=true` 且 broker 实际拉取条数 ≈ 0（仅 `ListOffsets`），结果集合与首次逐项一致。
  2. Given 设 `MaxEntries=5`，When 向同一分区 Append 10 条（offset 0..9），Then 缓存存活 5 条且为 offset 5..9（最旧被丢弃），`Stats.TotalEntries=5`。
  3. Given 设 `MaxBytes=8`（单条逻辑 4B），When Append 5 条，Then 仅 2 条存活且为最大 offset 两条，`PhysicalBytes ≥ TotalBytes`（体现开销倍率）。
  4. Given 同 topic 名不同 topicID（重建），When Append 新数据，Then 旧 offset 数据被 purge，`GetRange` 仅返回新区间。
  5. Given `logStart` 由淘汰前移至 N，When `EvictBelow(N)`，Then offset<N 的缓存被驱逐、`GetRange` 不再返回。
  6. Given 设置页将上限调小，When `SetLimits`，Then 立即 trim 至新上限、不超预算。

- 实现落点 / 后续同步：本 FR 触及 `internal/cache`（新增）+ `scan.Scanner`（加可选 Cache 字段、reconcile 改造）+ `config`（上限配置）+ `Settings.tsx`（UI）+ CLI `--no-cache` / `kp cache`；按纪律需同步 `README.md` 命令参考、`kp schema`、本变更记录、以及 `skill/SKILL.md`。**本版先出设计文档**，`internal/cache` 已有原型草稿（cache.go + cache_test.go，已实现双上限 / 守卫 / 对账接口），待评审后按本文对齐实现并补齐上述同步项。

> 【架构图待经 drawio MCP 补充】：新增"缓存层位置图"与"对账时序图"（对应 figures/6），禁止手改 `.drawio` XML；本节先用文字 + 表格描述，图文一致待 MCP 导出 PNG 后闭合。

### FR-16 数据目录可配置与一次性迁移（v1.5 新增）

- **背景与动机**：v1.4 之前所有用户数据固定落在 `%APPDATA%/kafka-pilot`（Linux / macOS `~/.kafka-pilot`），与二进制分离，换机器 / 拷目录即丢失，且不便多份工具并存。用户要求「profile 与 data 可自定义配置」，并默认与 `kp.exe` 放在一起，同时把本机历史数据一次性迁过去。

- **目录布局与优先级**：

```
<kp.exe 所在目录>/
├── kp.exe
├── kp-settings.yaml     # 配置指针：固定放 exe 旁，不在数据目录内
├── kp.pid               # 服务 PID（同样放 exe 旁，见下）
└── kp-data/             # 默认数据目录（可改）
    ├── config.yaml      # profile（凭据 DPAPI / base64 加密）
    ├── audit.db         # SQLite WAL；运行时另有 audit.db-wal / -shm
    └── cache.db         # 本地缓存（FR-15 实现后）
```

  解析优先级：**① `KP_APPDATA` 环境变量（测试覆盖，设置时不触发旧目录迁移）→ ② `kp-settings.yaml` 的 `data_dir`（相对路径基于 exe 目录、绝对路径原样）→ ③ 默认 `DefaultDataDir() = <exe 目录>/kp-data`**。`kp-settings.yaml` 的位置必须固定，否则找不到指向数据目录的指针；高级场景可用 `KP_SETTINGS` 覆盖其路径。

- **Settings 契约**：`Settings{data_dir, migrated_from, migrated_at, migration_done}`，YAML 落盘 0600。首次 `LoadSettings()` 会持久化默认值，使选择稳定；`migration_done` 是一次性迁移的幂等标记。`DataLayout()` 产出双端共享的只读布局结构（生效 / 默认 / 旧版目录、settings 与各文件路径、存在标记、迁移标记、环境变量覆盖），CLI 与 REST 渲染同一结构，杜绝口径漂移。

- **一次性迁移契约（安全红线）**：
  1. 源为旧位置 `LegacyAppDataDir()`（Windows `%APPDATA%/kafka-pilot`，Linux / macOS `~/.kafka-pilot`），目标为当前解析出的数据目录；
  2. **只复制、不删除**源文件（随时可回滚）；目标已存在的同名文件**绝不覆盖**；
  3. `audit.db` + `audit.db-wal` + `audit.db-shm` 必须作为**一组整体复制**：若目标已有 db，则三个文件一个都不复制，避免把不匹配的 WAL 混入已有库造成损坏；旧进程被强杀时 WAL 尚未 checkpoint，整组复制可让新进程打开时正常重放；
  4. `migration_done` 写入 `kp-settings.yaml` 后不再自动执行；`kp settings migrate --force` / `POST /api/settings/migrate {"force":true}` 只做复查，仍不覆盖；
  5. 迁移前建议 `kp stop`：服务正在写 WAL 时复制可能少最后几条审计记录（缓存 / 审计均为可重建或可容忍的本地数据，不丢 Kafka 数据）。

- **切换目录契约**：`SetDataDir(value, currentDir, copyCurrent)` 是 CLI 与 REST 共用的**唯一用例**：校验非空 → 解析并创建目录 → 写探针验证可写 → `copyCurrent` 为真时把当前目录的 `config.yaml` + audit 三件套复制过去（默认开启，防止 profile「凭空消失」）→ 保留迁移标记并保存相对 / 绝对原值。返回的 `UpdateResult`（`status / data_dir / configured / previous_data_dir / settings_path / copied / skipped / restart_required / hint`）两端字段完全一致。

- **重启语义与 PID 文件**：长运行服务在启动时解析一次目录并持有 SQLite 连接，改目录 / 迁移后必须 `kp restart` 才切换；`GET /api/settings` 额外给出 `active_data_dir` 与 `pending_change`（配置值 ≠ 生效值）。`kp.pid` 放在 **exe 同目录**、与数据目录解耦，读取时兼容查找数据目录 / 旧版 app-data 目录下的旧 pid 文件，保证改目录后 `kp stop` / `restart` 仍能定位服务。

- **三端对称（§7 纪律）**：

| 能力 | CLI | REST | Web UI |
|---|---|---|---|
| 查看布局 | `kp settings show` | `GET /api/settings` | 设置页「数据目录」卡片路径表 |
| 修改目录 | `kp settings set-data-dir <path> [--copy-current=false]` | `PUT /api/settings {"data_dir","copy_current"}` | 输入框 + 保存 / 恢复默认 / 复制开关 |
| 一次性迁移 | `kp settings migrate [--force]` | `POST /api/settings/migrate {"force"}` | 「从旧位置迁移」按钮 |
| 命令自描述 | `kp schema` 命令清单含 `settings` | `GET /api/schema` | — |

  `settings` 命令为纯本地文件管理：不连 Kafka、不需要服务运行、不写审计记录（故不走 `NewOpContext` / `FinishOp` 埋点），CLI 侧以 `PersistentPreRunE` no-op 跳过 `app.New()` 以避免副作用。

- **验收（GWT）**：
  1. Given 全新目录且无 `kp-settings.yaml`，When 首次启动，Then 生成 `data_dir: <exe>/kp-data` 并把旧位置 `config.yaml` / audit 三件套复制进来；再次启动不再复制（`migration_done=true`）。
  2. Given 目标已有 `audit.db` 而旧位置有 `-wal` / `-shm`，When 迁移，Then 三个 SQLite 文件一个都不复制，目标库不被污染（单测覆盖）。
  3. Given 数据目录 A 有 3 个 profile，When `kp settings set-data-dir B`，Then B 下出现 `config.yaml` 与 `audit.db`，A 中文件保留，`restart_required=true`。
  4. Given 服务在数据目录 A 运行，When `PUT /api/settings` 改为 B，Then `active_data_dir=A`、`pending_change=true`，`kp restart` 后 `active_data_dir=B` 且 profile / 审计记录与切换前一致。
  5. Given 迁移完成，When `kp settings migrate --force`，Then `copied=[]` 且目标文件哈希不变。

- **实现落点**：`internal/config/settings.go`（`ExeDir` / `DefaultDataDir` / `Settings` / `DataLayout` / `DataDir` / `MigrateData` / `CopyDataDir` / `SetDataDir` / `copyLayout`）+ `settings_test.go`；`internal/config/config.go`（`NewManager` 走 `DataDir()`）；`internal/app/app.go`（`DataDir` / `LegacyDir` 字段、audit 路径）；`cmd/kp/main.go`（`settings` 命令、schema）、`cmd/kp/service.go`（pid 兼容查找）；`internal/serve/server.go`（3 个端点）；`web/src/{api.ts,types.ts,pages/Settings.tsx,index.css}`；文档同步 `README.md` / `AGENTS.md` / `skill/SKILL.md`。`kp schema` 与 `GET /api/schema` 共用 `app.CommandSchema()`（同一份命令清单 + 退出码），从结构上避免两端清单漂移。架构图模块划分未变（config 包内扩展），按「图表维护约定」**无需重绘 figures/1~5**。

## 七、总体架构设计

![总体架构](figures/1-总体架构.png)

分层职责：

| 层 | 职责 | 关键约束 |
|---|---|---|
| 使用方 | AI Agent（Skill over CLI）、人类（客户端） | — |
| 接入层 | CLI（cobra，JSON 输出 + 稳定退出码）；本地服务（127.0.0.1 HTTP + SSE，本地工具无认证鉴权） | 接入层不做业务逻辑，只做参数解析 / 渲染，不接触 Kafka 凭证 |
| 用例层 | `internal/app`：Scan / Produce / Inspect / Audit 四类用例 + 审计埋点 | **CLI 与客户端共用同一条执行链路**——这是"同一套内核"的落点 |
| 内核层 | config / kafka / scan / filter / produce / audit / cache 七个模块（cache 为本版新增，FR-15） | 不反向依赖接入层；模块间只经 app 组合；Kafka 凭证仅在 config 模块内解密使用 |
| 外部依赖 | Kafka 集群；本地存储（SQLite） | — |

**为什么这样分层**：用户的核心诉求之一是"除了交互层，底层公用同一套内核"。用例层是唯一入口意味着：AI 走 CLI 得到的结果，与人在客户端点同样的操作得到的结果，来自同一段代码、产出同样的审计记录——双端一致性不靠约定，靠结构保证。

## 八、内核模块设计

![内核模块分解](figures/2-内核模块分解.png)

| 模块 | 职责 | 关键设计 |
|---|---|---|
| `config` | profile 增删改查；**数据目录解析（`kp-data` 默认 + `kp-settings.yaml` 可配置 + `KP_APPDATA` 覆盖）、一次性迁移与目录切换**（FR-16）；凭据加解密（Windows DPAPI） | 配置文件 YAML；`kp-settings.yaml` 固定放 exe 旁、数据目录可移动；`config.yaml` / audit 三件套整组复制、不覆盖不删除；凭据密文存储，开发环境可选明文并显式标注；**Kafka 凭证只在内核持有，CLI / UI 不感知** |
| `kafka` | 连接池（按 profile 缓存 Client，多 broker 发现、SASL / 单向 TLS 组合按 profile 配置化）、元数据获取、offset 定位、lag 计算 | 基于 franz-go（kgo），纯 Go 无 CGO，原生支持 SASL（PLAIN/SCRAM）与单向 TLS（CA 证书 / 跳过校验） |
| `scan` | 分区并行读取、时间 / 条数边界计算、跨分区归并排序、截断判定 | 每分区独立 reader（direct assign）；归并按 timestamp 倒序；扫描在内存中完成（≤10000 条，v1.2e） |
| `filter` | 三层过滤：key → keyword → 表达式；Decoder 插件接口 | 三层串联短路：前一层数量少时后一层计算量随之减少；gojq 程序预编译、单次扫描复用；内置 text / json 两个 Decoder，json 为默认，预留自定义插件注册 |
| `produce` | 单条注入 + 脚本生成器 + 背压管道 + 限速 | 见第十二章流程 4 |
| `audit` | op / session 摘要与**有界命中快照**入 SQLite（operations 行不含消息体，scan 命中快照独立表存储）、双阶段落库、会话聚合、保留策略清理（含快照） | SQLite 用 modernc.org/sqlite（纯 Go，免 CGO，保持单二进制交叉编译能力） |
| `cache` | 有界物化前缀缓存（FR-15）：`cache_messages` + `cache_meta` 双表，Append/GetRange/EvictBelow/EvictPartition/Clear/SetLimits/enforce，双上限（MaxEntries/MaxBytes）淘汰最旧 offset；复用 modernc.org/sqlite、独立 `cache.db` + WAL | 幂等写入（同 offset 重写为空操作）；Topic ID 重建守卫；`enforce()` 保证不超预算；与 scan 解耦，缓存丢失可随时从 broker 重建 |

**依赖方向约束**：`cmd/kp → internal/app → internal/{config,kafka,scan,filter,produce,audit} → 第三方库`。内核模块禁止反向 import 接入层；app 层不做协议解析只做编排；禁止隐式全局单例（连接池由 config 显式持有）。

## 九、CLI 与 Skill 设计

### 9.1 命令清单

```
kp profile list|add|use|show|remove
        # add 支持（均可选，v1.2a）：--sasl-mechanism/--sasl-username/--sasl-password
        #                         --tls/--tls-ca/--tls-skip-verify
        #   bootstrap-servers 支持逗号分隔多地址（broker1:9092,broker2:9092）
kp topic list [--filter <kw>]
kp topic describe <topic>
kp group list [--topic <t>]
kp group lag <group> [--topic <t>]

kp scan --topic <t> [--partition <n>[,<n>...]]
        [--last <n>] [--since <ts> --until <ts>]
        [--offset-from <n> --offset-to <n>]
        [--key <k>] [--key-regex <re>] [--keyword <kw>] [--ignore-case]
        [--expr '<jq>'] [--timeout <s>]
        [--format json|table] [--output <file>]    # --limit 已取消（v1.2，--last 即唯一回扫上限）

kp produce --topic <t> --key <k> (--value <v> | --value-file <f>) [--header k=v]   # key 必填；分区按 key 自动判定，无 --partition（v1.2b）
kp produce --topic <t> --script <gen.js> [--count <n>] [--concurrency <n>] [--rate <n>] [--dry-run]   # --script 为服务端本地文件路径（v1.2f）
                                                                                                      # HTTP: POST /api/produce/batch 用 script_content 传内联源码，或 script 传路径

kp session new [--label <l>] | end <id> | list | show <id>
kp audit list [--session <id>] [--source ai|human] [--since <ts>]
kp audit show <op-id>
kp audit clear [--before <ts>]     # 手动清空审计

kp settings show                                  # 查看数据目录布局（v1.5 / FR-16）
kp settings set-data-dir <path> [--copy-current=false]   # 改目录（默认复制现有数据）
kp settings migrate [--force]                     # 一次性从旧 %APPDATA% 位置迁移

kp schema                      # 机器可读命令契约（命令清单含 settings）
kp serve [--port <n>]          # 启动本地服务（浏览器访问）
kp desktop                     # 启动服务 + 打开客户端窗口

# 全局参数：--profile <p> · --session <id> · --intent <text> · --format json|table
```

### 9.2 输出契约（面向 AI 的关键设计）

- 默认输出 JSON（单对象；进度场景为 NDJSON 流）；`--format table` 供人类快速查看。
- 查询结果结构固定：

```json
{
  "opId": "op_01H...",
  "topic": "orders",
  "total": 5000,
  "matched": 42,
  "truncated": false,
  "warnings": ["scan>1000"],
  "elapsedMs": 1830,
  "messages": [
    {"partition": 2, "offset": 88121, "timestamp": "2026-11-07T10:23:11.320+08:00",
     "key": "A1001", "value": {...}, "headers": {"traceId": "..."}}
  ]
}
```

- **字段口径（v1.3c）**：`total` 是本次实际读取并检查的原始消息数（≈ 分区数 × `--last`），**不是 topic 总消息数、也不是命中数**；`matched` 是过滤后**全部**命中数，各分区结果都保留，不再全局截断至 `--last`；`matched_raw` 仅截断时出现，为截断前真实命中数；`partition_ranges` 给出各分区实际扫描区间，UI 据此显示覆盖分区数并生成结果分区筛选条。`warnings` 为机器码（`scan>1000` / `result-cap` / `timeout`），CLI / 审计保持原样，Web UI 译为中文人话。
- 退出码：`0` 成功；`2` 参数 / 校验错误；`3` 集群连接失败；`4` 超时或被中断（仍输出部分结果，body 内 `truncated/cancelled` 标记）；`5` 内部错误。错误 JSON 含 `code`、`message`、`hint`（给 AI 的修正建议）。

### 9.3 SKILL.md 要点

| 部分 | 内容 |
|---|---|
| 触发条件 | 用户提到 Kafka 查数据 / 造数据 / 看积压 / 排查消息丢失 |
| Always do | 任务开始先 `kp session new` 取 6 位短 ID，其后**每条命令携带 `--session`**；查询先窄后宽（先 last 100 再扩大）；每次写明 `--intent` |
| Ask first | 注入条数 > 1000 时先向用户确认 |
| Never do | 不得对生产 profile 执行 produce；不得尝试 topic 增删改（工具未提供）；不得绕过回扫上限（`--last ≤ 10000`）；**不得执行 `kp audit clear`（防抹除操作痕迹，v1.2 增补）** |
| Playbook | 排查消息丢失（时间窗 + key 对比上下游）；压测造数（脚本模板 + dry-run → 正式执行） |
| 错误恢复 | exit 2 → 按 hint 修正参数重试；exit 3 → 检查 profile / 网络并告知用户 |

## 十、Windows 客户端设计

### 10.1 形态

`kp desktop` 启动本地服务（127.0.0.1 随机端口；本地单机工具，**不做认证鉴权**）→ 以 WebView2 窗口（Windows 10/11 系统自带）或默认浏览器打开。前端为 React + TypeScript 静态资源，`embed.FS` 打进同一个二进制——**分发物始终只有一个 kp.exe**。

**设计语言：Fluent Design（v1.1 新增）**

| 维度 | 规格 |
|---|---|
| 组件库 | Microsoft 官方 Fluent UI React v9（@fluentui/react-components），原生贴合 Windows 11 观感 |
| 材质 | 标题区 / 侧边导航应用 Mica / Acrylic 材质，内容区保持纯色，避免全屏毛玻璃影响数据可读性 |
| 几何 | 4px 控件圆角、8pt 网格间距、卡片式信息分区 |
| 色彩 | 跟随系统亮 / 暗主题 + Windows 系统强调色；highLag / 失败 / 截断用 Fluent 语义色（红 / 橙 / 灰），状态一眼可辨 |
| 布局原则 | 简洁至上：每页一个主任务，单层侧边导航，列表 + 详情双栏结构，无装饰性元素 |
| 控件 | CommandBar（批量操作）、DataGrid（消息列表，虚拟滚动支撑 10000 条）、RadioGroup（回扫量固定 5 档，v1.2e）、内联性能提示（>1000 档位，替代 Dialog 确认）、等宽代码编辑器 textarea（粘贴 JS + 示例模板，v1.3）、调用规则说明卡（v1.3）、ctx 速查表 / 可载入示例卡（v1.3a）、数字 Input type=number（三个注入参数可直接键入，v1.3b）、sticky 操作按钮组（v1.3b）、ProgressRing + Bar（注入进度）、Breadcrumb（topic 层级） |
| 动效 | 克制：页面切换 / 展开收起 150~250ms，注入进度用连续动画，不添加无关特效 |
| 密度 | 紧凑 / 舒适两档可切；消息列表默认紧凑密度 |

### 10.2 信息架构

```
顶栏：profile 下拉 · 连接状态指示 · 服务端口
侧边导航（Acrylic 材质，Fluent 图标）：
├── Topics（列表：名称 / 分区数 / 分区最新 offset，v1.1b 信息极简）
├── 消息查询（查询构建器 + 结果列表 + 消息详情含 hex）
├── 数据注入（单条 / 脚本 / dry-run / 进度条 / 停止）
├── Consumer Groups（列表 / lag 排行 / highLag 高亮）
├── AI 观测区（只读：会话 → 时间线 → op 详情 + 最终结果统计 + 快照重放）
└── 设置（profile 管理（含 SASL / TLS 表单，v1.2）/ 保留策略 / lag 阈值 / 注入默认参数 / 命中快照留证开关）
```

### 10.3 关键交互

| 场景 | 交互 |
|---|---|
| 查询 > 1000 条 | 回扫量选中 5000 / 10000 档时，扫描按钮旁显示内联性能提示（"回扫 N 条，可能耗时较长"），不阻断扫描；同时在结果 `warnings` 中返回 `scan>1000`（v1.2e 替代原 Fluent Dialog 确认框） |
| 多分区查询结果（v1.3c） | 各分区命中**全部返回**、不再全局截断。结果表上方「结果分区」chip 条默认选中「全部」，列出每个有命中的分区及条数；点分区即本地筛选、分页与计数联动、不重新扫描。统计条标签「已扫描 / 命中 / 耗时 (ms)」并常驻口径说明；`warnings` 内部码翻译成中文人话（`last-truncated` / `result-cap` / `scan>1000` / `timeout`），不直接透出裸码 |
| 表达式编辑 | 输入即本地预编译校验，语法错误即时红字提示（不发请求） |
| 批量注入 | 直接粘贴 / 编辑 JS（默认预填可运行示例，可一键切换「每次 1 条 / 每次 2 条 / ctx 全成员」等模板）→ **在数字输入框直接键入**计划总条数 / 并发 / 速率（范围非法即红字提示并禁用按钮，v1.3b）→ 编辑器正下方 **sticky 常驻**的 Dry-run / 执行按钮（v1.3b）→ 进度条 + 实时 TPS → 完成显示调用次数、构造条数、成功 / 失败分布 → 结果与按钮之后的「调用规则」卡与「ctx 速查」卡（全部 5 个成员、返回值字段、沙箱边界、3 个可载入示例，v1.3a）作为参考说明，不遮挡主操作 |
| AI 观测区 | 运行中 op 有"进行中"徽标（serve 轮询 SQLite + SSE 推送，≤1s 延迟）；op 详情展示最终结果统计；scan 类 op 提供「快照重放」按钮，读取命中快照纯本地展示，不发起任何 Kafka 读写（v1.2） |
| 空状态 | 未配置 profile 时全站引导页；查询无结果给"扩大范围"建议按钮 |

### 10.4 界面实现基线（v1.3d 修订）

界面设计与交互**以 `web/src` 业务前端为唯一实现基线**；设计变更直接修改业务前端，并同步本文档对应的 FR 与 §10.1~10.3 描述。各页面实现要点：

| # | 页面 | 实现要点 |
|---|---|---|
| 1 | Topics | 列表 + 详情双栏，信息极简：仅名称 / 分区数 / 分区最新 offset；右侧展开逐分区最新 offset；lag 观测收敛至 Consumer Groups 页（v1.1b） |
| 2 | 消息查询 | 查询构建器（定位维度 + 三层过滤，jq 即输即校验；回扫量为 5 档单选，旁注「每分区各回扫 N 条、总量 ≈ 分区数 × N」）→ **「按 key 定位分区（实验性）」显式开关**（默认关闭、扫描全部分区；填 Topic + 精确 key 后可启用，实时算分区并预置分区多选，结果标注只扫的分区与漏数据风险，v1.3e）→ 统计条（已扫描 / 命中 / 耗时 / 截断 + 常驻口径说明，v1.3c）→ **结果分区筛选条**（默认「全部」，各分区命中全返回、可按分区本地筛选，v1.3c）→ 结果表 + 行展开（JSON/Hex 双视图）；含 >1000 档位内联性能提示示例，告警以中文解释而非裸码；页面整体可滚动到底（v1.3e 修复 `.kp-fill` 裁切） |
| 3 | 数据注入 | 单条 / 脚本批量选项卡；**直接粘贴的 JS 编辑器**（默认预填示例 + 三个示例模板，无文件选择器）+ 编辑器下方**常驻 Dry-run / 执行按钮**（v1.3b）+ 参数卡（计划总条数 / 并发 / 速率均为可直接键入的数字输入框，v1.3b；分区策略固定按 key）+ 背压管道进度（TPS / 已发送 / 错误，中止不回滚）+ 结果区之后的 **「调用规则」卡**与 **「ctx 速查」卡**（全部 5 个成员表 / 返回值字段规格 / 沙箱可用与不可用清单 / 3 个可载入示例，v1.3a） |
| 4 | Consumer Groups | 只读 lag 排行 + 分区级明细联动，无 offset 重置入口 |
| 5 | AI 操作审计（浅色） | 三栏：会话（短 ID + 来源徽标）→ 时间线（状态色 + 最终结果统计 + 运行中 SSE）→ op 详情（含「快照重放」，读取本地快照纯展示） |
| 6 | 设置（深色） | **数据目录卡（v1.5 / FR-16）**：生效 / 配置 / 默认 / 旧版目录与各文件路径定义表、迁移状态徽标、目录输入 + 保存 / 恢复默认、「同时复制现有数据」开关、「从旧位置迁移」按钮、重启提示；profile 管理（含可选 SASL 认证 / TLS 分组表单，v1.2）/ 注入默认参数 / 审计保留 / 命中快照留证开关 / 外观（跟随系统主题、强调色、密度）；页面整体可滚动（`.kp-fill--settings`） |

实现以 Fluent UI React v9 组件为准，主题 / 强调色 / 密度跟随系统；架构图 1~5 与 drawio 源文件仍按「图表维护约定」通过 drawio MCP 维护。

## 十一、数据与存储设计

### 11.1 存储布局

v1.5 / FR-16：数据目录默认与二进制同级，可经 `kp-settings.yaml` 的 `data_dir` 指到任意目录；旧 `%APPDATA%/kafka-pilot`（Linux / macOS `~/.kafka-pilot`）只作为一次性迁移的**源**，迁移后保留但不再使用。

```
<kp.exe 所在目录>/
├── kp.exe
├── kp-settings.yaml     # 固定：data_dir / migrated_from / migrated_at / migration_done
├── kp.pid               # 固定：服务 PID + 端口（与数据目录解耦）
└── kp-data/             # ← data_dir（默认，可改；相对路径基于 exe 目录）
    ├── config.yaml      # profiles（凭据 DPAPI 加密）
    ├── audit.db         # SQLite（WAL 模式，存审计摘要、最终结果与 scan 命中快照；operations 行不含消息体）
    ├── audit.db-wal     # 与 audit.db 同组，迁移 / 切换目录时必须整组复制
    ├── audit.db-shm
    ├── cache.db         # FR-15 实现后的本地查询结果缓存
    └── logs/            # 内核结构化日志（排障用，非审计）
```

迁移规则与 CLI / REST 契约见 §6 FR-16；`KP_APPDATA` 环境变量仍可整体覆盖数据目录（测试用，此时不触发迁移）。

### 11.2 核心表

**operations**

| 字段 | 类型 | 说明 |
|---|---|---|
| op_id | TEXT PK | `op_` + ULID |
| session_id | TEXT NULL | 所属会话（6 位短 ID） |
| source | TEXT | ai / human |
| actor | TEXT | agent 名或用户名 |
| intent | TEXT NULL | AI 声明的意图 |
| command | TEXT | 完整命令（含参数；>2KB 截断并标记 `commandTruncated:true`，v1.2） |
| started_at | INTEGER | unix ms |
| duration_ms | INTEGER | |
| status | TEXT | running / ok / error / truncated / cancelled / crashed（v1.2：双阶段落库新增 running，崩溃收敛新增 crashed） |
| result_summary | TEXT(JSON) | **仅最终结果统计**：扫描数 / 命中数 / 注入成功 / 失败 / TPS 等 |
| warnings | TEXT(JSON) | |
| error | TEXT NULL | code + message |

> v1.2 约束：operations 行不含任何消息体，单条 < 4KB（command 截断保底）；原 payload_ref 字段取消；命中消息存独立 snapshots 表。

**sessions**：session_id PK（6 位短 ID）、label、started_at、ended_at、op_count、auto（24h 自动结束时为 true，v1.2）。

**snapshots**（v1.2 新增）：op_id PK、session_id、created_at、messages（JSON，命中消息，单 op 序列化 ≤1MB）、truncated（超 100 条 / 1MB 截断时为 true）；随 operations 同批次滚动清理。

**索引**：`operations(session_id, started_at)`、`operations(source, started_at)`。

### 11.3 保留策略

- SQLite 摘要与命中快照：合计保留 30 天或总量 500MB，超限滚动删除最旧 session 及其 op（含快照）；无 session 的独立 op 按自身 started_at 参与滚动删除（v1.2 澄清）；每日清理任务，`kp serve` 启动时触发；
- **operations 行不落任何消息体**；scan 命中内容以有界快照（≤100 条 / ≤1MB / op）落 snapshots 表；内容核对走「快照重放」（FR-09）；
- 手动清理：`kp audit clear`（可按时间截点，一并清理快照）。

## 十二、关键流程

### 12.1 查询链路

![查询链路时序](figures/3-查询链路时序.png)

要点：一次命令 = 一个 op_id 贯穿；ListOffsets 定位（时间窗 → offset 区间，last N → **逐分区** HWM 回推 N 条；v1.3c：命中 ≤ 分区数 × N，各分区结果全部返回，不再全局截断）；每分区独立 reader 并行 fetch；三层过滤串联短路；仅安全上限 / 超时才截断，告警随结果返回并落审计（最终结果 + 命中快照）。

### 12.2 注入管道

![注入管道与背压](figures/4-注入管道与背压.png)

要点：有界队列（cap 1024）实现背压——内存占用不随总条数增长（1 万与 50 万条内存峰值相同）；生成与发送解耦，脚本慢不拖垮 broker、发送慢自动反压生成器；取消只停新任务，已确认写入不回滚、如实回报。

### 12.3 审计与只读回放

![审计与只读回放](figures/5-审计与只读回放.png)

要点：AI 与人类操作落同一张表、同一套字段，`source` 区分；op 双阶段落库（启动插入 running → 终态更新），serve 轮询 SQLite 并经 SSE 推送观测区实时流入；**operations 行只存最终结果、不含消息体**，scan 命中内容落有界快照（≤100 条 / ≤1MB）——需要核对内容时「快照重放」纯本地读取展示，不发起任何 Kafka 读写；观测区仅有 GET + SSE 只读通道，服务端不暴露写接口。

## 十三、非功能设计

| 类别 | 要求 |
|---|---|
| 性能 | 默认查询 P95 ≤ 3s；注入 ≥ 5000 TPS（并发 8、不限速，估算值）；扫描内存两档口径（v1.2 修订，替代自相矛盾的单一 100MB）：默认场景（`--last 100`，12 分区共 1200 条）≤ 50MB；最坏情形为 v1.3c 结果安全上限 50000 条 × 64KB 截断值 + 元数据【仅极端 topic 触发，且按分区均摊保留、所有分区仍有结果；常规查询命中数 = 分区数 × `--last`，远低于上限，待实测校准】 |
| 安全 | 服务只绑定 127.0.0.1；CLI 与 UI 无认证鉴权（本地单机工具定位，D14 确认）；Kafka 凭证（SASL 密码）DPAPI 加密且仅内核 config 持有，show / 日志 / 审计全程脱敏；ca_cert 为文件路径、非敏感明文存储；gojq / goja 双沙箱均无 IO；CLI 硬拒绝 `--last` > 10000（v1.2e）；`tls.skip_verify` 开启时连接结果显式告警 |
| 兼容 | Windows 10/11 x64（WebView2 系统自带）；Kafka 2.x / 3.x（kgo 兼容）；连接安全：明文 / SASL（PLAIN、SCRAM-SHA-256/512）/ 单向 TLS（CA 证书校验服务端 / 可跳过校验）/ SASL over TLS 四种组合可选（FR-13、D18） |
| 可观测 | 内核 slog 结构化日志写 `<data_dir>/logs/`（默认 `kp-data/logs/`，v1.5 起随数据目录走），与审计分离（日志是排障用，审计是追溯用） |
| UI 设计 | Fluent Design（D17）：Fluent UI React v9、跟随系统主题与强调色、8pt 网格、动效克制 |
| 可访问性 | 客户端关键操作键盘可达，亮暗主题下文字对比度均 ≥ 4.5:1 |

## 十四、技术选型

| 组件 | 选型 | 理由 | 备选 |
|---|---|---|---|
| Kafka 客户端 | franz-go (kgo) | 纯 Go、性能好、direct assign 支持完善、SASL（PLAIN/SCRAM）与单向 TLS 组合支持完善、维护活跃 | sarama（API 老）、confluent-go（CGO 依赖） |
| jq | gojq | 纯 Go、兼容 jq 语法、可预编译 | 内嵌解释器自研（成本高） |
| JS 引擎 | goja | 纯 Go、ES2017+、无进程隔离开销 | 外部 node 子进程（违背单二进制） |
| SQLite | modernc.org/sqlite | 纯 Go 免 CGO，保住单二进制 | mattn/go-sqlite3（需 CGO） |
| CLI | cobra | 事实标准，help / 补全完善 | — |
| 前端 | React + Vite + TS + **Fluent UI React v9** | 官方 Fluent 组件库，原生 Windows 11 观感，embed.FS 打包 | 自建组件库（成本高、观感不统一） |

## 十五、风险与遗留问题

| 编号 | 风险 | 类别 | 概率 | 影响 | 等级 | 应对 |
|---|---|---|---|---|---|---|
| RK-01 | 大消息（接近 1MB）扫描时内存压力大 | 技术 | 3 | 3 | 9 中 | 64KB 截断（仅响应与快照内）；内存口径两档（§13）上线前用 1MB 消息实测校准 |
| RK-02 | goja 与 Node 行为差异导致脚本结果偏差 | 技术 | 3 | 2 | 6 低 | SKILL.md 注明支持的语法子集；dry-run 必走 |
| RK-03 | LogAppendTime 与 CreateTime 混用导致时间窗查询偏差 | 数据 | 3 | 3 | 9 中 | 结果返回每分区实际扫描区间，供用户核对；文档注明按 CreateTime |
| RK-04 | key hash 分区在 broker 端 partitioner 与 kgo 不一致 | 技术 | 2 | 3 | 6 低 | 统一由 kgo 默认分区器按 key 一致性 hash 判定，**工具不提供手动指定分区**（v1.2b）；FR-06 与 UI 明确"分区由 key 自动判定"语义，避免双端预期偏差 |
| RK-05 | 审计与命中快照含敏感信息（查询参数 / 命中消息内容落盘） | 合规 | 3 | 3 | 9 中（v1.2 升级） | operations 行不落消息体 + command 截断 2KB；快照有界（≤100 条 / 1MB）并计入保留滚动清理；设置页「命中快照留证」开关可整体关闭；`kp audit clear` 手动清空（含快照） |
| RK-06 | WebView2 在精简版 Windows 缺失 | 兼容 | 2 | 2 | 4 低 | 自动回落默认浏览器打开 |
| RK-07 | 扫描仅支持 JSON 过滤，Avro 消息只能 hex 查看 | 需求 | 3 | 2 | 6 低 | 已预留 Decoder 插件接口（D16）；后续按需实现 Schema Registry 插件（触发条件：团队引入 Avro） |
| RK-08 | serve 轮询 SQLite 感知 CLI 跨进程 op 的实时链路依赖 WAL 与轮询频率 | 技术 | 2 | 2 | 4 低 | 500ms 轻量查询（WAL 读不阻塞写）+ SSE 批量推送；实测校准端到端延迟 ≤1s（v1.2 新增） |

## 十六、版本规划

| 版本 | 范围 | 交付物 |
|---|---|---|
| v0.1 | CLI 骨架 + profile + topic 浏览 + scan（last / key / keyword）+ 单条注入 + 审计摘要 | kp.exe（CLI 可用） |
| v0.2 | jq 表达式 + 时间段查询 + session 短 ID（session new / --session）+ kp schema | AI 基本可用 |
| v0.3 | JS 脚本批量注入 + 背压管道 + 限速 + 进度 / 中断 + **dry-run（v1.2 提前：安全特性与批量注入同版，SKILL playbook 自 v0.3 起可完整执行）** | 造数能力完整 |
| v0.4 | Web UI 操作区（Fluent Design：查询构建器 + 注入面板 + **profile 完整管理含 SASL / TLS 表单**） | 人类操作界面 |
| v0.5 | AI 观测区（时间线 + SSE 实时 + **快照重放**）+ group lag 观测 | 完整闭环，对齐本设计文档全部 FR |

**MVP 定义**：v0.1 + v0.2 + v0.3 = 最小价值闭环（AI 能查、能造、有留痕）。观测区（v0.5）建议提前与 v0.4 并行，因为"人看得到 AI 干了什么"是本工具区别于普通 CLI 的核心价值。

## 十七、需求追踪矩阵（RTM）

| 需求 | PRD 章节 | 设计落点 | 图 | 验收 |
|---|---|---|---|---|
| 查看数据（key/关键词/表达式/条数/时间段） | §6 FR-03~05 | scan + filter 模块 | 图3 | §6 GWT |
| 测试注入（单条 / 脚本并发） | §6 FR-06~08 | produce 模块 | 图4 | §6 GWT |
| AI 操作留痕（最终结果 + 有界命中快照）+ 客户端只读回放（快照重放） | §6 FR-09~10、FR-12 | audit 模块 + 观测区 | 图5 | §6 GWT |
| 双端同一内核 | §2.2、§7 | 用例层唯一入口 | 图1 | 成功指标"双端一致性" |
| Skill over CLI | §9 | SKILL.md + 输出契约 | — | FR-14 |
| Kafka 连接可选认证（SASL / TLS 四组合） | §6 FR-13 | config + kafka 模块 | — | §6 GWT |
| Fluent Design 界面 | §10 | Fluent UI React v9 + §13 UI 设计 | — | §10 设计规格 |
| 数据目录可配置 + 一次性迁移 | §6 FR-16、§11.1 | `internal/config/settings.go` + CLI / REST / 设置页 | — | §6 FR-16 GWT + `settings_test.go` |

## 十八、假设确认结果（v1.1 全部闭环）

| # | 原假设 | v1.1 确认结果 |
|---|---|---|
| A1 | 注入默认并发 8、队列 1024、限速 0（不限） | ✅ 确认；并发 / 队列 / 速率 / 进度间隔全部支持 profile 默认值 + 命令行覆盖（FR-07/08） |
| A2 | 审计保留 30 天 / 500MB | ✅ 确认，设置页可改 |
| A3 | lag 高阈值 10000 | ✅ 确认，可配置 |
| A4 | session 依赖环境变量 KP_SESSION_ID | ✅ 已按反馈修订：`kp session new` 随机生成 6 位短 ID（数字+字母），Agent 先生成、后每条命令显式携带 `--session`，命令尽可能短（FR-10、D15） |
| A5 | 消息时间统一取 Kafka 消息时间戳（CreateTime） | ✅ 确认 |
| A6 | value 非 JSON 时表达式过滤退化 | ✅ 确认；jq 为默认解析插件，预留 `filter.Decoder` 扩展接口，后续支持用户自定义插件（FR-04、D16） |

**v1.1 附加确认的两条硬约束**：① 审计只含最终结果、不落任何消息体（FR-09、D7；**v1.2 修订**：scan 落有界命中快照、produce 不落，operations 行仍不含消息体）；② CLI 与 UI 无认证鉴权、Kafka 凭证仅内核持有（FR-13、D14）。

**v1.2 评审新增决策**：① `--last` 为回扫量语义（回扫 N 条原始消息、命中 ≤ N），`--limit` 取消【**v1.3c 修订**：N 是每分区回扫量，命中 ≤ 分区数 × N，各分区结果全部返回、不再全局截断】；② dry-run 提前至 v0.3；③ 重放 = 有界命中快照 + 纯本地回放，不发起任何 Kafka 读写；④ 客户端支持完整 profile 管理（含认证表单）；⑤ Kafka 连接认证显式化——SASL 用户名密码与 TLS 独立可选、四种组合（D18）。

**v1.2a 收窄（用户反馈 2 条）**：① TLS 收窄为**单向**——客户端用 CA 证书校验服务端身份或直接 `skip_verify` 跳过校验，移除 mTLS 双向认证（`client_cert` / `client_key` 字段及 `--tls-client-cert` / `--tls-client-key` 参数取消）；② `bootstrap_servers` 显式支持配置多个 broker 地址（YAML 数组 / CLI 逗号分隔 / kgo 自动发现全集群）。

---

*本方案为产品设计参考，具体实施请结合公司合规与法务要求确认。*

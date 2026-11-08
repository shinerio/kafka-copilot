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

---

## 文档索引（v1.5 拆分后）

> 本文件现为**全局总览**：保留文档信息、变更记录、关键决策（D1~D18）与图表维护约定。
> 详细设计已按主题拆到 `docs/` 下各分册，**正文逐行迁移、未删改**；完整索引见 `docs/README.md`，拆分前完整原文保存在 `archive/kafka-pilot-设计文档-v1.5-拆分前快照.md`。

| 分册 | 收录内容（原章节） | 何时读 |
|---|---|---|
| `00-项目总览与需求.md` | §二~§五：背景目标、术语、角色场景、需求范围 | 了解产品边界 |
| `01-总体架构与内核模块.md` | §七、§八、§十二、§十四：分层架构、内核模块、关键流程、技术选型 | 改架构 / 新增内核模块 |
| `02-功能需求-查询与注入.md` | §六 FR-01~FR-08：topic / group / 查询 / 过滤 / 扫描边界 / 注入 | 改 scan / produce |
| `03-功能需求-审计与配置.md` | §六 FR-09~FR-16：审计、会话、操作区 / 观测区、Profile、schema、数据目录 | 改审计 / 配置 |
| `04-接口契约-CLI与REST.md` | §九：命令清单、输出契约、SKILL 要点 | 改 CLI / REST 契约 |
| `05-界面设计.md` | §十：界面形态、信息架构、交互、实现基线 | 改 Web UI |
| `06-数据存储与保留策略.md` | §十一：存储布局、核心表、保留策略 | 改 SQLite / 文件布局 |
| `07-非功能与交付.md` | §十三、§十五~§十八：非功能、风险、版本规划、RTM、假设确认 | 评审 / 排期 |
| `kafka-pilot-本地缓存详细设计.md` | FR-15 本地缓存**现行设计基线**（独立详设 v1.0） | 实现缓存 |
| `archive/kafka-pilot-设计文档-v1.5-拆分前快照.md` | 拆分前 v1.5 完整原文，仅供溯源 | 核对历史 |

**优先级**：FR-15 以 `kafka-pilot-本地缓存详细设计.md` 为准（§6 中 FR-15 段落为 v1.4 历史草稿，保留于 `03-功能需求-审计与配置.md` 仅供溯源）；其余能力契约冲突时，按 `AGENTS.md` §7 以设计文档 FR 与 CLI 实际行为为准。

**章节编号**：分册沿用原 `§x` / `FR-xx` 编号，变更记录、RTM、`README.md`、`skill/SKILL.md` 中的交叉引用继续有效，按上表定位分册即可。

# 接口契约：CLI 与 Skill

> **分册说明**：本文件由 `docs/kafka-pilot-设计文档.md` v1.5 按主题拆分而来，正文与原章节逐行一致（未删改），章节编号沿用原编号，既有 `§x` / `FR-xx` 交叉引用继续有效。
> **收录章节**：原 §九 CLI 与 Skill 设计（9.1 命令清单 / 9.2 输出契约 / 9.3 SKILL.md 要点）
> **索引**：`docs/README.md` · **全局总览**：`docs/kafka-pilot-设计文档.md` · **拆分前完整快照**：`archive/kafka-pilot-设计文档-v1.5-拆分前快照.md`
> **维护纪律**：改动本册须按 `AGENTS.md` §7 同步主设计文档变更记录、`README.md`、`skill/SKILL.md`。

---

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


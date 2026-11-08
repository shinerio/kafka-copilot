# Kafka Pilot

Kafka 数据操作工具，提供 **命令行（CLI）** 与 **Web UI** 双端，用于查询、生产（注入）和消费审计 Kafka 数据。单二进制（Go）+ React 前端，配置与审计数据落地到本地 SQLite / YAML，**默认与 kp.exe 放在同一目录的 `kp-data/`**。

---

## 1. 构建

需要 Go（以 `go.mod` 为准，当前 1.26.0）与 Node 20+（仅构建前端时需要）。

```bash
# 在项目根目录
go build -o kp.exe ./cmd/kp        # Windows
# 或
go build -o kp ./cmd/kp            # Linux / macOS
```

构建产物即 `kp`（或 `kp.exe`），所有功能都在这个二进制里。前端已嵌入二进制；**若改了 `web/`，必须先 `cd web && npm run build` 再 `go build`**，否则嵌入的是旧产物。

---

## 2. 数据目录（Profile / 审计 / 缓存）

所有连接 profile（`config.yaml`）、审计库（`audit.db`）与本地缓存都放在**数据目录**里。默认布局：

```
<kp.exe 所在目录>/
├── kp.exe
├── kp-settings.yaml        # 数据目录配置（固定放这里，不在数据目录内）
└── kp-data/
    ├── config.yaml         # profile（密码 Windows DPAPI 加密）
    ├── audit.db            # SQLite（WAL；运行时另有 -wal / -shm）
    └── ...
```

**解析优先级**（高 → 低）：

1. 环境变量 `KP_APPDATA`：直接指定数据目录（测试用；设置时不触发旧目录迁移）。
2. `kp-settings.yaml` 里的 `data_dir`：相对路径基于 **kp.exe 所在目录**，绝对路径原样使用。
3. 默认 `<exe 目录>/kp-data`。

`kp-settings.yaml` 固定放在 kp.exe 旁（高级场景可用 `KP_SETTINGS` 指定其它位置）——它不能放进数据目录内部，否则无法找到指向自己的配置。示例：

```yaml
data_dir: kp-data
migrated_from: "C:\\Users\\me\\AppData\\Roaming\\kafka-pilot"
migrated_at: "2026-11-08T22:50:00+08:00"
migration_done: true
```

### 查看 / 修改 / 迁移

```bash
kp settings show                          # 输出当前数据目录布局（JSON）
kp settings set-data-dir D:/kp/storage    # 改目录，默认把现有数据一起复制过去
kp settings set-data-dir kp-data --copy-current=false   # 只改配置，新目录从空开始
kp settings migrate                       # 从旧位置一次性迁移；--force 可复查
```

REST 与 CLI 完全对称：`GET /api/settings`、`PUT /api/settings`（`{"data_dir":"...","copy_current":true}`）、`POST /api/settings/migrate`（`{"force":false}`）。Web UI「设置」页顶部的「数据目录」卡片提供同样能力（显示生效目录 / 配置目录 / 默认目录 / 旧版目录 / 各文件路径与迁移状态，可保存、恢复默认、一键迁移）。

> **改目录后必须 `kp restart`**：正在运行的服务仍使用启动时解析出的目录。`GET /api/settings` 的 `active_data_dir` 是生效值，`pending_change=true` 表示配置已改但尚未重启。

> **一次性迁移**：首次启动或执行 `kp settings migrate` 时，把旧位置（Windows `%APPDATA%/kafka-pilot`，Linux / macOS `~/.kafka-pilot`）的 `config.yaml` 与 `audit.db` 三件套复制进当前数据目录。规则：
> - 目标已存在的文件**绝不覆盖**；
> - `audit.db` + `audit.db-wal` + `audit.db-shm` 作为**一组整体复制**，避免 WAL 与 db 不匹配造成损坏；
> - **源文件保留、不删除**，随时可回滚；
> - `kp-settings.yaml` 里的 `migration_done` 标记保证只做一次，`--force` 也只复查、不覆盖。

> **建议**：迁移 / 切换目录前先 `kp stop`，避免 SQLite WAL 在服务写入时被复制而少掉最后几条记录。切换目录默认会复制现有 `config.yaml` / `audit.db`，不会让 profile「凭空消失」。

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

> **实现说明**：`kp.pid`（记录 `pid` / `port` / `started_at`）放在 **kp.exe 同目录**，与数据目录解耦——改了 `data_dir` 后 `kp stop` / `kp restart` 依然能找到正在运行的服务。读取时还会兼容查找旧版本写在数据目录或 `%APPDATA%/kafka-pilot` 下的 pid 文件。`stop` / `restart` 不依赖 Kafka 连接，因此即使配置损坏也能执行。

> **动态更新**：通过 Web UI「设置」页或 `PUT /api/profiles` 保存 profile 会**即时生效**（内存更新 + 落盘 + 失效该 profile 的 Kafka 连接缓存），**无需重启**。需要 `kp restart` 的情况：① 直接手改 `config.yaml`；② **修改了数据目录**；③ 完成一次性迁移后让服务切换到新目录。

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

### 5.8 settings — 数据目录
```bash
kp settings show
kp settings set-data-dir kp-data
kp settings set-data-dir D:/kafka-pilot-data --copy-current=false
kp settings migrate
kp settings migrate --force
```

`settings` 命令只做本地文件管理，不连 Kafka、不需要服务在运行，也不写审计记录。输出为 JSON：`show` 返回 `data_dir` / `default_data_dir` / `legacy_dir` / `settings_path` / `config_path` / `audit_path` / `*_exists` / `migration_*` 等字段；`set-data-dir` 与 `migrate` 返回 `copied` / `skipped` / `restart_required` 等字段。

---

## 6. Web UI 使用

启动服务后访问 `http://127.0.0.1:<端口>`，主要功能页：

- **设置**：顶部「数据目录」卡片（查看 / 修改目录、复制现有数据、从旧位置迁移、恢复默认）；下方是 profile 管理（含 SASL / TLS）与审计保留。
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

**Q：换了数据目录后 profile / 审计记录都不见了？**
A：先看 `kp settings show` 的 `active_data_dir`（服务实际用的目录）与 `data_dir`（配置值）。改目录后必须 `kp restart`；若新目录是空的，用 `kp settings set-data-dir <旧目录>` 切回，或用 `kp settings migrate --force` 从旧位置复查。切换时默认会复制现有数据，也可用 `--copy-current=false` 明确从空开始。

**Q：旧 `%APPDATA%/kafka-pilot` 目录还在，能删吗？**
A：迁移是**复制而非删除**，建议确认 `kp-data` 下 `config.yaml` / `audit.db` 与新服务都正常后再自行处理。想回滚时，把 `kp-settings.yaml` 的 `data_dir` 改回旧目录并重启即可。

---

## 8. 退出码

| 码 | 含义 |
|----|------|
| 0  | 成功 |
| 2  | 参数错误 |
| 3  | 连接错误（Kafka / SASL / TLS） |
| 4  | 超时 |
| 5  | 内部错误 |

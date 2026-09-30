# 边缘设备遥测接入与云端同步网关（本地模拟）

一套不依赖任何外部服务或真实凭据的边缘遥测接入能力：用本地 SQLite 文件分别模拟
**云端存储**与**边缘侧缓冲**，覆盖设备注册/凭据管理、批量遥测的幂等与乱序合并、
离线缓冲与重连回放，保证弱网、离线重连、重复上报下数据**最终一致且可追溯**。

## 1. 架构与同步协议

```
设备 ──(secret 鉴权)──> EdgeAgent（边缘代理）
                           │
                 在线 ─────┴─────> CloudIngestService ──> LocalStore (cloud.db, 云端)
                           │
                 离线 ──> EdgeSpool (edge_spool.db, 边缘持久缓冲, 有容量上限)
                           │
                 重连 ──> replay() 按确定顺序回放 ──> CloudIngestService
```

- 设备每次上报携带 `device_id` + 预先注册获得的 `secret`（仅本地生成，非真实凭据）。
- 边缘代理无论在线/离线都先做**本地凭据校验**；在线时云端再校验一次。
- 云端对每个批次做幂等/序号判定，返回四类状态：
  `accepted`（按序接受）、`duplicate`（重复幂等）、`deferred`（乱序挂起，稍后自动提升）、
  以及被拒绝时的明确原因码（见下）。
- 所有数据落在本地 SQLite（WAL 模式），进程重启后凭据、云端批次、边缘缓冲都不丢。

### HTTP 端点（本地验证用）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/devices/register` | 注册设备，返回 secret、作用范围、有效期 |
| POST | `/telemetry` | 设备直连云端上报（头 `x-device-id` / `x-device-secret`） |
| POST | `/edge/submit` | 经边缘代理上报（在线直传 / 离线缓冲） |
| POST | `/edge/link` | 切换边缘链路 `{"online": false/true}`，模拟断网/重连 |
| POST | `/edge/replay` | 触发重连回放，返回每条批次的云端判定 |
| GET  | `/devices/{id}/state` | 云端视角：序号水位 + 全部批次及状态 |
| GET  | `/edge/metrics` | 边缘可观测指标与事件日志（含溢出次数） |

## 2. 凭据、有效期与作用范围

- 注册时指定 `scopes` 与 `ttl_seconds`；`secret` 用 `secrets.token_hex` 本地生成。
- 内置作用范围：
  - `telemetry:write`：允许上报遥测；
  - `register:device`：仅允许设备注册类操作（用于构造越权场景）。
- 鉴权按**固定顺序**判定，三类失败彼此可区分（`RejectReason`，HTTP 403）：

  | 顺序 | 条件 | 原因码 |
  | --- | --- | --- |
  | 1 | 设备不存在或 secret 不匹配 | `credential_not_found` |
  | 2 | 当前时间 ≥ `expires_at` | `credential_expired` |
  | 3 | 凭据作用范围不含所需 scope | `scope_denied` |

  密钥不匹配与设备不存在统一归为 `credential_not_found`，避免泄漏设备存在性；
  但它与 `credential_expired`、`scope_denied` 仍然严格可区分。

## 3. 幂等与序号规则

- 序号 `seq` 为**设备维度**、从 1 开始的连续递增整数；云端为每个设备维护
  `next_expected` 水位。
- 幂等键：`(device_id, batch_id)`，数据库唯一约束兜底。
- 内容指纹：对 `payload` 做 key 排序的规范化 JSON 后取 SHA-256。

判定规则（同一设备由内存锁串行化，配合 DB 唯一约束，并发安全）：

1. **重复批次**：同 `(device_id, batch_id)` 且指纹一致 → 返回 `duplicate`，
   不重复落库（HTTP 仍为成功语义）。
2. **序号冲突，拒绝而非覆盖**：
   - 同 `batch_id` 但指纹不同；
   - 同 `seq` 槽位已被其他 `batch_id`/不同内容占用；
   两种都返回 `sequence_conflict`，已存数据原样保留。
3. **乱序**：`seq > next_expected` 时持久化为 `deferred` 挂起；
   当缺口序号到达，沿连续序号自动把挂起批次提升为 `accepted` 并推进水位
   （例如先到 3、再到 1、最后到 2，最终 1/2/3 全部 accepted，水位=4）。
4. `seq < next_expected` 时该槽位必然已有数据，走"重复"或"冲突"分支。

## 4. 离线缓冲、容量上限与淘汰策略

- 离线期间，鉴权通过的批次写入 `edge_spool.db`（SQLite），**重启不丢**；
  同 `(device_id, batch_id)` 离线重复上报同样幂等忽略。
- 缓冲容量为条数上限（默认 1000，`create_app(capacity=...)` 可调，测试用小容量）。
- **溢出策略：`reject-new`（显式拒绝，不静默丢弃）**
  - 缓冲达到上限后，新批次直接被拒绝，原因码 `buffer_full`（HTTP 429）；
  - 已缓冲的旧数据原样保留，等待回放；
  - 拒绝计入 `GET /edge/metrics` 的 `buffer_full` 计数，并记录 `buffer-full` 事件，
    调用方可观测并决定何时重试。
- **回放顺序确定**：`(device_id ASC, seq ASC, 入队 id ASC)`，避免不同设备/序号交叉
  导致的不确定结果。
- 回放时云端返回 `duplicate`（例如该批次已由其他链路送达）也视为成功并移出缓冲；
  凭据过期/越权等鉴权问题则**保留**在缓冲并计入 `replay_failures`，待凭据更新后重放。
- 回放完成后边缘缓冲清空，云端按序号水位呈现完整连续序列，两侧设备状态一致。

## 5. 本地验证方法

需要 Python 3.14+ 与 [uv](https://docs.astral.sh/uv/)。

```bash
uv sync                       # 安装依赖（含 pytest 开发依赖）
uv run pytest                 # 运行全部单测（默认打印输入与判定依据日志）
uv run pytest -q              # 简洁输出
```

测试场景覆盖：

- `tests/test_credentials.py`：凭据不存在 / 过期 / 越权三类原因可区分；
- `tests/test_ingest.py`：重复批次幂等、同序号或同 batch_id 内容冲突拒绝、
  乱序挂起与自动提升、16 线程并发重复批次与并发乱序补全；
- `tests/test_edge_sync.py`：离线缓冲、进程重启后缓冲恢复、回放后云端/边缘一致、
  云端已存在批次的重复回放、缓冲溢出（`buffer_full` + 指标）、离线鉴权失败；
- `tests/test_api.py`：HTTP 端到端生命周期与原因码。

手动体验（启动服务）：

```bash
uv run python main.py         # http://127.0.0.1:8000 ，文档 /docs
```

```bash
# 1) 注册
curl -s -X POST localhost:8000/devices/register \
  -H 'content-type: application/json' \
  -d '{"device_id":"dev-1","scopes":["telemetry:write"],"ttl_seconds":3600}'
# 2) 断网 -> 离线缓冲两条
curl -s -X POST localhost:8000/edge/link -H 'content-type: application/json' -d '{"online":false}'
curl -s -X POST localhost:8000/edge/submit -H 'content-type: application/json' \
  -H 'x-device-id: dev-1' -H 'x-device-secret: <secret>' \
  -d '{"batch_id":"b1","seq":1,"payload":[{"temp":21.5}]}'
# 3) 重连回放
curl -s -X POST localhost:8000/edge/link -H 'content-type: application/json' -d '{"online":true}'
curl -s -X POST localhost:8000/edge/replay
curl -s localhost:8000/devices/dev-1/state
```

## 6. 代码结构

```
edge_telemetry/
  errors.py        # RejectReason 原因码与 TelemetryError
  models.py        # DeviceCredential / TelemetryBatch / IngestResult
  storage.py       # LocalStore（云端）与 EdgeSpool（边缘缓冲），SQLite 持久化
  credentials.py   # 注册 + 三类可区分鉴权失败
  ingest.py        # 幂等/乱序延迟/序号冲突，内容指纹，设备级串行锁
  edge_agent.py    # 在线直传/离线缓冲/容量保护/确定性回放/指标
  api.py           # FastAPI 接线
tests/             # 场景化单测，日志打印输入与判定依据
```

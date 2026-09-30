# 边缘设备遥测接入与云端同步网关（本地模拟 · Python）

用**本地存储**模拟边缘侧与云端的一套遥测接入 / 同步系统，覆盖弱网、离线重连、
重复上报、乱序到达等场景，保证数据**最终一致且可追溯**。无任何外部服务或真实凭据。

## 能力一览

- **设备注册与凭据管理**：凭据有有效期（`expires_at`）和作用范围（scope）；
  凭据不存在、已过期、越权、设备不存在四类失败原因使用**独立错误码**，可明确区分。
- **批量上报幂等与乱序处理**：以 `batch_id + 内容哈希` 去重，重复批次不产生
  重复数据；乱序批次进入有界重排缓冲，缺口补齐后按序并入；**同序号内容冲突一律
  拒绝**（`SEQUENCE_CONFLICT`），不静默覆盖；同一设备并发上报按每设备锁串行化。
- **离线缓冲与重连回放**：边缘 outbox 每条原子落盘，进程重启不丢、序号不复用；
  缓冲上限支持 `REJECT_NEW`（默认，返回 `BUFFER_FULL`）与 `DROP_OLDEST`
  两种可观测策略；回放完成后云边设备状态一致（可校验）。
- **可追溯**：云端保存每个批次的账本（内容指纹 + 判定结果 + 原因），边缘统计
  溢出 / 终态失败事件；单测日志打印输入与"判定依据"。

## 目录结构

```
edge_telemetry/
  errors.py     # 稳定错误码（失败原因可区分）
  models.py     # 设备/凭据/批次/缓冲条目模型 + 规范JSON与内容哈希
  storage.py    # KV 抽象、内存实现、JSON 文件原子落盘实现（重启恢复）
  registry.py   # 设备注册、凭据签发/撤销、鉴权
  gateway.py    # 云端：幂等、乱序合并、冲突拒绝、账本
  transport.py  # 本地传输（可断网 / 注入瞬时失败）
  edge.py       # 边缘：序号、outbox、溢出策略、回放、一致性校验
main.py         # FastAPI 演示服务 + 脚本式端到端演示
tests/          # 单测（存储/凭据/网关幂等乱序并发/边缘同步）
docs/protocol.md# 同步协议、序号与幂等规则、凭据范围、缓冲淘汰策略
```

## 本地验证

需要 Python 3.14+（项目使用 uv）。

```bash
# 1) 安装依赖（pytest 在 dev 依赖中）
uv sync

# 2) 跑全部单测（日志会逐条打印输入与"判定依据"）
uv run python -m pytest tests -v

# 3) 脚本式端到端演示：注册→离线缓冲→弱网失败→重连回放→一致性校验
EDGE_TELEMETRY_DEMO=1 uv run python main.py

# 4) 启动 HTTP 演示服务（交互式接口见 /docs）
uv run python main.py            # 默认 127.0.0.1:8080
# 可选：把状态持久化到 JSON 文件，重启后仍可恢复
EDGE_TELEMETRY_STORE=./data/demo.json uv run python main.py
```

### HTTP 接口速测

```bash
BASE=http://127.0.0.1:8080
# 注册设备并拿到凭据
curl -s -X POST $BASE/devices/register -H 'Content-Type: application/json' \
  -d '{"device_id":"dev-1"}'
# 上报（离线也会先进边缘缓冲）
curl -s -X POST $BASE/telemetry -H 'Content-Type: application/json' \
  -H "x-credential-id: $CID" -H "x-credential-secret: $SEC" \
  -d '{"device_id":"dev-1","points":[{"temp":21.5}]}'
# 回放（online=false 可模拟链路离线）
curl -s -X POST $BASE/edge/flush -H 'Content-Type: application/json' \
  -d '{"device_id":"dev-1","online":true}'
# 查看云端状态 / 边缘统计
curl -s $BASE/cloud/state/dev-1
curl -s $BASE/edge/stats/dev-1
```

鉴权失败响应体含稳定错误码，例如：

```json
{"detail": {"code": "CREDENTIAL_EXPIRED", "message": "凭据已过期: ...", "details": {...}}}
```

## 测试覆盖的场景

| 场景 | 测试 |
| --- | --- |
| 重复批次幂等、无重复数据 | `test_gateway.py::test_duplicate_batch_is_idempotent` |
| 乱序到达 → 延迟 → 缺口补齐级联合并 | `test_gateway.py::test_out_of_order_then_gap_fill` |
| 同序号内容冲突（已并入 / 缓冲中）拒绝 | `test_sequence_*`、`test_pending_conflict_rejected` |
| 凭据过期 / 不存在 / 越权 / 跨设备 | `test_registry.py`、`test_cross_device_credential_is_scope_denied` |
| 同设备并发（同批次 + 乱序多批次） | `test_gateway.py::test_concurrent_same_batch_and_distinct_seqs` |
| 离线缓冲 + 弱网重放 + 重启不丢 + 最终一致 | `test_edge_sync.py` |
| 缓冲溢出：REJECT_NEW / DROP_OLDEST 可观测 | `test_buffer_full_*`、`test_buffer_overflow_*` |
| 凭据过期导致回放终态失败（DEAD + 事件） | `test_expired_credential_during_flush_marks_dead` |

协议细节（判定顺序、合并语义、缓冲淘汰、错误码表）见
[`docs/protocol.md`](docs/protocol.md)。

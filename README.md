# 机房设备故障日志只读查询 API

值班分析员共用的**只读 SQL 入口**：固定两条 PostgreSQL 连接，提交单条
`SELECT` 与**总截止时间**，排队等待也计入预算；超时立即取消数据库语句，
确认事务结束才复用连接，无法确认则弃置连接并补建；迟到结果不会改写超时
回执。后续查询始终拿到属于自己的结果。

内置**锁等待诊断**：值班发现查询被表锁/行锁卡住时，诊断接口把本服务正在
执行的请求关联到实际 PostgreSQL 后端，给出请求标识、等待类型与阻塞者
PID，且走独立监测连接，两条查询连接都忙时仍可用。

## 技术栈

- FastAPI（Swagger UI：`/docs`，ReDoc：`/redoc`）
- psycopg 3（异步连接 + libpq `PQcancel`）
- PostgreSQL 16
- Docker Compose 编排，预置 7 条故障日志、只读角色 `fault_ro` 与监测角色
  `fault_mon`

## 快速开始（Docker Compose）

```bash
docker compose up --build
# API:    http://localhost:8000
# Swagger: http://localhost:8000/docs
```

自测（另开终端，容器内或本机均可）：

```bash
docker compose exec api python -m tests.selftest
```

## 接口

### `POST /query`

```json
{
  "sql": "SELECT id, device_id, severity FROM fault_logs LIMIT 5",
  "timeout_ms": 2000
}
```

`timeout_ms` 是**包含排队时间**的总预算（单调时钟），从请求进入即计时。
成功与超时回执都携带 `request_id`，可在诊断接口中关联定位。

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 成功 | 200 | `status=ok`，`rows` 为本次查询自身结果，附 `request_id`、`wait_ms`/`execution_ms` |
| 执行超时 | 504 | `error.code=query_timeout`，`status=timeout`，`request_id`、`cancelled`、`connection_discarded`、`late_result_ignored` |
| 排队期间预算耗尽 | 503 | `error.code=acquire_timeout`，语句**从未发送**到数据库 |
| SQL 不合法 | 400 | `error.code=invalid_sql` |

### `GET /diagnostics/lock-waits`

锁等待诊断。把本服务的在途请求与 `pg_stat_activity` / `pg_blocking_pids`
关联，**只报告确认处于锁等待**的请求：

```json
{
  "status": "ok",
  "generated_at": "2026-11-17T12:00:00.000000+00:00",
  "in_flight": {"executing": 2, "queued": 1},
  "backends_checked": 2,
  "skipped_unstable": 0,
  "lock_waits": [
    {
      "request_id": "9f1c…",
      "backend_pid": 1234,
      "wait_event_type": "Lock",
      "wait_event": "advisory",
      "blocking_pids": [1201],
      "wait_duration_ms": 1823.4,
      "sql": "SELECT pg_advisory_xact_lock(…)"
    }
  ]
}
```

- **不误报**：排队中的请求没有后端、正常执行的 SQL（如 `pg_sleep`，
  等待类型 `Timeout`）都不会出现在 `lock_waits` 中；只有
  `wait_event_type='Lock'` 或 `pg_blocking_pids` 非空才上报。
- **独立监测连接**：诊断走专用只读连接（`fault_mon` 角色，
  `pg_monitor` 权限），不占用两条业务查询连接，连接占满时仍可用。
- **防串号**：请求登记带单调 token，释放/弃置连接前先注销；诊断核对
  期间状态发生变化的请求会被剔除（`skipped_unstable`），超时、重试或
  连接被重新借出后，旧请求不会归到新后端。
- **诚实失败**：监测连接不可用时返回 `503`
  `error.code=diagnostics_unavailable`，绝不返回虚假的空结果；监测
  连接会自动重连恢复。

### `GET /health`

返回连接池容量、空闲/占用数、累计创建与弃置数。

## 关键设计

### 1. 容量恒为 2，排队计入总预算

`QueryConnectionPool` 用容量为 2 的信号量限制**物理后端数**，第三个请求
排队等待而非新开连接。`acquire(deadline)` 接收绝对单调时钟截止时间，
等待与执行共用同一预算；排队期间过期则直接返回 503，不发送任何语句。

### 2. 单条只读 SELECT 的三层防护

1. `app/validation.py` 仅允许单条 `SELECT`/`WITH`/`TABLE`/`VALUES`，拒绝
   多语句（额外分号）、写关键字、`SELECT ... INTO` 等。
2. 每个请求在显式 `BEGIN READ ONLY` 事务中执行，会话默认
   `default_transaction_read_only=on`，并设置局部 `statement_timeout` 兜底。
3. 数据库角色 `fault_ro` 仅有 `SELECT` 权限，且角色默认只读；直连也无法
   写入（`InsufficientPrivilege`）。

### 3. 超时处理协议（`app/executor.py`）

1. 语句作为 **shielded task** 运行，超时回执一旦确定就不会被迟到结果覆盖。
2. 截止时间到，调用 libpq `PQcancel`（经独立取消连接）请求服务器中止语句。
3. `await` 该 shielded 任务直至真正结束（收到 `QueryCanceled` 或拿到迟到
   结果），杜绝协程残留。
4. 执行 `ROLLBACK` 并核对 `transaction_status == IDLE`，**确认事务结束**才
   把连接放回池；无法确认则关闭弃置并补建，保持容量为 2。
5. 迟到完成的结果只做审计日志，回执固定为 504 且不携带 `rows`。

### 4. 连接级故障的弃置补建与无感重试

后端被带外终止（如 `pg_terminate_backend`）时，连接级错误触发弃置并由池
补建；只读语句在同一总预算内安全重试一次，客户端无感知。若补建暂时失败，
池临时缩水并在下一次获取时懒建补回，不会死锁。

### 5. 在途请求注册表与锁等待诊断（`app/registry.py` 等）

1. 请求一进入执行器就以 `queued` 登记（`app/registry.py`），借到连接后
   绑定该连接的 `backend_pid` 并发放单调 **token**；释放或弃置连接**之前**
   注销，token 不匹配的迟到清理一律忽略。
2. 诊断（`app/diagnostics.py`）对注册表做**前后两次快照**，只上报两次
   快照间 `(request_id, token, backend_pid)` 完全一致的条目，宁可少报也
   不把旧请求挂到重新借出的后端上。
3. 监测连接（`app/monitor.py`）独立于查询池，查询失败即抛
   `DiagnosticsUnavailable`（503），连接弃置后按需重建。

## 无 Docker 的本地运行与自测

本机若无容器运行时，可用 `pgserver`（PyPI 上的免 root PostgreSQL 16）：

```bash
pip install -r requirements.txt pgserver
python tests/local_run.py        # 拉起 PG16 + uvicorn
python -m tests.selftest         # 端到端自测（含锁等待诊断）
python tests/test_protocol.py    # 超时/迟到结果/排队/锁诊断协议直测
```

## 目录结构

```
app/            FastAPI 应用（config/pool/executor/registry/monitor/
                diagnostics/validation/schemas/main）
db/01_init.sql  建表、预置日志、只读角色 fault_ro、监测角色 fault_mon
tests/          selftest.py（HTTP 端到端）、test_protocol.py（协议直测）、
                local_run.py（免 Docker 启动器）
Dockerfile / docker-compose.yml / requirements.txt
```

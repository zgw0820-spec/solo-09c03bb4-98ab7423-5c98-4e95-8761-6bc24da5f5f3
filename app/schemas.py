"""Request/response schemas surfaced in the OpenAPI (Swagger) document."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    sql: str = Field(
        ...,
        description=(
            "单条只读 SELECT 语句（允许 WITH/VALUES/TABLE，"
            "禁止多语句、写操作、SELECT INTO 等）"
        ),
        examples=["SELECT id, device_id, severity FROM fault_logs LIMIT 5"],
    )
    timeout_ms: int = Field(
        ...,
        ge=1,
        le=60_000,
        description=(
            "总截止时间（毫秒），从请求进入起计时，包含排队等连接的时间"
        ),
    )


class QueryResponse(BaseModel):
    status: Literal["ok", "timeout"]
    executed: bool = Field(
        description="语句是否真正发送到了数据库后端"
    )
    request_id: str | None = Field(
        default=None,
        description="本次请求标识，可在 /diagnostics/lock-waits 中关联定位",
    )
    wait_ms: float | None = None
    execution_ms: float | None = None
    row_count: int | None = None
    rows: list[list[Any]] | None = None
    cancelled: bool | None = None
    connection_discarded: bool | None = None
    late_result_ignored: bool | None = None
    detail: str | None = None


class ErrorResponse(BaseModel):
    error: dict[str, str]


class HealthResponse(BaseModel):
    status: Literal["healthy"]
    pool: dict[str, Any]


class LockWaitEntry(BaseModel):
    request_id: str = Field(description="阻塞中的请求标识（与 /query 回执一致）")
    backend_pid: int = Field(description="执行该请求的 PostgreSQL 后端 PID")
    wait_event_type: str | None = Field(
        description="pg_stat_activity.wait_event_type，锁等待为 Lock"
    )
    wait_event: str | None = Field(
        description="pg_stat_activity.wait_event，如 relation/advisory"
    )
    blocking_pids: list[int] = Field(
        description="pg_blocking_pids：阻塞该后端的会话 PID 列表"
    )
    wait_duration_ms: float = Field(
        description="自语句开始执行的毫秒数（锁等待时长的下界）"
    )
    sql: str = Field(description="该请求提交的 SQL（截断至 200 字符）")


class InFlightSummary(BaseModel):
    executing: int = Field(description="正在后端执行的请求数")
    queued: int = Field(description="排队等连接的请求数（绝无后端，非锁等待）")


class LockWaitDiagnosticsResponse(BaseModel):
    status: Literal["ok"]
    generated_at: str
    in_flight: InFlightSummary
    backends_checked: int = Field(
        description="本次在 pg_stat_activity 中核对的执行中后端数"
    )
    skipped_unstable: int = Field(
        description="核对期间状态发生变化、为防误报而被剔除的请求数"
    )
    lock_waits: list[LockWaitEntry] = Field(
        description="确认处于锁等待的本服务请求（排队与正常执行不计入）"
    )

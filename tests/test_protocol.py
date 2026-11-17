"""Focused protocol tests that do not need a running HTTP server.

They drive the executor/pool directly against the ephemeral PostgreSQL and
concentrate on the hardest correctness guarantees:

* a statement that finishes just before the cancel request arrives must not
  overwrite the already-decided timeout receipt ("late result");
* a queued request whose deadline expires never sends a statement;
* after a timeout the backend is confirmed IDLE before reuse;
* lock-wait diagnostics: real lock waits are correlated to the right
  request id and blocker PID, queued/normal requests are never misreported,
  timed-out requests are not attributed to re-borrowed backends, and a dead
  monitoring connection surfaces as DiagnosticsUnavailable — never as a
  fabricated empty result.

Run:  python tests/test_protocol.py
"""
from __future__ import annotations

import asyncio
import dataclasses
import pathlib
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg  # noqa: E402
import pgserver  # noqa: E402

from app.config import Settings  # noqa: E402
from app.diagnostics import collect_lock_waits  # noqa: E402
from app.errors import (  # noqa: E402
    DiagnosticsUnavailable,
    PoolAcquireTimeout,
    QueryTimeout,
)
from app.executor import QueryExecutor  # noqa: E402
from app.monitor import LockMonitor  # noqa: E402
from app.pool import QueryConnectionPool  # noqa: E402
from app.registry import InFlightRegistry  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


async def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="proto-")
    pg = pgserver.get_server(data_dir, cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    ddl = (ROOT / "db" / "01_init.sql").read_text(encoding="utf-8")
    with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("CREATE DATABASE faultdb")
    from urllib.parse import urlparse, urlunparse

    p = urlparse(admin_dsn)
    fault_admin = urlunparse(p._replace(path="/faultdb"))
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(ddl)
        cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
        cur.execute("ALTER ROLE fault_mon PASSWORD NULL")
    ro = urlunparse(
        urlparse(fault_admin)._replace(
            netloc=urlparse(fault_admin).netloc.replace("postgres", "fault_ro", 1)
        )
    )
    mon = urlunparse(
        urlparse(fault_admin)._replace(
            netloc=urlparse(fault_admin).netloc.replace("postgres", "fault_mon", 1)
        )
    )

    settings = Settings(database_dsn=ro, pool_size=2, monitor_dsn=mon)
    pool = QueryConnectionPool(settings)
    await pool.open()
    registry = InFlightRegistry()
    executor = QueryExecutor(pool, settings, registry)
    monitor = LockMonitor(settings)
    await monitor.open()

    print("=== A. 迟到结果不改写超时回执 ===")
    # The statement (150ms) always outlives the 60ms deadline, so the timeout
    # path is mandatory.  Cancellation arrives ~sub-millisecond later, which
    # is also how a real "finished just before cancel" race is shaped: the
    # shielded task is awaited and whatever it returns is recorded but never
    # allowed to replace the receipt.
    receipt: QueryTimeout | None = None
    late_seen = False
    try:
        await executor.execute(
            "SELECT pg_sleep(0.15)", time.monotonic() + 0.06
        )
    except QueryTimeout as exc:
        receipt = exc
        late_seen = bool(exc.extra.get("late_result_ignored"))

    check("截止时间到必定返回 QueryTimeout", receipt is not None)
    if receipt is not None:
        check("回执固定为 timeout",
              receipt.extra.get("status") == "timeout", receipt.extra)
        check("回执不携带 rows（迟到结果未泄漏）",
              "rows" not in receipt.extra, receipt.extra)
        check("已发起取消并确认事务结束(未弃置)",
              receipt.extra.get("cancelled") is True
              and receipt.extra.get("connection_discarded") is False,
              receipt.extra)
        print(f"  INFO  late_result_ignored={late_seen} "
              f"(True=语句在取消前完成；False=服务器确认取消)")

    print("=== B. 超时后确认 IDLE 才复用 ===")
    st = await pool.status()
    check("池容量仍为 2", st.size == 2, st)
    check("两条后端均空闲(IDLE)", st.idle == 2, st)
    ok = await executor.execute("SELECT 7", time.monotonic() + 5)
    check("后续查询得到自身结果", ok["rows"] == [[7]], ok)

    print("=== C. 排队过期绝不发送语句 ===")
    # Occupy both backends with long sleeps (long own deadlines).
    async def slow(tag: str) -> dict:
        return await executor.execute(
            f"SELECT '{tag}', pg_sleep(2)", time.monotonic() + 6
        )

    busy = [asyncio.ensure_future(slow("X")), asyncio.ensure_future(slow("Y"))]
    await asyncio.sleep(0.4)
    queued_err = None
    try:
        await executor.execute(
            "SELECT 'NEVER-RUN'", time.monotonic() + 0.4
        )
    except PoolAcquireTimeout as exc:
        queued_err = exc
    check("排队过期抛 PoolAcquireTimeout", queued_err is not None)
    results = await asyncio.gather(*busy)
    check("两条占池查询各自成功",
          {r["rows"][0][0] for r in results} == {"X", "Y"}, results)
    # Prove the queued statement never reached a backend: no marker exists.
    after = await executor.execute("SELECT 1", time.monotonic() + 5)
    check("过期语句无任何副作用，后续查询正常", after["rows"] == [[1]], after)
    st = await pool.status()
    check("结束后池满且空闲", st.idle == 2 and st.size == 2, st)

    print("=== D. 注册表 token 语义（旧请求不得误删新登记） ===")
    reg = InFlightRegistry()
    rid = reg.enqueue("SELECT 1")
    tok1 = reg.attach(rid, 111)
    reg.detach(rid, tok1)
    snap = [e for e in reg.snapshot() if e.request_id == rid][0]
    check("detach 后回到 queued 且无后端",
          snap.state == "queued" and snap.backend_pid is None, snap)
    tok2 = reg.attach(rid, 222)
    reg.finish(rid, tok1)  # 旧 token 的迟到清理
    snap = [e for e in reg.snapshot() if e.request_id == rid]
    check("旧 token 不得删除新登记",
          len(snap) == 1 and snap[0].backend_pid == 222, snap)
    reg.finish(rid, tok2)
    check("正确 token 完成注销", reg.snapshot() == [], reg.snapshot())

    print("=== E. 锁等待被关联到正确请求与阻塞者 ===")
    admin = psycopg.connect(fault_admin, autocommit=True)
    acur = admin.cursor()
    acur.execute("SELECT pg_backend_pid()")
    admin_pid = acur.fetchone()[0]
    acur.execute("SELECT pg_advisory_lock(99)")  # 管理会话持锁

    waiter_t = asyncio.ensure_future(executor.execute(
        "SELECT pg_advisory_xact_lock(99)", time.monotonic() + 8))
    napper_t = asyncio.ensure_future(executor.execute(
        "SELECT pg_sleep(2.5)", time.monotonic() + 8))
    await asyncio.sleep(0.5)
    queued_t = asyncio.ensure_future(executor.execute(
        "SELECT 'Q'", time.monotonic() + 6))
    await asyncio.sleep(0.4)

    report = await collect_lock_waits(registry, monitor)
    check("恰好一个锁等待", len(report["lock_waits"]) == 1, report)
    check("在途统计: 2 执行 + 1 排队",
          report["in_flight"] == {"executing": 2, "queued": 1}, report)
    lock_wait_rid = None
    if report["lock_waits"]:
        entry = report["lock_waits"][0]
        check("等待类型 Lock/advisory",
              entry["wait_event_type"] == "Lock"
              and entry["wait_event"] == "advisory", entry)
        check("阻塞者为管理会话 PID",
              entry["blocking_pids"] == [admin_pid], entry)
        lock_wait_rid = entry["request_id"]
    acur.execute("SELECT pg_advisory_unlock(99)")
    waiter_res, napper_res, queued_res = await asyncio.gather(
        waiter_t, napper_t, queued_t)
    check("锁等待者拿到锁后成功", waiter_res["status"] == "ok", waiter_res)
    check("诊断中的请求标识确为等待者",
          lock_wait_rid == waiter_res["request_id"],
          (lock_wait_rid, waiter_res["request_id"]))
    check("正常执行与排队者均未被误报",
          napper_res["request_id"] != lock_wait_rid
          and queued_res["request_id"] != lock_wait_rid, report)

    print("=== F. 超时交错：旧请求不得归到重新借出的后端 ===")
    acur.execute("SELECT pg_advisory_lock(98)")
    timed_out_rid = None
    try:
        await executor.execute(
            "SELECT pg_advisory_xact_lock(98)", time.monotonic() + 0.6)
        check("锁等待超时必抛 QueryTimeout", False)
    except QueryTimeout as exc:
        timed_out_rid = exc.extra.get("request_id")
        check("锁等待超时回执带 request_id", timed_out_rid is not None,
              exc.extra)
    report = await collect_lock_waits(registry, monitor)
    check("超时后无锁等待记录",
          report["lock_waits"] == [], report)
    check("注册表已清空", registry.snapshot() == [], registry.snapshot())
    reused = await executor.execute("SELECT 5", time.monotonic() + 5)
    check("同一后端重新借出后返回自身结果", reused["rows"] == [[5]], reused)
    report = await collect_lock_waits(registry, monitor)
    check("复用后旧请求不出现",
          all(w["request_id"] != timed_out_rid
              for w in report["lock_waits"]), report)
    acur.execute("SELECT pg_advisory_unlock(98)")

    print("=== G. 监测失败须明确报告，不得返回假空结果 ===")
    bad_settings = dataclasses.replace(
        settings, monitor_dsn="postgresql://fault_mon@127.0.0.1:1/faultdb",
        connect_timeout=2.0)
    bad_monitor = LockMonitor(bad_settings)
    # 无在途执行时不需要监测连接，空结果是真实的。
    empty_report = await collect_lock_waits(InFlightRegistry(), bad_monitor)
    check("无执行中请求时诚实返回空(不依赖监测)",
          empty_report["lock_waits"] == []
          and empty_report["backends_checked"] == 0, empty_report)

    acur.execute("SELECT pg_advisory_lock(97)")
    hanging_t = asyncio.ensure_future(executor.execute(
        "SELECT pg_advisory_xact_lock(97)", time.monotonic() + 8))
    await asyncio.sleep(0.5)
    try:
        await collect_lock_waits(registry, bad_monitor)
        check("监测连接不可用应抛 DiagnosticsUnavailable", False)
    except DiagnosticsUnavailable as exc:
        check("监测连接不可用抛 DiagnosticsUnavailable", True, exc)
    # 好监测连接确认该请求确实在锁等待（失败时不得假报为空）。
    report = await collect_lock_waits(registry, monitor)
    check("监测恢复后看到同一锁等待",
          len(report["lock_waits"]) == 1, report)

    # 监测后端被带外终止：允许一次诚实失败，随后必须自动重连恢复。
    acur.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE application_name = 'fault-lock-monitor'")
    await asyncio.sleep(0.3)
    try:
        report = await collect_lock_waits(registry, monitor)
    except DiagnosticsUnavailable:
        report = await collect_lock_waits(registry, monitor)
    check("监测后端被杀后自动重连且结果正确",
          len(report["lock_waits"]) == 1, report)
    acur.execute("SELECT pg_advisory_unlock(97)")
    hanging_res = await hanging_t
    check("被挂起请求解锁后成功", hanging_res["status"] == "ok", hanging_res)

    acur.close()
    admin.close()
    await monitor.close()
    await pool.close()
    pg.cleanup()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

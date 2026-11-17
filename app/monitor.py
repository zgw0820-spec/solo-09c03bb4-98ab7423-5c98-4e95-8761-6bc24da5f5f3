"""Dedicated read-only monitoring connection for lock diagnostics.

The diagnostics endpoint must keep working while **both** query-pool
connections are busy (that is exactly when operators need it), so it gets
its own physical connection, completely outside :class:`QueryConnectionPool`.

The connection uses the separate ``fault_mon`` role (granted ``pg_monitor``)
and is only ever used for read-only catalogue queries against
``pg_stat_activity`` / ``pg_blocking_pids``.

Failure policy: any error — connect failure, dead backend, query timeout —
is raised as :class:`DiagnosticsUnavailable`.  The caller must surface that
honestly (HTTP 503) instead of returning a fabricated empty result.  The
broken connection is dropped and lazily rebuilt on the next call.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from psycopg import AsyncConnection

from .config import Settings
from .errors import DiagnosticsUnavailable

logger = logging.getLogger("monitor")

# One row per requested backend.  ``pg_blocking_pids`` is the authoritative
# source for *lock* blockage; wait_event_type/wait_event describe what the
# backend is currently waiting on (NULL when it is actively running).
_ACTIVITY_SQL = """
SELECT a.pid,
       a.wait_event_type,
       a.wait_event,
       a.state,
       pg_blocking_pids(a.pid) AS blocking_pids
FROM pg_stat_activity AS a
WHERE a.pid = ANY(%s)
"""


class LockMonitor:
    """Owns one resilient monitoring connection."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._conn: AsyncConnection | None = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def open(self) -> None:
        """Best-effort connect at startup; failures are logged, not fatal.

        Diagnostics calls reconnect lazily, so a database that is briefly
        unreachable at API startup must not take the whole service down.
        """
        try:
            await self._ensure_conn()
            logger.info("monitor connection established")
        except DiagnosticsUnavailable as exc:
            logger.warning("monitor connection not available at startup: %s", exc)

    async def close(self) -> None:
        await self._drop_conn()

    # ------------------------------------------------------------------ #
    async def snapshot_waits(self, pids: list[int]) -> dict[int, dict[str, Any]]:
        """Return wait/blocking info for ``pids``, keyed by backend PID.

        Raises :class:`DiagnosticsUnavailable` on any failure — never a
        partial or empty map that could be mistaken for "no lock waits".
        """
        if not pids:
            return {}
        conn = await self._ensure_conn()
        try:
            async with asyncio.timeout(self._settings.monitor_query_timeout):
                async with conn.cursor() as cur:
                    await cur.execute(_ACTIVITY_SQL, (list(pids),))
                    rows = await cur.fetchall()
        except DiagnosticsUnavailable:
            raise
        except Exception as exc:
            # Includes asyncio.TimeoutError: the monitoring query itself
            # exceeded its budget.  Drop the connection so the next call
            # gets a fresh backend, and report honestly.
            logger.warning("monitoring query failed: %s", exc)
            await self._drop_conn()
            raise DiagnosticsUnavailable(
                f"监测查询失败，无法诊断: {type(exc).__name__}: {exc}"
            ) from exc

        return {
            row[0]: {
                "pid": row[0],
                "wait_event_type": row[1],
                "wait_event": row[2],
                "state": row[3],
                "blocking_pids": list(row[4] or []),
            }
            for row in rows
        }

    # ------------------------------------------------------------------ #
    async def _ensure_conn(self) -> AsyncConnection:
        conn = self._conn
        if conn is not None and not conn.closed:
            return conn
        await self._drop_conn()
        try:
            conn = await AsyncConnection.connect(
                self._settings.monitor_dsn,
                connect_timeout=self._settings.connect_timeout,
                autocommit=True,
                application_name="fault-lock-monitor",
            )
            # Defence in depth: the diagnostics connection is read-only even
            # if the role were ever misconfigured.
            await conn.execute("SET default_transaction_read_only = on")
        except Exception as exc:
            raise DiagnosticsUnavailable(
                f"监测连接不可用，无法诊断: {type(exc).__name__}: {exc}"
            ) from exc
        self._conn = conn
        logger.info("monitor connection (re)established")
        return conn

    async def _drop_conn(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()

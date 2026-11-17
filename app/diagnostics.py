"""Lock-wait diagnostics: correlate in-flight requests with PG backends.

Only requests that are *currently executing* on a known backend PID can be
lock-waiting.  Queued requests (no backend yet) and normally executing
statements (e.g. ``pg_sleep`` waits, ``wait_event_type='Timeout'``) are
explicitly **not** lock waits.

To avoid attributing a backend to a stale request (timeout / retry /
connection re-borrowed while the catalogue query was in flight), the
registry is snapshotted twice — before and after the monitoring query —
and only entries identical in ``(request_id, token, backend_pid)`` across
both snapshots are reported.  Anything that changed mid-flight is dropped
from the report rather than guessed at.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from .monitor import LockMonitor
from .registry import InFlight, InFlightRegistry

logger = logging.getLogger("diagnostics")


def _identity(entry: InFlight) -> tuple[str, int, int | None]:
    return (entry.request_id, entry.token, entry.backend_pid)


async def collect_lock_waits(
    registry: InFlightRegistry,
    monitor: LockMonitor,
) -> dict[str, Any]:
    """Build the diagnostics report.

    Raises :class:`DiagnosticsUnavailable` (via the monitor) when the
    monitoring connection cannot answer — the endpoint maps that to an
    honest HTTP 503 instead of a fabricated empty list.
    """
    before = registry.snapshot()
    executing = [e for e in before if e.state == "executing" and e.backend_pid]
    queued = sum(1 for e in before if e.state != "executing")

    waits: list[dict[str, Any]] = []
    checked = 0
    skipped_unstable = 0

    if executing:
        rows = await monitor.snapshot_waits(
            [e.backend_pid for e in executing if e.backend_pid is not None]
        )
        checked = len(executing)
        # Second snapshot: only report entries that did not change while the
        # monitoring query ran (token guards timeout/retry/re-borrow races).
        stable = {
            _identity(e)
            for e in registry.snapshot()
            if e.state == "executing" and e.backend_pid
        }
        now = time.monotonic()
        for entry in executing:
            if _identity(entry) not in stable:
                skipped_unstable += 1
                continue
            row = rows.get(entry.backend_pid)
            if row is None:
                # Backend vanished (terminated out-of-band) — not a lock wait.
                continue
            blockers = row["blocking_pids"]
            if row["wait_event_type"] == "Lock" or blockers:
                started = entry.execution_started_at or entry.enqueued_at
                waits.append(
                    {
                        "request_id": entry.request_id,
                        "backend_pid": entry.backend_pid,
                        "wait_event_type": row["wait_event_type"],
                        "wait_event": row["wait_event"],
                        "blocking_pids": blockers,
                        # Lower bound: measured from statement start, since
                        # PG does not expose when the lock wait began.
                        "wait_duration_ms": round((now - started) * 1000, 3),
                        "sql": entry.sql,
                    }
                )

    return {
        "status": "ok",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "in_flight": {
            "executing": len(executing),
            "queued": queued,
        },
        "backends_checked": checked,
        "skipped_unstable": skipped_unstable,
        "lock_waits": waits,
    }

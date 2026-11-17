"""In-flight request registry: correlates API requests with PG backends.

The lock-wait diagnostics endpoint must answer "which *of our own* requests
is currently stuck on a database lock, and who blocks it".  That requires a
trustworthy mapping from request to the physical backend (PID) it is running
on.  This registry is that mapping.

Correctness rules implemented here:

* A request is registered as ``queued`` the moment it enters the executor,
  before any connection is borrowed — queued requests have **no** backend
  and must never show up as lock waits.
* ``attach`` binds the request to the borrowed connection's ``backend_pid``
  and returns a monotonically increasing **token**.  Every later state
  change must present that token, so a stale cleanup path (e.g. a timed-out
  request whose connection has already been re-borrowed by someone else)
  cannot detach or remove a newer registration.
* ``detach`` (retry path) clears the backend binding but keeps the request
  registered as queued: after a connection-level error the old backend must
  no longer be attributed to this request.
* ``finish`` removes the registration.  The executor calls it *before*
  returning the connection to the pool, so the next borrower of the same
  backend PID is never confused with the previous request.

All methods are synchronous and mutate plain dicts; on a single asyncio
event loop each call is atomic between awaits, so no locking is needed.
"""
from __future__ import annotations

import itertools
import time
import uuid
from dataclasses import dataclass

MAX_SQL_TEXT = 200


@dataclass(frozen=True)
class InFlight:
    """Immutable snapshot of one in-flight request."""

    request_id: str
    token: int
    state: str  # "queued" | "executing"
    backend_pid: int | None
    sql: str
    enqueued_at: float  # monotonic
    execution_started_at: float | None  # monotonic, set on attach


class InFlightRegistry:
    def __init__(self) -> None:
        self._items: dict[str, InFlight] = {}
        self._tokens = itertools.count(1)

    # ------------------------------------------------------------------ #
    def enqueue(self, sql: str) -> str:
        """Register a freshly arrived request; returns its request id."""
        request_id = uuid.uuid4().hex
        self._items[request_id] = InFlight(
            request_id=request_id,
            token=0,
            state="queued",
            backend_pid=None,
            sql=sql[:MAX_SQL_TEXT],
            enqueued_at=time.monotonic(),
            execution_started_at=None,
        )
        return request_id

    def attach(self, request_id: str, backend_pid: int) -> int:
        """Bind the request to a borrowed backend; returns the new token."""
        current = self._items.get(request_id)
        if current is None:
            # Defensive: request already finished (should not happen).
            return 0
        token = next(self._tokens)
        self._items[request_id] = InFlight(
            request_id=request_id,
            token=token,
            state="executing",
            backend_pid=backend_pid,
            sql=current.sql,
            enqueued_at=current.enqueued_at,
            execution_started_at=time.monotonic(),
        )
        return token

    def detach(self, request_id: str, token: int) -> None:
        """Unbind from the backend (connection lost, statement retried).

        The request stays registered as ``queued`` with no PID so the dead
        backend is no longer attributed to it.
        """
        current = self._items.get(request_id)
        if current is None or current.token != token:
            return
        self._items[request_id] = InFlight(
            request_id=request_id,
            token=current.token,
            state="queued",
            backend_pid=None,
            sql=current.sql,
            enqueued_at=current.enqueued_at,
            execution_started_at=None,
        )

    def finish(self, request_id: str, token: int | None = None) -> None:
        """Remove the registration.

        With ``token`` given, removal happens only when the token matches
        the current registration — a stale cleanup from an earlier borrow
        (timeout/retry/re-borrow) can never erase a newer registration.
        Without a token the entry is removed unconditionally (final
        backstop in the executor's ``finally``).
        """
        current = self._items.get(request_id)
        if current is None:
            return
        if token is not None and current.token != token:
            return
        del self._items[request_id]

    # ------------------------------------------------------------------ #
    def snapshot(self) -> list[InFlight]:
        """Consistent point-in-time copy of all in-flight requests."""
        return list(self._items.values())

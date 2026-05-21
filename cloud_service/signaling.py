"""
Signaling layer: WebSocket connection management + dead-robot eviction.

In this step (step 4), the signaling WebSocket carries one message type:
HeartbeatMessage. The robot opens a persistent WebSocket to the cloud
right after registration and sends a heartbeat every N seconds. The
cloud updates the registry's last_heartbeat_ts on receipt.

In step 6, this same module will gain handlers for session_start /
peer_ready / session_live / session_end. The SignalingMessage tagged
union in schemas.py already covers those types — we just don't dispatch
on them yet.

CONNECTION MANAGER:
We keep a per-robot WebSocket reference in ConnectionManager. This serves
two purposes:
  1. Heartbeat tracking: incoming heartbeats look up the WebSocket's
     robot_id to update the right registry entry.
  2. Outbound signaling (step 6): when the cloud needs to push a
     session_start to a specific robot, it looks up the WebSocket here.

DEAD-ROBOT EVICTION:
A background task runs every HEARTBEAT_CHECK_INTERVAL_S seconds, scans the
registry, and marks any robot offline whose last heartbeat is older than
HEARTBEAT_TIMEOUT_S. The thresholds are conservative (3x the heartbeat
interval) to tolerate transient network blips without false eviction.

DESIGN: WHY MARK_OFFLINE INSTEAD OF REMOVE
A dead robot is moved to "offline" status but kept in the registry. The
alternative (remove from registry) would mean users running `user list`
right after a robot crash would not see it at all, with no record that
it ever existed. Marking offline preserves visibility ("robot-1 was
here, currently down") which is useful for operators triaging incidents.
The robot can re-register on restart, which transitions it back to online
via touch_heartbeat (see Registry).
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

from fastapi import WebSocket, WebSocketDisconnect
from pydantic import TypeAdapter, ValidationError

from cloud_service.registry import Registry
from common.logging import get_logger
from common.schemas import HeartbeatMessage, SignalingMessage


_log = get_logger("signaling")


# Timing constants. HEARTBEAT_INTERVAL_S must match what the cloud reports
# to robots in RobotRegisterResponse (see schemas). HEARTBEAT_TIMEOUT_S
# is the wall-clock age at which a robot is declared offline.
HEARTBEAT_INTERVAL_S: float = 2.0
HEARTBEAT_TIMEOUT_S: float = 6.0      # 3x interval — tolerant of one missed heartbeat
HEARTBEAT_CHECK_INTERVAL_S: float = 1.0  # eviction sweep frequency


# Pydantic TypeAdapter that decodes any SignalingMessage variant based on its
# `type` discriminator field. Constructed once at module load to avoid per-
# message overhead.
_signaling_decoder = TypeAdapter(SignalingMessage)


class ConnectionManager:
    """
    Tracks active WebSocket connections, keyed by robot_id and session_id.

    Two parallel dicts because robot connections and user connections are
    addressed differently — a robot is uniquely identified by its robot_id,
    a user is identified by the session_id the cloud minted for them.
    Looking up the "user for this session" is therefore O(1) without
    indirection through a robot.

    Thread/task safety: methods are async but all mutations happen on the
    event loop thread, so no explicit locking is needed. The registry
    (separate concern) has its own thread-safe lock for the cases where
    sync code touches it.
    """

    def __init__(self):
        self._robots: dict[str, WebSocket] = {}
        self._users: dict[str, WebSocket] = {}

    async def attach_robot(self, robot_id: str, ws: WebSocket) -> None:
        """Register a WebSocket as the active connection for a robot."""
        prior = self._robots.get(robot_id)
        if prior is not None:
            _log.warning(
                f"robot {robot_id!r} reconnected; closing prior WebSocket"
            )
            try:
                await prior.close(code=1001, reason="Replaced by new connection")
            except Exception:
                pass
        self._robots[robot_id] = ws

    def detach_robot(self, robot_id: str) -> None:
        self._robots.pop(robot_id, None)

    def get_robot(self, robot_id: str) -> Optional[WebSocket]:
        return self._robots.get(robot_id)

    async def attach_user(self, session_id: str, ws: WebSocket) -> None:
        """Register a WebSocket as the active connection for a user session."""
        prior = self._users.get(session_id)
        if prior is not None:
            _log.warning(
                f"user session {session_id!r} reconnected; closing prior WebSocket"
            )
            try:
                await prior.close(code=1001, reason="Replaced by new connection")
            except Exception:
                pass
        self._users[session_id] = ws

    def detach_user(self, session_id: str) -> None:
        self._users.pop(session_id, None)

    def get_user(self, session_id: str) -> Optional[WebSocket]:
        return self._users.get(session_id)

    def __len__(self) -> int:
        return len(self._robots) + len(self._users)


# Backwards-compatible aliases so the existing robot_ws_handler keeps working
# (it calls attach/detach/get). We can drop these in step 6 when refactoring.
def _robot_attach(mgr: ConnectionManager, robot_id: str, ws: WebSocket):
    return mgr.attach_robot(robot_id, ws)


def _robot_detach(mgr: ConnectionManager, robot_id: str):
    mgr.detach_robot(robot_id)


def _robot_get(mgr: ConnectionManager, robot_id: str):
    return mgr.get_robot(robot_id)


async def robot_ws_handler(
    ws: WebSocket,
    robot_id: str,
    registry: Registry,
    connections: ConnectionManager,
) -> None:
    """
    Per-robot WebSocket handler.

    Lifecycle:
      1. Accept the connection.
      2. Verify the robot is registered (reject otherwise — robots must
         POST /robots/register before opening their WebSocket).
      3. Register the WebSocket in the ConnectionManager.
      4. Loop: receive JSON messages, decode as SignalingMessage, dispatch.
         For step 4, only heartbeat is meaningful; other types are logged
         and ignored until step 6 implements them.
      5. On disconnect or error: detach from ConnectionManager and mark
         the robot offline (the next register call will bring it back
         online).
    """
    await ws.accept()

    if robot_id not in registry:
        _log.warning(
            f"WebSocket open for unregistered robot {robot_id!r}; closing"
        )
        await ws.close(code=4004, reason="Robot not registered")
        return

    await connections.attach_robot(robot_id, ws)
    _log.info(f"robot {robot_id!r} signaling WebSocket connected")

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = _signaling_decoder.validate_json(raw)
            except ValidationError as exc:
                _log.warning(
                    f"robot {robot_id!r} sent malformed signaling message: {exc}"
                )
                continue

            if isinstance(msg, HeartbeatMessage):
                registry.touch_heartbeat(robot_id)
            else:
                # Step 6 will handle peer_ready and other variants. For now,
                # log and drop.
                _log.debug(
                    f"robot {robot_id!r} sent {msg.type!r} "
                    f"(not yet handled in step 4)"
                )

    except WebSocketDisconnect:
        _log.info(f"robot {robot_id!r} signaling WebSocket disconnected")
    except Exception:
        _log.exception(f"robot {robot_id!r} signaling WebSocket error")
    finally:
        connections.detach_robot(robot_id)
        # We mark offline rather than removing entirely; see module docstring.
        registry.mark_offline(robot_id)


async def user_ws_handler(
    ws: WebSocket,
    session_id: str,
    connections: ConnectionManager,
) -> None:
    """
    Per-user-session WebSocket handler.

    In step 5 (this step), the handler just accepts the connection, attaches
    it to the ConnectionManager, and parks. The cloud doesn't send anything
    on this channel yet — that work is step 6's session signaling.

    Why open it at all today? Two reasons:
      1. End-to-end testability: the `user connect` command can be exercised
         against a real cloud, and we verify it reaches the right endpoint.
      2. Slot for step 6: when session signaling lands, every piece (URL,
         routing, lifecycle) is already wired; only the message-handler
         logic needs to be added.

    Note: session validation (does this session_id correspond to a real
    pending session?) is deferred to step 6 along with the rest of the
    session lifecycle.
    """
    await ws.accept()
    await connections.attach_user(session_id, ws)
    _log.info(f"user session {session_id!r} signaling WebSocket connected")

    try:
        while True:
            raw = await ws.receive_text()
            # User-initiated signaling messages (peer_ready) arrive in step 6.
            # Today we log and drop anything inbound.
            _log.debug(f"user {session_id!r} sent inbound message: {raw!r}")
    except WebSocketDisconnect:
        _log.info(f"user session {session_id!r} signaling WebSocket disconnected")
    except Exception:
        _log.exception(f"user session {session_id!r} signaling WebSocket error")
    finally:
        connections.detach_user(session_id)


async def heartbeat_eviction_loop(
    registry: Registry,
    connections: ConnectionManager,
    timeout_s: float = HEARTBEAT_TIMEOUT_S,
    check_interval_s: float = HEARTBEAT_CHECK_INTERVAL_S,
) -> None:
    """
    Periodic sweep that marks robots offline if their heartbeat is stale.

    Runs forever (until cancelled). Intended to be launched on cloud
    startup via FastAPI's lifespan handler.

    Edge case: a robot whose WebSocket disconnected has already been
    marked offline by robot_ws_handler's finally block, so this loop
    is mostly a safety net for cases where the cloud doesn't see the
    disconnect (kernel-level TCP timeout, etc.).
    """
    while True:
        try:
            await asyncio.sleep(check_interval_s)
            now = time.time()
            for r in registry.list_robots():
                if r.status == "online" and (now - r.last_heartbeat_ts) > timeout_s:
                    _log.warning(
                        f"robot {r.robot_id!r} heartbeat timeout "
                        f"({now - r.last_heartbeat_ts:.1f}s > {timeout_s}s); "
                        f"marking offline"
                    )
                    registry.mark_offline(r.robot_id)
                    # If we still hold a WebSocket reference, drop it too.
                    ws = connections.get_robot(r.robot_id)
                    if ws is not None:
                        try:
                            await ws.close(code=1001, reason="Heartbeat timeout")
                        except Exception:
                            pass
                        connections.detach_robot(r.robot_id)
        except asyncio.CancelledError:
            _log.info("heartbeat eviction loop stopped")
            raise
        except Exception:
            # Never let the sweep loop die — log and continue.
            _log.exception("heartbeat eviction loop error (continuing)")
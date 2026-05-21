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
    Tracks active WebSocket connections, keyed by robot_id.

    Thread/task safety: methods are async but all mutations happen on the
    event loop thread, so no explicit locking is needed. The registry
    (separate concern) has its own thread-safe lock for the cases where
    sync code touches it.
    """

    def __init__(self):
        self._connections: dict[str, WebSocket] = {}

    async def attach(self, robot_id: str, ws: WebSocket) -> None:
        """
        Register a WebSocket as the active connection for robot_id.
        If a prior WebSocket existed for this robot_id, it is closed —
        matching the replace-on-duplicate policy from registration.
        """
        prior = self._connections.get(robot_id)
        if prior is not None:
            _log.warning(
                f"robot {robot_id!r} reconnected; closing prior WebSocket"
            )
            try:
                await prior.close(code=1001, reason="Replaced by new connection")
            except Exception:
                pass  # prior socket may already be in a bad state
        self._connections[robot_id] = ws

    def detach(self, robot_id: str) -> None:
        """Remove the WebSocket entry for a robot. Idempotent."""
        self._connections.pop(robot_id, None)

    def get(self, robot_id: str) -> Optional[WebSocket]:
        return self._connections.get(robot_id)

    def __len__(self) -> int:
        return len(self._connections)


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

    await connections.attach(robot_id, ws)
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
        connections.detach(robot_id)
        # We mark offline rather than removing entirely; see module docstring.
        registry.mark_offline(robot_id)


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
                    ws = connections.get(r.robot_id)
                    if ws is not None:
                        try:
                            await ws.close(code=1001, reason="Heartbeat timeout")
                        except Exception:
                            pass
                        connections.detach(r.robot_id)
        except asyncio.CancelledError:
            _log.info("heartbeat eviction loop stopped")
            raise
        except Exception:
            # Never let the sweep loop die — log and continue.
            _log.exception("heartbeat eviction loop error (continuing)")
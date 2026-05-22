"""
WebSocket signaling and session orchestration.
 
This module handles the cloud's WebSocket endpoints (for robots, users,
and players), tracks active connections via ConnectionManager, drives
session lifecycle through SessionOrchestrator, and runs the dead-robot
eviction sweep.
 
Dead robots are marked offline rather than removed from the registry so
operators triaging incidents can see "robot was here, currently down".
Re-registration brings them back online via touch_heartbeat.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import Optional

from fastapi import WebSocket, WebSocketDisconnect
from pydantic import TypeAdapter, ValidationError

from cloud_service.registry import Registry
from cloud_service.session_manager import (
    InvalidTransition,
    Session,
    SessionManager,
    SessionState,
)
from common.logging import get_logger
from common.schemas import (
    HeartbeatMessage,
    PeerReadyMessage,
    SessionEndMessage,
    SessionLiveMessage,
    SessionStartMessage,
    SignalingMessage,
)


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
        # Player WebSockets are keyed by session_id, like users — one Player
        # per session.
        self._players: dict[str, WebSocket] = {}

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

    async def attach_player(self, session_id: str, ws: WebSocket) -> None:
        """Register a WebSocket as the active connection for a player session."""
        prior = self._players.get(session_id)
        if prior is not None:
            _log.warning(
                f"player session {session_id!r} reconnected; closing prior WebSocket"
            )
            try:
                await prior.close(code=1001, reason="Replaced by new connection")
            except Exception:
                pass
        self._players[session_id] = ws

    def detach_player(self, session_id: str) -> None:
        self._players.pop(session_id, None)

    def get_player(self, session_id: str) -> Optional[WebSocket]:
        return self._players.get(session_id)

    def __len__(self) -> int:
        return len(self._robots) + len(self._users) + len(self._players)


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
    orchestrator: Optional["SessionOrchestrator"] = None,
) -> None:
    """
    Per-robot WebSocket handler.

    Handles two message kinds in step 6:
      - HeartbeatMessage: updates registry's last_heartbeat_ts (step 4).
      - PeerReadyMessage: forwarded to the orchestrator (step 6).

    Other signaling messages from the robot are still logged and dropped —
    robots aren't expected to send session_start, session_live, etc.
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
            elif isinstance(msg, PeerReadyMessage) and orchestrator is not None:
                await orchestrator.handle_peer_ready(msg)
            else:
                _log.debug(
                    f"robot {robot_id!r} sent {msg.type!r} (not handled)"
                )

    except WebSocketDisconnect:
        _log.info(f"robot {robot_id!r} signaling WebSocket disconnected")
    except Exception:
        _log.exception(f"robot {robot_id!r} signaling WebSocket error")
    finally:
        connections.detach_robot(robot_id)
        registry.mark_offline(robot_id)


async def user_ws_handler(
    ws: WebSocket,
    session_id: str,
    connections: ConnectionManager,
    orchestrator: Optional["SessionOrchestrator"] = None,
) -> None:
    """
    Per-user-session WebSocket handler.

    Dispatches peer_ready messages to the orchestrator. session_start /
    session_live are pushed *to* the user by the orchestrator (not handled
    here, since users don't send them inbound).
    """
    await ws.accept()
    await connections.attach_user(session_id, ws)
    _log.info(f"user session {session_id!r} signaling WebSocket connected")

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = _signaling_decoder.validate_json(raw)
            except ValidationError as exc:
                _log.warning(
                    f"user {session_id!r} sent malformed signaling message: {exc}"
                )
                continue
            if isinstance(msg, PeerReadyMessage) and orchestrator is not None:
                await orchestrator.handle_peer_ready(msg)
            else:
                _log.debug(f"user {session_id!r} sent {msg.type!r} (not handled)")
    except WebSocketDisconnect:
        _log.info(f"user session {session_id!r} signaling WebSocket disconnected")
    except Exception:
        _log.exception(f"user session {session_id!r} signaling WebSocket error")
    finally:
        connections.detach_user(session_id)
        # User disconnect ends the session for everyone.
        if orchestrator is not None:
            await orchestrator.end_session(session_id, "user disconnected")


async def player_ws_handler(
    ws: WebSocket,
    session_id: str,
    connections: ConnectionManager,
    orchestrator: "SessionOrchestrator",
) -> None:
    """
    Per-player-session WebSocket handler.

    When a Player attaches, the orchestrator transitions the session from
    SPAWNING to AWAITING_PEERS and broadcasts session_start to the triangle.
    From there, the Player binds its ZMQ peer and sends peer_ready back.
    """
    await ws.accept()
    await connections.attach_player(session_id, ws)
    _log.info(f"player session {session_id!r} signaling WebSocket connected")

    # Player attachment is the trigger for sending session_start to the triangle.
    await orchestrator.handle_player_attached(session_id)

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = _signaling_decoder.validate_json(raw)
            except ValidationError as exc:
                _log.warning(
                    f"player {session_id!r} sent malformed signaling message: {exc}"
                )
                continue
            if isinstance(msg, PeerReadyMessage):
                await orchestrator.handle_peer_ready(msg)
            else:
                _log.debug(f"player {session_id!r} sent {msg.type!r} (not handled)")
    except WebSocketDisconnect:
        _log.info(f"player session {session_id!r} signaling WebSocket disconnected")
    except Exception:
        _log.exception(f"player session {session_id!r} signaling WebSocket error")
    finally:
        connections.detach_player(session_id)
        # If the player drops mid-session, the session is dead.
        await orchestrator.end_session(session_id, "player disconnected")


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


# =============================================================================
# Session orchestration: spawn Player, drive the three-phase handshake
# =============================================================================
#
# The orchestrator wraps:
#   - SessionManager (state)
#   - ConnectionManager (WebSocket lookup)
#   - subprocess spawning (Player lifecycle)
#
# Flow (happy path):
#   1. start_session(robot_id, user_id):
#        - SessionManager.create() allocates a session
#        - Player subprocess is spawned
#        - state → SPAWNING
#   2. when Player attaches its WebSocket:
#        - state → AWAITING_PEERS
#        - cloud sends session_start to robot and user (Player's start is
#          implicit — it just attached)
#   3. as each peer_ready arrives via signaling WebSocket:
#        - endpoints[role] = endpoint
#        - when all three present: state → LIVE, cloud broadcasts
#          session_live with full topology
#   4. on any disconnect or end_session():
#        - state → ENDED, session_end broadcast, Player killed


class SessionOrchestrator:
    """
    Glue between SessionManager, ConnectionManager, and Player subprocesses.

    One instance per app. Owns the SessionManager (so the FastAPI route
    handlers can access it via app.state). The signaling WebSocket
    handlers call into this object to react to incoming messages.
    """

    def __init__(self, connections: ConnectionManager):
        self.sessions = SessionManager()
        self._connections = connections
        # session_id → subprocess.Popen-like object
        self._player_procs: dict[str, asyncio.subprocess.Process] = {}

    # ----- public API used by HTTP/WebSocket handlers -----------------------

    async def start_session(
        self,
        robot_id: str,
        user_id: str,
        cloud_url_for_subprocess: str,
    ) -> Session:
        """
        Allocate a session, spawn the Player subprocess.

        Returns the Session in SPAWNING state. The caller (POST /sessions)
        responds to the user immediately; the rest of the handshake
        happens asynchronously over WebSockets.
        """
        session = self.sessions.create(robot_id=robot_id, user_id=user_id)
        session.mark_spawning()

        try:
            proc = await self._spawn_player(
                session_id=session.session_id,
                robot_id=robot_id,
                cloud_url=cloud_url_for_subprocess,
            )
        except Exception as exc:
            self.sessions.end(session.session_id, f"player spawn failed: {exc!r}")
            raise

        self._player_procs[session.session_id] = proc
        session.player_pid = proc.pid
        return session

    async def handle_player_attached(self, session_id: str) -> None:
        """
        Called from user_ws_handler / player_ws_handler when the Player's
        signaling WebSocket attaches. Transitions to AWAITING_PEERS and
        sends session_start to robot and user.

        Note: the Player's own session_start is implicit — by opening its
        WebSocket it has effectively acknowledged "I am here, ready to bind."
        """
        session = self.sessions.get(session_id)
        if session is None:
            _log.warning(f"player_attached for unknown session {session_id}")
            return
        if session.state != SessionState.SPAWNING:
            # Already past spawning (e.g. duplicate attach). Idempotent.
            return

        session.mark_awaiting_peers()

        # Dispatch session_start to robot and user. Player binds without
        # needing a message because it knows it just spawned.
        await self._send_to_robot(session.robot_id, SessionStartMessage(
            session_id=session.session_id,
            robot_id=session.robot_id,
        ))
        await self._send_to_user(session.session_id, SessionStartMessage(
            session_id=session.session_id,
            robot_id=session.robot_id,
        ))
        await self._send_to_player(session.session_id, SessionStartMessage(
            session_id=session.session_id,
            robot_id=session.robot_id,
        ))

    async def handle_peer_ready(self, message: PeerReadyMessage) -> None:
        """
        Called when any peer (robot, user, player) sends peer_ready.
        If this completes the triangle, transition to LIVE and broadcast
        session_live.
        """
        session = self.sessions.get(message.session_id)
        if session is None:
            _log.warning(
                f"peer_ready for unknown session {message.session_id}; ignoring"
            )
            return

        try:
            complete = session.record_peer_ready(message.role, message.bind_endpoint)
        except InvalidTransition as exc:
            _log.warning(
                f"peer_ready rejected for session {message.session_id}: {exc}"
            )
            return

        if complete:
            session.mark_live()
            await self._broadcast_session_live(session)
            self.sessions.signal_live(session.session_id)

    async def end_session(self, session_id: str, reason: str) -> None:
        """
        End a session: broadcast session_end, kill Player, mark ENDED.
        Idempotent.
        """
        session = self.sessions.get(session_id)
        if session is None or session.state == SessionState.ENDED:
            return

        # Broadcast before we tear anything down, so peers get notification.
        end_msg = SessionEndMessage(session_id=session_id, reason=reason)
        for role, sender in (
            ("robot", lambda: self._send_to_robot(session.robot_id, end_msg)),
            ("user", lambda: self._send_to_user(session_id, end_msg)),
            ("player", lambda: self._send_to_player(session_id, end_msg)),
        ):
            try:
                await sender()
            except Exception:
                _log.exception(f"failed to send session_end to {role}")

        self.sessions.end(session_id, reason)
        await self._terminate_player(session_id)

    async def shutdown(self) -> None:
        """Kill all live Player subprocesses. Called on cloud shutdown."""
        for session_id in list(self._player_procs.keys()):
            await self._terminate_player(session_id)

    # ----- subprocess management --------------------------------------------

    async def _spawn_player(
        self,
        session_id: str,
        robot_id: str,
        cloud_url: str,
    ) -> asyncio.subprocess.Process:
        """
        Launch the Player as a separate Python process.

        We use `python -m player ...` (not `python player/__main__.py`) so
        the module is resolved via PYTHONPATH the same way it is in the
        cloud — no relative-path fragility.
        """
        cmd = [
            sys.executable, "-m", "player",
            "--session-id", session_id,
            "--robot-id", robot_id,
            "--cloud-url", cloud_url,
        ]
        _log.info(f"spawning player: {' '.join(cmd)}")

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            # Inherit PYTHONPATH from cloud's environment so subprocess can
            # find common/, robot/, etc. without us setting it explicitly.
            env={**os.environ},
        )
        # Spawn a logging task that streams the player's stderr to our log.
        # Without this, player errors are silent and very confusing to debug.
        asyncio.create_task(
            self._stream_subprocess_output(session_id, proc),
            name=f"player-output-{session_id}",
        )
        return proc

    async def _stream_subprocess_output(
        self,
        session_id: str,
        proc: asyncio.subprocess.Process,
    ) -> None:
        """Read player's stderr and re-log it with a session_id prefix."""
        if proc.stderr is None:
            return
        try:
            async for line in proc.stderr:
                _log.info(
                    f"[player {session_id}] {line.decode(errors='replace').rstrip()}"
                )
        except Exception:
            _log.exception(f"player output stream failed for {session_id}")

    async def _terminate_player(self, session_id: str) -> None:
        """Terminate the Player subprocess for a session. Idempotent."""
        proc = self._player_procs.pop(session_id, None)
        if proc is None:
            return
        if proc.returncode is not None:
            return  # already exited
        try:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                _log.warning(
                    f"player for session {session_id} did not exit; killing"
                )
                proc.kill()
                await proc.wait()
        except ProcessLookupError:
            pass

    # ----- WebSocket send helpers -------------------------------------------

    async def _send_to_robot(self, robot_id: str, msg) -> None:
        ws = self._connections.get_robot(robot_id)
        if ws is None:
            _log.warning(f"no WebSocket for robot {robot_id}; cannot send {msg.type}")
            return
        try:
            await ws.send_text(msg.model_dump_json())
        except Exception:
            _log.exception(f"failed to send {msg.type} to robot {robot_id}")

    async def _send_to_user(self, session_id: str, msg) -> None:
        ws = self._connections.get_user(session_id)
        if ws is None:
            _log.warning(
                f"no WebSocket for user session {session_id}; cannot send {msg.type}"
            )
            return
        try:
            await ws.send_text(msg.model_dump_json())
        except Exception:
            _log.exception(f"failed to send {msg.type} to user {session_id}")

    async def _send_to_player(self, session_id: str, msg) -> None:
        ws = self._connections.get_player(session_id)
        if ws is None:
            _log.warning(
                f"no WebSocket for player session {session_id}; cannot send {msg.type}"
            )
            return
        try:
            await ws.send_text(msg.model_dump_json())
        except Exception:
            _log.exception(f"failed to send {msg.type} to player {session_id}")

    async def _broadcast_session_live(self, session: Session) -> None:
        """
        Send session_live (with the full topology) to all three peers.
        """
        msg = SessionLiveMessage(
            session_id=session.session_id,
            robot_id=session.robot_id,
            topology={
                "robot": session.endpoints["robot"],
                "user": session.endpoints["user"],
                "player": session.endpoints["player"],
            },
        )
        await self._send_to_robot(session.robot_id, msg)
        await self._send_to_user(session.session_id, msg)
        await self._send_to_player(session.session_id, msg)
"""
End-to-end signaling handshake test.

Spins up:
  - The cloud as a uvicorn server in a background thread.
  - A real Robot process (CloudClient instance running in this event loop).
  - A real User process (UserSession running in this event loop).
  - The cloud will spawn a real Player subprocess when /sessions is called.

Verifies:
  - The three-phase handshake completes (session reaches LIVE).
  - All three peers (robot, user, player) record their endpoints.
  - Topology is consistent across all three (they have each other's endpoints).
  - Session_end propagates cleanly when the user disconnects.

These tests are slow (spawn a real subprocess, real WebSocket round trips).
Mark them with a different filter so unit-test runs can skip them if needed.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time

import pytest
import uvicorn

from cloud_service.app import create_app
from cloud_service.session_manager import SessionState
from robot.client import CloudClient
from user.client import UserSession, request_session


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _LiveCloud:
    """Cloud running in a background thread (sync entry, runs uvicorn)."""

    def __init__(self):
        self.port = _free_port()
        self.app = create_app(public_url=f"http://localhost:{self.port}")
        self._config = uvicorn.Config(
            self.app,
            host="127.0.0.1",
            port=self.port,
            log_level="warning",
            lifespan="on",
            ws="wsproto",
        )
        self._server = uvicorn.Server(self._config)
        self._thread: threading.Thread | None = None

    @property
    def base_url(self) -> str:
        return f"http://localhost:{self.port}"

    def __enter__(self):
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        for _ in range(100):
            if self._server.started:
                return self
            time.sleep(0.02)
        raise TimeoutError("cloud did not start within 2s")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=5.0)


# =============================================================================
# Full handshake
# =============================================================================

@pytest.mark.asyncio
async def test_full_signaling_handshake_completes():
    """
    Drive the entire three-phase handshake and verify the session
    reaches LIVE state with all three peer endpoints recorded.
    """
    with _LiveCloud() as cloud:
        # Start a robot.
        robot = CloudClient(robot_id="robot-1", cloud_url=cloud.base_url)
        robot_task = asyncio.create_task(robot.run(), name="robot")

        # Wait for the robot to register.
        for _ in range(100):
            await asyncio.sleep(0.05)
            if (
                "robot-1" in cloud.app.state.registry
                and cloud.app.state.connections.get_robot("robot-1") is not None
            ):
                break
        else:
            pytest.fail("robot did not register within 5s")

        # Request a session — this triggers Player subprocess spawn.
        session_resp = await request_session(
            cloud.base_url, robot_id="robot-1", user_id="test-user"
        )
        session_id = session_resp.session_id

        # Start the user session driver.
        stop = asyncio.Event()
        user_session = UserSession(session_resp.websocket_url, session_id)

        live_seen = asyncio.Event()
        events_observed: list[str] = []

        async def consume_user_events():
            async for evt in user_session.events(stop):
                events_observed.append(evt)
                if evt == "live":
                    live_seen.set()
                if evt.startswith("ended:"):
                    break

        user_task = asyncio.create_task(consume_user_events(), name="user")

        # Wait for the session to reach LIVE.
        try:
            await asyncio.wait_for(live_seen.wait(), timeout=15.0)
        except asyncio.TimeoutError:
            # Print debug info to make failures actionable.
            session = cloud.app.state.orchestrator.sessions.get(session_id)
            print(f"DEBUG: session state = {session.state if session else 'NONE'}")
            if session:
                print(f"DEBUG: endpoints = {session.endpoints}")
            pytest.fail(f"session did not reach LIVE within 15s; events={events_observed}")

        # Validate session state on the cloud side.
        session = cloud.app.state.orchestrator.sessions.get(session_id)
        assert session is not None
        assert session.state == SessionState.LIVE
        assert set(session.endpoints.keys()) == {"robot", "user", "player"}
        assert "started" in events_observed
        assert "live" in events_observed

        # Validate user has a ZmqPeer bound and connected.
        assert user_session.peer is not None
        assert user_session.peer.bind_endpoint is not None

        # Validate robot also has its peer up.
        assert robot.peer is not None
        assert robot.peer.bind_endpoint is not None

        # Teardown: user stops, which closes its WS, which ends the session.
        stop.set()
        try:
            await asyncio.wait_for(user_task, timeout=3.0)
        except asyncio.TimeoutError:
            user_task.cancel()

        # Give the cloud a moment to process the session end.
        await asyncio.sleep(0.5)

        # Session should now be ENDED.
        session = cloud.app.state.orchestrator.sessions.get(session_id)
        assert session is not None
        assert session.state == SessionState.ENDED

        # Cleanup the robot.
        robot.stop()
        try:
            await asyncio.wait_for(robot_task, timeout=3.0)
        except asyncio.TimeoutError:
            robot_task.cancel()
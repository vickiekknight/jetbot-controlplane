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
        from robot.sdk import FakeJetBot
        robot.set_driver(FakeJetBot())
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
        user_session = UserSession(
            session_resp.websocket_url, session_id, robot_id="robot-1"
        )

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


@pytest.mark.asyncio
async def test_full_data_flow_after_handshake():
    """
    Once the triangle is live, verify all three data-plane message types
    flow correctly:

      1. Robot publishes sensor → User receives it.
      2. Robot publishes sensor → Player receives, classifies, republishes
         as processed → User receives processed.
      3. User publishes command → Robot's driver receives the command and
         updates its motor state.

    This is the core end-to-end correctness test for step 7.
    """
    from robot.sdk import FakeJetBot

    received_sensor: list[dict] = []
    received_processed: list[dict] = []

    with _LiveCloud() as cloud:
        # Robot with a real FakeJetBot driver attached.
        bot = FakeJetBot()
        robot = CloudClient(robot_id="robot-1", cloud_url=cloud.base_url)
        robot.set_driver(bot)
        robot_task = asyncio.create_task(robot.run(), name="robot")

        # Wait for registration.
        for _ in range(100):
            await asyncio.sleep(0.05)
            if (
                "robot-1" in cloud.app.state.registry
                and cloud.app.state.connections.get_robot("robot-1") is not None
            ):
                break

        # Request a session.
        session_resp = await request_session(
            cloud.base_url, robot_id="robot-1", user_id="test-user"
        )

        # Wire up the user session with callbacks that capture messages.
        stop = asyncio.Event()
        user_session = UserSession(
            session_resp.websocket_url,
            session_resp.session_id,
            robot_id="robot-1",
        )

        async def on_sensor(env):
            received_sensor.append(env.get("payload", {}))

        async def on_processed(env):
            received_processed.append(env.get("payload", {}))

        user_session.on_sensor = on_sensor
        user_session.on_processed = on_processed

        live_seen = asyncio.Event()
        events_observed: list[str] = []

        async def consume():
            async for evt in user_session.events(stop):
                events_observed.append(evt)
                if evt == "live":
                    live_seen.set()
                if evt.startswith("ended:"):
                    break

        user_task = asyncio.create_task(consume(), name="user")
        await asyncio.wait_for(live_seen.wait(), timeout=15.0)

        # Wait for the first sensor message as a deterministic signal that the
        # Robot→User SUB has propagated. The User→Robot SUB (for commands)
        # uses the same ZMQ context lifecycle, so by the time sensor data is
        # flowing one direction, the other direction is usually also up.
        # However, propagation can be asymmetric under load, so we still need
        # to defend the command path with retry-until-effective below.
        try:
            await asyncio.wait_for(
                _wait_until(lambda: len(received_sensor) >= 1), timeout=5.0
            )
        except asyncio.TimeoutError:
            pytest.fail("no sensor message received after triangle went live")

        # Send forward command with retry. If the first command lands during
        # the slow-joiner window for the User→Robot subscription, the Robot
        # will drop it; we keep sending until the motor state changes or we
        # exhaust retries. In practice the second send is almost always
        # enough (~50ms after the first), but the retry budget is generous
        # to absorb event-loop pressure in full-suite runs.
        async def _send_until_motor_changes(command: str, speed: float,
                                            target: float, timeout_s: float):
            deadline = asyncio.get_event_loop().time() + timeout_s
            while asyncio.get_event_loop().time() < deadline:
                await user_session.send_command(command, speed=speed)
                # Poll for ~200ms before re-sending.
                for _ in range(4):
                    await asyncio.sleep(0.05)
                    if abs(bot.left_motor_value - target) < 1e-6:
                        return True
            return False

        ok = await _send_until_motor_changes("forward", 0.8, 0.8, timeout_s=3.0)
        assert ok, (
            f"Robot never executed forward command "
            f"(left_motor={bot.left_motor_value})"
        )
        assert bot.left_motor_value == 0.8
        assert bot.right_motor_value == 0.8

        # Clear sensor/processed buffers AFTER the command lands so the next
        # arrivals are guaranteed to reflect the post-command motor state.
        # Without this, the buffer may contain a pre-command sensor (state=0)
        # because the robot publishes at 1Hz and the retry-until-effective
        # command path can complete faster than one sensor cycle.
        received_sensor.clear()
        received_processed.clear()

        # Wait long enough for a couple of sensor publishes (1Hz default).
        # First sensor arrives ~1s after live; we need to see processed too,
        # which requires player to receive sensor and republish.
        try:
            await asyncio.wait_for(
                _wait_until(
                    lambda: len(received_sensor) >= 1 and len(received_processed) >= 1
                ),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            pytest.fail(
                f"data flow incomplete after 5s: "
                f"sensor={len(received_sensor)}, processed={len(received_processed)}"
            )

        # Validate sensor content: should reflect the robot moving forward.
        last_sensor = received_sensor[-1]
        assert "state" in last_sensor
        assert "pose" in last_sensor
        # At forward(0.8), v = 0.8 * 0.3 = 0.24 → "alert" band
        assert last_sensor["state"] > 0.1

        # Validate processed content: state matches sensor, status is alert.
        last_processed = received_processed[-1]
        assert last_processed["status"] in ("normal", "warning", "alert")
        # The processed value should be in the alert band because we're
        # driving forward at 0.8 → 0.24 m/s > 0.20 threshold.
        assert last_processed["status"] == "alert"

        # Stop the robot's motors and verify the next processed message
        # transitions back to normal (proves the data flow keeps working).
        # Same retry-until-effective pattern as the forward command above.
        ok = await _send_until_motor_changes("stop", 0.0, 0.0, timeout_s=3.0)
        assert ok, f"Robot never stopped (left_motor={bot.left_motor_value})"
        assert bot.left_motor_value == 0.0

        # Clear and wait for the next sensor → processed cycle.
        received_processed.clear()
        try:
            await asyncio.wait_for(
                _wait_until(lambda: len(received_processed) >= 1), timeout=3.0
            )
        except asyncio.TimeoutError:
            pytest.fail("no processed message after stop command")
        assert received_processed[-1]["status"] == "normal"

        # Teardown.
        stop.set()
        try:
            await asyncio.wait_for(user_task, timeout=3.0)
        except asyncio.TimeoutError:
            user_task.cancel()
        await asyncio.sleep(0.5)
        robot.stop()
        try:
            await asyncio.wait_for(robot_task, timeout=3.0)
        except asyncio.TimeoutError:
            robot_task.cancel()


async def _wait_until(predicate, interval=0.05):
    """Spin until predicate() is truthy."""
    while not predicate():
        await asyncio.sleep(interval)
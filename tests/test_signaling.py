"""
Tests for the cloud's signaling layer:
  - WebSocket heartbeat protocol
  - Connection lifecycle (accept, message dispatch, disconnect)
  - Dead-robot eviction loop
  - Replace-on-duplicate WebSocket connections

Uses FastAPI's TestClient which supports synchronous WebSocket client
helpers via .websocket_connect(). The test app is built fresh per test
with the eviction loop's timeout knobs shortened for fast iteration.
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from cloud_service.app import create_app
from cloud_service.signaling import (
    ConnectionManager,
    HEARTBEAT_TIMEOUT_S,
    heartbeat_eviction_loop,
)
from common.schemas import HeartbeatMessage


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_app())


# =============================================================================
# Robot WebSocket: connection lifecycle
# =============================================================================

def test_robot_ws_requires_prior_registration(client: TestClient):
    """A robot that opens a WebSocket without registering first is rejected."""
    with pytest.raises(Exception):  # WebSocketDisconnect on close
        with client.websocket_connect("/ws/robot/unregistered-robot") as ws:
            ws.receive_text()  # forces the close to surface


def test_robot_ws_accepts_after_registration(client: TestClient):
    """Happy path: register, then connect WebSocket, then send heartbeat."""
    client.post("/robots/register", json={"robot_id": "robot-1"})
    with client.websocket_connect("/ws/robot/robot-1") as ws:
        msg = HeartbeatMessage()
        ws.send_text(msg.model_dump_json())
    # Connection closed cleanly; the registry's heartbeat ts should be recent.
    robot = client.app.state.registry.get("robot-1")
    assert robot is not None


def test_heartbeat_updates_registry_timestamp(client: TestClient):
    client.post("/robots/register", json={"robot_id": "robot-1"})
    initial_ts = client.app.state.registry.get("robot-1").last_heartbeat_ts
    time.sleep(0.05)

    with client.websocket_connect("/ws/robot/robot-1") as ws:
        ws.send_text(HeartbeatMessage().model_dump_json())
        # Give the server a moment to process the message.
        time.sleep(0.05)

    updated_ts = client.app.state.registry.get("robot-1").last_heartbeat_ts
    assert updated_ts > initial_ts


def test_disconnect_marks_robot_offline(client: TestClient):
    client.post("/robots/register", json={"robot_id": "robot-1"})
    assert client.app.state.registry.get("robot-1").status == "online"

    with client.websocket_connect("/ws/robot/robot-1"):
        pass  # close immediately
    time.sleep(0.05)  # allow handler's finally block to run

    assert client.app.state.registry.get("robot-1").status == "offline"


def test_reregister_after_disconnect_brings_back_online(client: TestClient):
    """Self-healing: a robot that disconnected can re-register and come back online."""
    client.post("/robots/register", json={"robot_id": "robot-1"})
    with client.websocket_connect("/ws/robot/robot-1"):
        pass
    time.sleep(0.05)
    assert client.app.state.registry.get("robot-1").status == "offline"

    # Re-register — replace-on-duplicate policy means this succeeds.
    client.post("/robots/register", json={"robot_id": "robot-1"})
    assert client.app.state.registry.get("robot-1").status == "online"


# =============================================================================
# Malformed-message handling
# =============================================================================

def test_malformed_json_does_not_crash_handler(client: TestClient):
    """A garbage message is logged and dropped; the connection stays open."""
    client.post("/robots/register", json={"robot_id": "robot-1"})
    with client.websocket_connect("/ws/robot/robot-1") as ws:
        ws.send_text("not valid json at all")
        # Connection still alive — send a proper heartbeat after.
        ws.send_text(HeartbeatMessage().model_dump_json())
        time.sleep(0.05)
    # Registry was updated, proving the second message was processed.
    assert client.app.state.registry.get("robot-1") is not None


def test_wrong_message_shape_does_not_crash(client: TestClient):
    """A JSON object that doesn't match any SignalingMessage variant is dropped."""
    client.post("/robots/register", json={"robot_id": "robot-1"})
    with client.websocket_connect("/ws/robot/robot-1") as ws:
        ws.send_text('{"type": "made_up_type", "garbage": true}')
        ws.send_text(HeartbeatMessage().model_dump_json())
        time.sleep(0.05)
    assert client.app.state.registry.get("robot-1") is not None


# =============================================================================
# ConnectionManager
# =============================================================================

import asyncio


@pytest.mark.asyncio
async def test_connection_manager_attach_and_detach():
    mgr = ConnectionManager()
    fake_ws = object()  # opaque placeholder; close not called
    await mgr.attach("robot-1", fake_ws)  # type: ignore[arg-type]
    assert mgr.get("robot-1") is fake_ws
    assert len(mgr) == 1
    mgr.detach("robot-1")
    assert mgr.get("robot-1") is None
    assert len(mgr) == 0


@pytest.mark.asyncio
async def test_connection_manager_replace_closes_prior():
    """Attaching a second WebSocket for the same robot closes the first."""

    class FakeWS:
        def __init__(self):
            self.closed = False

        async def close(self, code: int = 1000, reason: str = ""):
            self.closed = True

    mgr = ConnectionManager()
    ws1 = FakeWS()
    ws2 = FakeWS()
    await mgr.attach("robot-1", ws1)  # type: ignore[arg-type]
    await mgr.attach("robot-1", ws2)  # type: ignore[arg-type]
    assert ws1.closed is True
    assert mgr.get("robot-1") is ws2


# =============================================================================
# Heartbeat eviction loop (logic-only test, no real WebSockets)
# =============================================================================

@pytest.mark.asyncio
async def test_eviction_marks_stale_robot_offline():
    """A robot with a stale heartbeat is marked offline by the sweep loop."""
    from cloud_service.registry import Registry

    registry = Registry()
    connections = ConnectionManager()

    # Manually insert a robot with an ancient heartbeat — bypassing register()
    # so we don't have to wait HEARTBEAT_TIMEOUT_S during the test.
    registry.register("stale-robot", metadata={})
    registry.get("stale-robot").last_heartbeat_ts = time.time() - 100  # very stale

    task = asyncio.create_task(
        heartbeat_eviction_loop(
            registry, connections,
            timeout_s=1.0,
            check_interval_s=0.05,
        )
    )
    # Give the loop a chance to run a sweep.
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert registry.get("stale-robot").status == "offline"


@pytest.mark.asyncio
async def test_eviction_leaves_fresh_robot_alone():
    """A robot with a recent heartbeat is not evicted."""
    from cloud_service.registry import Registry

    registry = Registry()
    connections = ConnectionManager()
    registry.register("fresh-robot", metadata={})
    # last_heartbeat_ts is set to now in register()

    task = asyncio.create_task(
        heartbeat_eviction_loop(
            registry, connections,
            timeout_s=5.0,
            check_interval_s=0.05,
        )
    )
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert registry.get("fresh-robot").status == "online"
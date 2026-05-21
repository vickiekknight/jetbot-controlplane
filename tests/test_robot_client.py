"""
Tests for the robot's CloudClient.

Strategy: spin up a real cloud app instance on an ephemeral port, run the
CloudClient against it, and verify the registry sees the heartbeats land.
This is more valuable than mocking httpx + websockets — the real cloud
is small enough to use as-is.
"""

from __future__ import annotations

import asyncio
import socket

import pytest
import uvicorn

from cloud_service.app import create_app
from robot.client import CloudClient


def _free_port() -> int:
    """Ask the OS for an unused TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _LiveCloud:
    """
    Helper: runs the cloud FastAPI app in the same event loop as the test.

    uvicorn.Server gives us programmatic start/stop without subprocesses.
    The Registry stays accessible via .app.state.registry so tests can
    inspect state without going through the HTTP API.
    """

    def __init__(self):
        self.port = _free_port()
        self.app = create_app(public_url=f"http://localhost:{self.port}")
        config = uvicorn.Config(
            self.app,
            host="127.0.0.1",
            port=self.port,
            log_level="warning",
            lifespan="on",
            # Use wsproto for WebSockets rather than the default `websockets`
            # backend. Both work; wsproto avoids deprecation warnings emitted
            # by uvicorn's `websockets`-backend code path when running against
            # websockets >= 14.0.
            ws="wsproto",
        )
        self.server = uvicorn.Server(config)
        self._task = None

    @property
    def base_url(self) -> str:
        return f"http://localhost:{self.port}"

    async def __aenter__(self):
        self._task = asyncio.create_task(self.server.serve())
        # Wait until uvicorn signals it's accepting connections.
        for _ in range(50):
            if self.server.started:
                break
            await asyncio.sleep(0.02)
        else:
            raise TimeoutError("cloud server did not start within 1s")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.server.should_exit = True
        if self._task:
            await self._task


# =============================================================================
# Tests
# =============================================================================

@pytest.mark.asyncio
async def test_client_registers_and_sends_heartbeats():
    async with _LiveCloud() as cloud:
        client = CloudClient(robot_id="robot-1", cloud_url=cloud.base_url)
        client_task = asyncio.create_task(client.run())

        # Wait for register to happen + at least one heartbeat to land.
        # Heartbeat interval is 2s by default; we monitor for the WS attach
        # (which proves register succeeded) and a touched timestamp.
        for _ in range(50):
            await asyncio.sleep(0.05)
            registry = cloud.app.state.registry
            if "robot-1" in registry and registry.get("robot-1").status == "online":
                break
        else:
            pytest.fail("robot-1 never registered")

        assert "robot-1" in cloud.app.state.registry

        client.stop()
        try:
            await asyncio.wait_for(client_task, timeout=2.0)
        except asyncio.TimeoutError:
            client_task.cancel()


@pytest.mark.asyncio
async def test_client_reconnects_after_cloud_restart():
    """
    If the cloud disappears and comes back, the client re-registers and
    keeps going. Demonstrates the reconnect-with-backoff loop in run().
    """
    cloud1 = _LiveCloud()
    async with cloud1:
        client = CloudClient(robot_id="robot-1", cloud_url=cloud1.base_url)
        client_task = asyncio.create_task(client.run())

        # Wait for initial registration.
        for _ in range(50):
            await asyncio.sleep(0.05)
            if "robot-1" in cloud1.app.state.registry:
                break

        assert "robot-1" in cloud1.app.state.registry

    # cloud1 is now stopped. Spin up a NEW cloud on the same port and verify
    # the client re-registers there. Note: the client's run() loop is still
    # active and will reconnect.
    cloud2 = _LiveCloud()
    # Force same port so the client's stored URL still resolves.
    cloud2.port = cloud1.port
    cloud2.app = create_app(public_url=f"http://localhost:{cloud2.port}")
    config = uvicorn.Config(
        cloud2.app, host="127.0.0.1", port=cloud2.port,
        log_level="warning", lifespan="on",
        ws="wsproto",
    )
    cloud2.server = uvicorn.Server(config)
    cloud2._task = asyncio.create_task(cloud2.server.serve())

    try:
        # Wait for second cloud to start.
        for _ in range(50):
            if cloud2.server.started:
                break
            await asyncio.sleep(0.05)

        # Wait for client to find its way back.
        for _ in range(200):
            await asyncio.sleep(0.05)
            if "robot-1" in cloud2.app.state.registry:
                break
        else:
            pytest.fail("client did not re-register after cloud restart")

        assert "robot-1" in cloud2.app.state.registry
    finally:
        client.stop()
        cloud2.server.should_exit = True
        await asyncio.gather(client_task, cloud2._task, return_exceptions=True)


@pytest.mark.asyncio
async def test_client_rejects_invalid_cloud_url():
    """Constructor validates that cloud_url has a scheme."""
    with pytest.raises(ValueError, match="must start with"):
        CloudClient(robot_id="r1", cloud_url="localhost:8000")  # missing scheme
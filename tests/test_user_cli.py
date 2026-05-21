"""
Tests for the user CLI: `user list` and `user connect`.

Strategy mirrors test_robot_client.py — spin up a real cloud on an
ephemeral port and run the CLI against it. Typer's CliRunner invokes
commands the same way the shell would, capturing stdout/stderr/exit
codes for assertion.

We don't mock httpx/websockets because the cost of running the real
thing is small and the integration coverage is meaningful.
"""

from __future__ import annotations

import asyncio
import socket

import pytest
from typer.testing import CliRunner

from cloud_service.app import create_app
from user.cli import app as cli_app


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ----- live cloud helper (sync, runs in a background thread) -----

import threading
import time
import uvicorn


class _LiveCloud:
    """
    Runs the cloud in a background thread. We use a thread (not asyncio) so
    that Typer's sync CliRunner can drive the CLI in the main thread while
    the cloud serves real HTTP/WebSocket requests in the background.
    """

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
        # Stub the orchestrator's spawn so CLI tests don't fork Python procs.
        async def fake_spawn(session_id, robot_id, cloud_url):
            class FakeProc:
                pid = -1
                returncode = 0
                def terminate(self): pass
                def kill(self): pass
                async def wait(self): return 0
                stderr = None
            return FakeProc()

        self.app.state.orchestrator._spawn_player = fake_spawn

        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        # Wait until uvicorn signals it's accepting connections.
        for _ in range(50):
            if self._server.started:
                return self
            time.sleep(0.02)
        raise TimeoutError("cloud server did not start within 1s")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=2.0)


# =============================================================================
# `user list`
# =============================================================================

def test_list_with_no_robots_shows_friendly_message():
    runner = CliRunner()
    with _LiveCloud() as cloud:
        result = runner.invoke(cli_app, ["list", "--cloud-url", cloud.base_url])
    assert result.exit_code == 0
    assert "No robots are currently registered" in result.stdout


def test_list_with_one_robot_shows_in_table():
    runner = CliRunner()
    with _LiveCloud() as cloud:
        # Pre-register a robot directly via the registry (faster than HTTP).
        cloud.app.state.registry.register("robot-1", metadata={"loc": "lab-A"})
        result = runner.invoke(cli_app, ["list", "--cloud-url", cloud.base_url])
    assert result.exit_code == 0
    assert "robot-1" in result.stdout
    assert "online" in result.stdout


def test_list_with_multiple_robots_shows_all():
    runner = CliRunner()
    with _LiveCloud() as cloud:
        cloud.app.state.registry.register("robot-1", metadata={})
        cloud.app.state.registry.register("robot-2", metadata={})
        cloud.app.state.registry.register("robot-3", metadata={})
        result = runner.invoke(cli_app, ["list", "--cloud-url", cloud.base_url])
    assert result.exit_code == 0
    for rid in ("robot-1", "robot-2", "robot-3"):
        assert rid in result.stdout


def test_list_shows_offline_robots():
    runner = CliRunner()
    with _LiveCloud() as cloud:
        cloud.app.state.registry.register("robot-1", metadata={})
        cloud.app.state.registry.mark_offline("robot-1")
        result = runner.invoke(cli_app, ["list", "--cloud-url", cloud.base_url])
    assert result.exit_code == 0
    assert "offline" in result.stdout


def test_list_against_unreachable_cloud_exits_nonzero():
    runner = CliRunner()
    # Random unused port. No server there.
    result = runner.invoke(
        cli_app, ["list", "--cloud-url", f"http://localhost:{_free_port()}"]
    )
    assert result.exit_code == 1
    assert "could not reach cloud" in result.stderr or "error" in result.stderr


# =============================================================================
# `user connect`
# =============================================================================

def test_connect_to_nonexistent_robot_exits_nonzero():
    runner = CliRunner()
    with _LiveCloud() as cloud:
        result = runner.invoke(
            cli_app,
            ["connect", "ghost-robot", "--cloud-url", cloud.base_url],
        )
    assert result.exit_code == 1
    assert "not registered" in result.stderr or "error" in result.stderr


def test_connect_to_existing_robot_opens_session(monkeypatch):
    """
    Happy path: robot exists, cloud accepts the session, CLI opens the
    WebSocket and waits. We monkeypatch open_user_signaling to return
    immediately so the test doesn't block forever.
    """
    runner = CliRunner()
    with _LiveCloud() as cloud:
        cloud.app.state.registry.register("robot-1", metadata={})

        # Patch the signaling iterator to be empty — opens WS, yields
        # nothing, returns. The CLI then prints "disconnected" and exits.
        async def fake_signaling(ws_url, stop):
            # Return an empty async generator. We never enter the body.
            return
            yield  # pragma: no cover

        monkeypatch.setattr("user.cli.open_user_signaling", fake_signaling)

        result = runner.invoke(
            cli_app,
            ["connect", "robot-1", "--cloud-url", cloud.base_url],
        )

    assert result.exit_code == 0, f"stderr: {result.stderr}\nstdout: {result.stdout}"
    assert "session created" in result.stdout
    assert "disconnected" in result.stdout


def test_connect_to_offline_robot_exits_nonzero():
    runner = CliRunner()
    with _LiveCloud() as cloud:
        cloud.app.state.registry.register("robot-1", metadata={})
        cloud.app.state.registry.mark_offline("robot-1")
        result = runner.invoke(
            cli_app,
            ["connect", "robot-1", "--cloud-url", cloud.base_url],
        )
    assert result.exit_code == 1
    assert "offline" in result.stderr or "error" in result.stderr


def test_connect_uses_generated_user_id_when_not_provided(monkeypatch):
    """If --user-id is omitted, a random user_id is generated."""
    runner = CliRunner()
    captured_user_ids: list[str] = []

    async def fake_request_session(cloud_url, robot_id, user_id):
        captured_user_ids.append(user_id)
        # Raise to short-circuit the rest of the flow.
        from user.client import UserClientError
        raise UserClientError("test short-circuit")

    monkeypatch.setattr("user.cli.request_session", fake_request_session)

    result = runner.invoke(
        cli_app,
        ["connect", "robot-1", "--cloud-url", "http://localhost:9999"],
    )
    assert result.exit_code == 1  # expected — we forced an error
    assert len(captured_user_ids) == 1
    assert captured_user_ids[0].startswith("user-")
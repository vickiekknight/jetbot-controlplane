"""
HTTP integration tests for the cloud service REST API.

Uses FastAPI's TestClient (which uses httpx under the hood) to drive the
ASGI app directly, no real server needed. Each test constructs a fresh
app via create_app() to ensure no state leaks between tests.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from cloud_service.app import create_app


@pytest.fixture
def client() -> TestClient:
    """Fresh app + client per test. Isolated state."""
    app = create_app(public_url="http://localhost:8000")

    # Stub the orchestrator's Player subprocess spawn so HTTP-only tests
    # don't actually fork Python processes. The session reaches SPAWNING
    # state but no real Player exists.
    async def fake_spawn(session_id, robot_id, cloud_url):
        class FakeProc:
            pid = -1
            returncode = 0  # already "exited"

            def terminate(self): pass
            def kill(self): pass
            async def wait(self): return 0
            stderr = None
        return FakeProc()

    app.state.orchestrator._spawn_player = fake_spawn  # type: ignore[method-assign]
    return TestClient(app)


# =============================================================================
# POST /robots/register
# =============================================================================

def test_register_robot_returns_websocket_url(client: TestClient):
    response = client.post(
        "/robots/register",
        json={"robot_id": "robot-1", "metadata": {"location": "lab"}},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["robot_id"] == "robot-1"
    assert body["websocket_url"] == "ws://localhost:8000/ws/robot/robot-1"


def test_register_with_minimal_payload(client: TestClient):
    """metadata is optional and defaults to {}"""
    response = client.post("/robots/register", json={"robot_id": "robot-2"})
    assert response.status_code == 200


def test_register_with_missing_robot_id_returns_422(client: TestClient):
    """Pydantic validation kicks in for malformed requests."""
    response = client.post("/robots/register", json={"metadata": {}})
    assert response.status_code == 422


def test_duplicate_registration_succeeds_with_replace(client: TestClient):
    """Two registrations for the same robot_id both succeed; second wins."""
    r1 = client.post("/robots/register", json={"robot_id": "robot-1", "metadata": {"v": 1}})
    r2 = client.post("/robots/register", json={"robot_id": "robot-1", "metadata": {"v": 2}})
    assert r1.status_code == 200
    assert r2.status_code == 200

    # Only one entry in the listing, with the latest metadata
    listing = client.get("/robots").json()
    robots = [r for r in listing["robots"] if r["robot_id"] == "robot-1"]
    assert len(robots) == 1
    assert robots[0]["metadata"] == {"v": 2}


# =============================================================================
# GET /robots
# =============================================================================

def test_list_robots_empty(client: TestClient):
    response = client.get("/robots")
    assert response.status_code == 200
    assert response.json() == {"robots": []}


def test_list_robots_returns_all_registered(client: TestClient):
    client.post("/robots/register", json={"robot_id": "robot-1"})
    client.post("/robots/register", json={"robot_id": "robot-2"})
    client.post("/robots/register", json={"robot_id": "robot-3"})

    listing = client.get("/robots").json()
    robot_ids = {r["robot_id"] for r in listing["robots"]}
    assert robot_ids == {"robot-1", "robot-2", "robot-3"}


def test_list_robots_includes_status(client: TestClient):
    client.post("/robots/register", json={"robot_id": "robot-1"})
    body = client.get("/robots").json()
    assert body["robots"][0]["status"] == "online"


# =============================================================================
# POST /sessions
# =============================================================================

def test_session_for_existing_robot_returns_201(client: TestClient):
    client.post("/robots/register", json={"robot_id": "robot-1"})
    response = client.post(
        "/sessions", json={"robot_id": "robot-1", "user_id": "user-a"}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["robot_id"] == "robot-1"
    assert body["session_id"].startswith("sess_")
    # WebSocket URL points at the user-side signaling endpoint
    assert body["websocket_url"].startswith("ws://localhost:8000/ws/user/")
    assert body["session_id"] in body["websocket_url"]


def test_session_for_nonexistent_robot_returns_404(client: TestClient):
    response = client.post(
        "/sessions", json={"robot_id": "ghost", "user_id": "user-a"}
    )
    assert response.status_code == 404
    assert "not registered" in response.json()["detail"]


def test_session_for_offline_robot_returns_409(client: TestClient):
    """If a robot is offline, sessions can't be created for it."""
    client.post("/robots/register", json={"robot_id": "robot-1"})
    # Reach in via app.state to flip status (in production this would
    # happen via heartbeat timeout in step 4).
    client.app.state.registry.mark_offline("robot-1")

    response = client.post(
        "/sessions", json={"robot_id": "robot-1", "user_id": "user-a"}
    )
    assert response.status_code == 409
    assert "offline" in response.json()["detail"]


def test_session_ids_are_unique(client: TestClient):
    client.post("/robots/register", json={"robot_id": "robot-1"})
    seen_ids = set()
    for _ in range(20):
        r = client.post("/sessions", json={"robot_id": "robot-1", "user_id": "u"})
        seen_ids.add(r.json()["session_id"])
    assert len(seen_ids) == 20  # all distinct


# =============================================================================
# Public URL handling
# =============================================================================

def test_public_url_https_produces_wss(client_factory=None):
    """An https:// public URL should yield wss:// websocket URLs."""
    app = create_app(public_url="https://cloud.example.com")
    client = TestClient(app)
    response = client.post("/robots/register", json={"robot_id": "robot-1"})
    assert response.json()["websocket_url"] == "wss://cloud.example.com/ws/robot/robot-1"


def test_public_url_strips_trailing_slash():
    """Trailing slashes in the public URL shouldn't produce double slashes."""
    app = create_app(public_url="http://cloud.example.com/")
    client = TestClient(app)
    response = client.post("/robots/register", json={"robot_id": "robot-1"})
    assert "//ws" not in response.json()["websocket_url"]
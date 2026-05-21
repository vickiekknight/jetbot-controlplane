"""
FastAPI app for the cloud service control plane.

This module defines three REST endpoints:

  POST /robots/register   Robot announces itself on startup.
  GET  /robots            User lists currently-registered robots.
  POST /sessions          User requests a session with a specific robot.

The signaling WebSocket endpoints (which heartbeat and orchestrate
sessions) are added in step 4 and step 6.

DESIGN:
The app is constructed via a factory function (create_app) rather than
declared at module scope. This is because:
  - Tests can construct independent app instances with fresh Registry
    state, without module-level mutable globals leaking between tests.
  - The factory can take configuration (public URL for advertised
    WebSocket URLs in responses) without environment-variable side
    effects.
  - Multiple apps can coexist in the same process if needed
    (multi-region scenarios, mocked downstream services, etc.).

The Registry is attached to app.state and accessed via a FastAPI
dependency, which makes it overridable in tests (dependency_overrides)
and clear from the function signature what each handler needs.
"""

from __future__ import annotations

import os
import secrets
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, status
import asyncio

from cloud_service.registry import Registry
from cloud_service.signaling import (
    ConnectionManager,
    heartbeat_eviction_loop,
    robot_ws_handler,
    user_ws_handler,
)
from common.logging import configure_logging, get_logger
from common.schemas import (
    RobotListResponse,
    RobotRegisterRequest,
    RobotRegisterResponse,
    SessionRequest,
    SessionResponse,
)


_log = get_logger("cloud_service")


def _get_registry(request: Request) -> Registry:
    """FastAPI dependency: extract the Registry attached to app.state."""
    return request.app.state.registry


def _ws_url(public_url: str, path: str) -> str:
    """
    Convert the cloud's HTTP public URL into a WebSocket URL with the given
    path. e.g. http://localhost:8000 + /ws/robot/r1 → ws://localhost:8000/ws/robot/r1.
    https → wss similarly.
    """
    if public_url.startswith("https://"):
        return "wss://" + public_url.removeprefix("https://").rstrip("/") + path
    if public_url.startswith("http://"):
        return "ws://" + public_url.removeprefix("http://").rstrip("/") + path
    # Fallback: assume it's already a host:port and pick ws://
    return f"ws://{public_url.rstrip('/')}{path}"


def create_app(public_url: str = "http://localhost:8000") -> FastAPI:
    """
    Build a FastAPI app instance.

    Args:
        public_url: the externally-reachable URL of the cloud service.
                    Used to construct WebSocket URLs that are returned
                    to robots and users. Defaults to localhost:8000 for
                    single-host development.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Start the heartbeat eviction sweep loop as a background task.
        # Stored on app.state so tests can introspect/cancel it directly.
        app.state.eviction_task = asyncio.create_task(
            heartbeat_eviction_loop(app.state.registry, app.state.connections),
            name="heartbeat-eviction",
        )
        _log.info("cloud service started")
        try:
            yield
        finally:
            app.state.eviction_task.cancel()
            try:
                await app.state.eviction_task
            except asyncio.CancelledError:
                pass
            _log.info("cloud service stopped")

    app = FastAPI(
        title="JetBot Control Plane",
        description="Cloud orchestrator for the robot/user/player triangle.",
        version="0.1.0",
        lifespan=lifespan,
    )

    # State: one Registry + one ConnectionManager per app instance, both
    # attached to app.state for access via dependencies. Tests can swap
    # either by overriding the corresponding dependency.
    app.state.registry = Registry()
    app.state.connections = ConnectionManager()
    app.state.public_url = public_url

    # ----- POST /robots/register -----------------------------------------

    @app.post(
        "/robots/register",
        response_model=RobotRegisterResponse,
        status_code=status.HTTP_200_OK,
        summary="Register a robot with the cloud service",
    )
    async def register_robot(
        req: RobotRegisterRequest,
        registry: Registry = Depends(_get_registry),
    ) -> RobotRegisterResponse:
        registry.register(req.robot_id, req.metadata)
        return RobotRegisterResponse(
            robot_id=req.robot_id,
            websocket_url=_ws_url(app.state.public_url, f"/ws/robot/{req.robot_id}"),
        )

    # ----- GET /robots ---------------------------------------------------

    @app.get(
        "/robots",
        response_model=RobotListResponse,
        summary="List all robots currently registered",
    )
    async def list_robots(
        registry: Registry = Depends(_get_registry),
    ) -> RobotListResponse:
        return RobotListResponse(robots=registry.list_robots())

    # ----- POST /sessions ------------------------------------------------

    @app.post(
        "/sessions",
        response_model=SessionResponse,
        status_code=status.HTTP_201_CREATED,
        summary="Request a session with a specific robot",
        responses={
            404: {"description": "Robot not found in registry"},
            409: {"description": "Robot exists but is not online"},
        },
    )
    async def create_session(
        req: SessionRequest,
        registry: Registry = Depends(_get_registry),
    ) -> SessionResponse:
        robot = registry.get(req.robot_id)
        if robot is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Robot {req.robot_id!r} is not registered",
            )
        if robot.status != "online":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Robot {req.robot_id!r} is currently {robot.status!r} "
                    f"and cannot accept a session"
                ),
            )

        # Session IDs are random hex strings prefixed for readability in logs.
        # NOT a security boundary — these aren't auth tokens. They're a handle
        # the user will use when opening its signaling WebSocket. The actual
        # session-state lifecycle starts when signaling happens (step 6); for
        # now we just generate the ID and tell the user where to connect.
        session_id = f"sess_{secrets.token_hex(6)}"
        _log.info(
            f"session {session_id} requested by user={req.user_id!r} "
            f"for robot={req.robot_id!r}"
        )
        return SessionResponse(
            session_id=session_id,
            robot_id=req.robot_id,
            websocket_url=_ws_url(app.state.public_url, f"/ws/user/{session_id}"),
        )

    # ----- WebSocket: /ws/robot/{robot_id} -------------------------------
    # The robot's signaling channel. Heartbeats today; session signaling
    # in step 6. See cloud_service/signaling.py for the protocol.

    @app.websocket("/ws/robot/{robot_id}")
    async def robot_signaling(ws: WebSocket, robot_id: str):
        await robot_ws_handler(
            ws=ws,
            robot_id=robot_id,
            registry=app.state.registry,
            connections=app.state.connections,
        )

    # ----- WebSocket: /ws/user/{session_id} ------------------------------
    # The user's signaling channel. In step 5 (now) the connection is
    # accepted but the cloud doesn't send anything on it; step 6 fills in
    # the session_start / session_live dispatch logic.

    @app.websocket("/ws/user/{session_id}")
    async def user_signaling(ws: WebSocket, session_id: str):
        await user_ws_handler(
            ws=ws,
            session_id=session_id,
            connections=app.state.connections,
        )

    return app


# Module-level app for `uvicorn cloud_service.app:app` style invocation.
# Honors a CLOUD_PUBLIC_URL env var so the user can override without
# editing code; defaults to localhost:8000 to match __main__.py.
import os
app = create_app(public_url=os.environ.get("CLOUD_PUBLIC_URL", "http://localhost:8000"))
configure_logging()
"""
HTTP and WebSocket client logic for the user CLI.

Separated from cli.py so this module can be tested in isolation — pure
async functions/classes that take a cloud URL and parameters and return
parsed results, no terminal output, no Typer dependency.

The CLI layer (cli.py) handles:
  - argument parsing
  - rendering tables / status messages
  - signal handling for SIGINT
  - exit codes

This layer handles:
  - HTTP requests and response parsing
  - WebSocket connection lifecycle + signaling handshake
  - ZmqPeer bind/connect driven by session_start / session_live
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator, Awaitable, Callable, Optional

import httpx
import websockets
from pydantic import TypeAdapter, ValidationError

from common.logging import get_logger
from common.schemas import (
    CommandPayload,
    PeerReadyMessage,
    RobotInfo,
    RobotListResponse,
    SessionEndMessage,
    SessionLiveMessage,
    SessionRequest,
    SessionResponse,
    SessionStartMessage,
    SignalingMessage,
)
from common.topics import Topics
from common.zmq_peer import ZmqPeer


_signaling_decoder = TypeAdapter(SignalingMessage)


class UserClientError(Exception):
    """Raised for any user-visible error talking to the cloud."""


_log = get_logger("user.client")


async def list_robots(cloud_url: str, timeout_s: float = 5.0) -> list[RobotInfo]:
    """GET /robots → parsed list."""
    url = f"{cloud_url.rstrip('/')}/robots"
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as http:
            response = await http.get(url)
            response.raise_for_status()
            return RobotListResponse.model_validate(response.json()).robots
    except httpx.HTTPStatusError as exc:
        raise UserClientError(
            f"cloud returned {exc.response.status_code}: {exc.response.text}"
        ) from exc
    except httpx.RequestError as exc:
        raise UserClientError(f"could not reach cloud at {url}: {exc}") from exc


async def request_session(
    cloud_url: str,
    robot_id: str,
    user_id: str,
    timeout_s: float = 5.0,
) -> SessionResponse:
    """POST /sessions → parsed response."""
    url = f"{cloud_url.rstrip('/')}/sessions"
    req = SessionRequest(robot_id=robot_id, user_id=user_id)
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as http:
            response = await http.post(url, json=req.model_dump())
            response.raise_for_status()
            return SessionResponse.model_validate(response.json())
    except httpx.HTTPStatusError as exc:
        try:
            detail = exc.response.json().get("detail", exc.response.text)
        except Exception:
            detail = exc.response.text
        raise UserClientError(
            f"cloud rejected session request ({exc.response.status_code}): {detail}"
        ) from exc
    except httpx.RequestError as exc:
        raise UserClientError(f"could not reach cloud at {url}: {exc}") from exc


class UserSession:
    """
    Drives the user side of the three-phase signaling handshake AND the
    data plane.

    Usage:
        session = UserSession(websocket_url, session_id, robot_id)
        session.on_sensor = lambda env: ...
        session.on_processed = lambda env: ...
        session.on_status = lambda env: ...
        async for event in session.events(stop_event):
            ...
        # while session is live:
        await session.send_command("forward", speed=0.5)
    """

    def __init__(self, websocket_url: str, session_id: str, robot_id: Optional[str] = None):
        self.websocket_url = websocket_url
        self.session_id = session_id
        # robot_id is needed to construct topic strings for publishing
        # commands. It's optional at construction time because legacy callers
        # don't have it; the cloud also sends it in session_start so we can
        # capture it then.
        self.robot_id: Optional[str] = robot_id
        self._peer: Optional[ZmqPeer] = None

        # Caller-set callbacks for inbound data-plane messages. Each callback
        # receives the *envelope* dict (sender, publish_ts_ns, payload).
        self.on_sensor: Optional[Callable[[dict], Awaitable[None]]] = None
        self.on_processed: Optional[Callable[[dict], Awaitable[None]]] = None
        self.on_status: Optional[Callable[[dict], Awaitable[None]]] = None

    @property
    def peer(self) -> Optional[ZmqPeer]:
        return self._peer

    async def send_command(self, command: str, speed: float = 0.5) -> None:
        """Publish a CommandPayload to robot/{id}/command. No-op if not live."""
        if self._peer is None or self.robot_id is None:
            return
        payload = CommandPayload(command=command, speed=speed)
        try:
            await self._peer.publish(
                Topics.command(self.robot_id), payload.model_dump()
            )
        except (RuntimeError, ValidationError):
            pass

    async def events(self, stop: asyncio.Event) -> AsyncIterator[str]:
        """
        Open the WebSocket and yield human-readable status events as the
        handshake progresses. Yields once for each phase: "started" (after
        peer_ready sent), "live" (after session_live received), "ended:..."
        on session_end. Returns when the WebSocket closes or stop is set.
        """
        try:
            async with websockets.connect(self.websocket_url) as ws:
                _log.info(f"user signaling WebSocket connected to {self.websocket_url}")
                async for evt in self._handle_messages(ws, stop):
                    yield evt
        except websockets.exceptions.InvalidURI as exc:
            raise UserClientError(f"invalid WebSocket URL: {exc}") from exc
        except websockets.exceptions.WebSocketException as exc:
            raise UserClientError(f"WebSocket connection failed: {exc}") from exc
        except OSError as exc:
            raise UserClientError(f"could not reach cloud WebSocket: {exc}") from exc
        finally:
            if self._peer is not None:
                await self._peer.close()
                self._peer = None

    async def _handle_messages(self, ws, stop: asyncio.Event) -> AsyncIterator[str]:
        while not stop.is_set():
            recv = asyncio.create_task(ws.recv())
            stop_task = asyncio.create_task(stop.wait())
            try:
                done, pending = await asyncio.wait(
                    [recv, stop_task], return_when=asyncio.FIRST_COMPLETED
                )
            except asyncio.CancelledError:
                # External cancellation (the outer finally block cancelled
                # us). asyncio.wait's natural behaviour leaves recv/stop_task
                # *orphaned* — recv will then complete later with a
                # ConnectionClosedOK that nobody retrieves, producing an
                # "Task exception was never retrieved" warning at shutdown.
                # Drain them explicitly and re-raise.
                for t in (recv, stop_task):
                    t.cancel()
                    try:
                        await t
                    except (asyncio.CancelledError, Exception):
                        pass
                raise
            # Normal path: one of the two tasks finished. Cancel the other
            # AND await it so its result/exception is consumed.
            for t in pending:
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            if stop_task in done:
                return
            try:
                raw = recv.result()
            except websockets.exceptions.ConnectionClosed:
                _log.info("cloud closed the user signaling WebSocket")
                return
            try:
                msg = _signaling_decoder.validate_json(raw)
            except ValidationError as exc:
                _log.warning(f"received malformed signaling message: {exc}")
                continue

            if isinstance(msg, SessionStartMessage):
                # Capture robot_id from the message so send_command works.
                self.robot_id = msg.robot_id
                await self._on_session_start(ws, msg)
                yield "started"
            elif isinstance(msg, SessionLiveMessage):
                await self._on_session_live(msg)
                yield "live"
            elif isinstance(msg, SessionEndMessage):
                yield f"ended:{msg.reason}"
                return
            else:
                _log.debug(f"ignoring inbound signaling message: {msg.type}")

    async def _on_session_start(self, ws, msg: SessionStartMessage) -> None:
        self._peer = ZmqPeer(name=f"user-{self.session_id}")
        endpoint = await self._peer.bind()
        reply = PeerReadyMessage(
            session_id=msg.session_id,
            role="user",
            bind_endpoint=endpoint,
        )
        await ws.send(reply.model_dump_json())
        _log.info(f"user peer bound at {endpoint}; peer_ready sent")

    async def _on_session_live(self, msg: SessionLiveMessage) -> None:
        """
        Connect SUBs to the robot's and player's PUB endpoints.

        Subscription topics (per spec table):
          - robot/{id}/sensor    from Robot  (raw sensor)
          - robot/{id}/processed from Player (Player's classification)
          - robot/{id}/status    from anyone
        """
        if self._peer is None:
            _log.error("session_live received before session_start; refusing")
            return
        rid = msg.robot_id
        sensor_topic = Topics.sensor(rid)
        processed_topic = Topics.processed(rid)
        status_topic = Topics.status(rid)

        robot_endpoint = msg.topology["robot"]
        player_endpoint = msg.topology["player"]

        # Robot publishes sensor + status.
        await self._peer.connect_to_peer(
            robot_endpoint, subscribe_to=[sensor_topic, status_topic]
        )
        # Player publishes processed + status.
        await self._peer.connect_to_peer(
            player_endpoint, subscribe_to=[processed_topic, status_topic]
        )

        # Wire callbacks into the peer's dispatch.
        # NOTE on closure capture: a lambda like `lambda t, e: cb(e)` binds
        # `cb` by *name* — by the time the lambda runs, `cb` may have been
        # reassigned to the most recent value. We use a default-argument
        # trick (`cb=...`) to capture the value at lambda-definition time.
        if self.on_sensor is not None:
            self._peer.on(sensor_topic, lambda t, e, cb=self.on_sensor: cb(e))
        if self.on_processed is not None:
            self._peer.on(processed_topic, lambda t, e, cb=self.on_processed: cb(e))
        if self.on_status is not None:
            self._peer.on(status_topic, lambda t, e, cb=self.on_status: cb(e))

        await self._peer.start()
        _log.info(
            f"user triangle live: robot={robot_endpoint}, player={player_endpoint}"
        )


# Backwards-compatible function used by older tests / cli.py.
# In step 7 we'll switch the CLI to use UserSession directly.
async def open_user_signaling(
    websocket_url: str,
    stop: asyncio.Event,
) -> AsyncIterator[str]:
    """
    Legacy entry point. Drives the handshake and yields phase events as
    human-readable strings. Wraps UserSession.events() — see that class
    for the underlying logic.
    """
    # session_id can be parsed from the URL tail; we don't actually need it
    # for the handshake (the cloud passes it back in messages).
    session_id = websocket_url.rstrip("/").rsplit("/", 1)[-1]
    session = UserSession(websocket_url, session_id)
    async for evt in session.events(stop):
        yield evt
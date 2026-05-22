"""
The robot's network client.
 
Three responsibilities:
  1. Register with the cloud over HTTP on startup.
  2. Maintain a long-lived signaling WebSocket: send heartbeats, receive
     session_start / session_live / session_end.
  3. Manage the data-plane ZmqPeer for the active session — bind on
     session_start, connect SUBs on session_live, tear down on session_end.
 
On WebSocket disconnect the client reconnects with jittered exponential
backoff (1s, 2s, 4s, capped at 30s). Each reconnect retries the full
register + WebSocket flow, since the cloud may have evicted the robot
during the outage.
"""

from __future__ import annotations

import asyncio
import random
from typing import Optional

import httpx
import websockets
from pydantic import TypeAdapter, ValidationError

from common.logging import get_logger
from common.schemas import (
    CommandPayload,
    HeartbeatMessage,
    PeerReadyMessage,
    RobotRegisterRequest,
    RobotRegisterResponse,
    SensorPayload,
    SessionEndMessage,
    SessionLiveMessage,
    SessionStartMessage,
    SignalingMessage,
    StatusPayload,
)
from common.topics import Topics
from common.zmq_peer import ZmqPeer
from robot.sdk import RobotDriver


_signaling_decoder = TypeAdapter(SignalingMessage)


# Reconnect backoff bounds. Random jitter is applied to prevent thundering-
# herd when many robots reconnect simultaneously (e.g. after cloud restart).
RECONNECT_BACKOFF_MIN_S: float = 1.0
RECONNECT_BACKOFF_MAX_S: float = 30.0


class CloudClient:
    """
    Cloud-side client for one robot.

    Usage:
        client = CloudClient(robot_id="robot-1", cloud_url="http://localhost:8000")
        await client.run()   # blocks until cancelled
    """

    def __init__(
        self,
        robot_id: str,
        cloud_url: str,
        metadata: dict | None = None,
    ):
        if not cloud_url.startswith(("http://", "https://")):
            raise ValueError(
                f"cloud_url must start with http:// or https://, got {cloud_url!r}"
            )
        self.robot_id = robot_id
        self.cloud_url = cloud_url.rstrip("/")
        self.metadata = metadata or {}
        self._log = get_logger(f"robot.client.{robot_id}")

        # Reported by the cloud in RobotRegisterResponse and used to space
        # heartbeats. Default is overwritten on successful register.
        self._heartbeat_interval_s: float = 2.0

        # Per-session state: ZMQ peer and the current session_id, if any.
        # Reset on session_end or WebSocket reconnect.
        self._peer: Optional[ZmqPeer] = None
        self._current_session_id: Optional[str] = None
        # Reference to the active WebSocket so handlers can send peer_ready.
        self._ws = None

        # The robot driver (FakeJetBot or equivalent) that incoming commands
        # are dispatched to and sensor data is read from. Attached by the
        # robot process via set_driver(); without one, commands are dropped
        # and sensor publishes are skipped (useful for tests of the signaling
        # layer in isolation).
        self._driver: Optional[RobotDriver] = None

        # Sensor publish cadence. Spec: "every second". Tunable for benchmarks.
        self._sensor_publish_interval_s: float = 1.0
        self._sensor_task: Optional[asyncio.Task] = None

        self._stop = asyncio.Event()

    def set_driver(self, driver: RobotDriver) -> None:
        """
        Attach a driver. Must be called before .run(); the cloud client
        otherwise has no SDK to dispatch commands into.
        """
        self._driver = driver

    @property
    def peer(self) -> Optional[ZmqPeer]:
        """Exposed for tests and the data-flow wiring in step 7."""
        return self._peer

    @property
    def current_session_id(self) -> Optional[str]:
        return self._current_session_id

    # ----- public API --------------------------------------------------------

    async def run(self) -> None:
        """
        Main loop: register → connect WebSocket → heartbeat → reconnect on failure.

        Returns only when stop() is called or the asyncio task is cancelled.
        """
        attempt = 0
        while not self._stop.is_set():
            try:
                ws_url = await self._register()
                attempt = 0  # reset backoff on successful register
                await self._heartbeat_session(ws_url)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                delay = self._backoff(attempt)
                self._log.warning(
                    f"connection to cloud failed ({exc!r}); "
                    f"reconnecting in {delay:.1f}s (attempt #{attempt})"
                )
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    return  # stop() called during backoff
                except asyncio.TimeoutError:
                    pass

    def stop(self) -> None:
        """Signal run() to exit at the next opportunity."""
        self._stop.set()

    # ----- internals ---------------------------------------------------------

    async def _register(self) -> str:
        """
        POST /robots/register; return the WebSocket URL.

        Raises on any non-2xx response. The exception is caught by run()
        which applies backoff and retries.
        """
        url = f"{self.cloud_url}/robots/register"
        req = RobotRegisterRequest(robot_id=self.robot_id, metadata=self.metadata)
        async with httpx.AsyncClient(timeout=5.0) as http:
            response = await http.post(url, json=req.model_dump())
            response.raise_for_status()
            parsed = RobotRegisterResponse.model_validate(response.json())
        self._heartbeat_interval_s = parsed.heartbeat_interval_s
        self._log.info(
            f"registered with cloud; ws_url={parsed.websocket_url}, "
            f"heartbeat_interval={parsed.heartbeat_interval_s}s"
        )
        return parsed.websocket_url

    async def _heartbeat_session(self, ws_url: str) -> None:
        """
        Open the signaling WebSocket and send heartbeats until disconnected.

        Runs two concurrent tasks: a heartbeat sender and a receiver. The
        receiver dispatches inbound session_start / session_live / session_end
        messages from the cloud — these drive the ZmqPeer bind/connect
        lifecycle that establishes the data-plane triangle.
        """
        async with websockets.connect(ws_url) as ws:
            self._ws = ws
            self._log.info(f"signaling WebSocket connected to {ws_url}")
            send_task = asyncio.create_task(
                self._send_heartbeats(ws), name=f"{self.robot_id}-heartbeat-send"
            )
            recv_task = asyncio.create_task(
                self._receive_loop(ws), name=f"{self.robot_id}-heartbeat-recv"
            )
            try:
                done, pending = await asyncio.wait(
                    [send_task, recv_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                for task in done:
                    task.result()
            finally:
                for task in (send_task, recv_task):
                    if not task.done():
                        task.cancel()
                # Tear down any session state held across this connection.
                await self._teardown_session("websocket closed")
                self._ws = None

    async def _send_heartbeats(self, ws) -> None:
        """Send a HeartbeatMessage every heartbeat_interval_s until cancelled."""
        try:
            while not self._stop.is_set():
                msg = HeartbeatMessage()
                await ws.send(msg.model_dump_json())
                await asyncio.sleep(self._heartbeat_interval_s)
        except asyncio.CancelledError:
            raise

    async def _receive_loop(self, ws) -> None:
        """Read inbound messages and dispatch on type."""
        async for raw in ws:
            try:
                msg = _signaling_decoder.validate_json(raw)
            except ValidationError as exc:
                self._log.warning(f"received malformed signaling message: {exc}")
                continue

            if isinstance(msg, SessionStartMessage):
                await self._on_session_start(ws, msg)
            elif isinstance(msg, SessionLiveMessage):
                await self._on_session_live(msg)
            elif isinstance(msg, SessionEndMessage):
                self._log.info(f"session_end received: {msg.reason}")
                await self._teardown_session(msg.reason)
            else:
                self._log.debug(f"ignoring inbound signaling message: {msg.type}")

    async def _on_session_start(self, ws, msg: SessionStartMessage) -> None:
        """Bind ZmqPeer and reply with peer_ready containing our endpoint."""
        if self._peer is not None:
            # Defensive: tear down any prior session before starting a new one.
            await self._teardown_session("new session_start")
        self._current_session_id = msg.session_id
        self._peer = ZmqPeer(name=f"robot-{self.robot_id}")
        endpoint = await self._peer.bind()
        reply = PeerReadyMessage(
            session_id=msg.session_id,
            role="robot",
            bind_endpoint=endpoint,
        )
        await ws.send(reply.model_dump_json())
        self._log.info(
            f"session {msg.session_id}: peer bound at {endpoint}; peer_ready sent"
        )

    async def _on_session_live(self, msg: SessionLiveMessage) -> None:
        """
        Connect SUBs to player and user PUBs.

        Subscription topics (per the spec's topic table):
          - robot/{id}/command from User (we dispatch into the SDK)
          - robot/{id}/status  from anyone (we log; future: react to peer state)

        We do NOT subscribe to robot/{id}/sensor because that's our own
        publication. We do NOT subscribe to robot/{id}/processed because
        the spec says only User consumes it.
        """
        if self._peer is None:
            self._log.error("session_live received before session_start; refusing")
            return

        player_endpoint = msg.topology["player"]
        user_endpoint = msg.topology["user"]
        rid = msg.robot_id

        command_topic = Topics.command(rid)
        status_topic = Topics.status(rid)

        # User publishes commands; subscribe to User's PUB for that topic.
        await self._peer.connect_to_peer(
            user_endpoint, subscribe_to=[command_topic, status_topic]
        )
        # Player publishes processed (not for us) and status; subscribe to status only.
        await self._peer.connect_to_peer(
            player_endpoint, subscribe_to=[status_topic]
        )

        # Register the handler that dispatches commands into the driver.
        self._peer.on(command_topic, self._handle_command)
        self._peer.on(status_topic, self._handle_status)

        await self._peer.start()
        self._log.info(
            f"session {msg.session_id}: triangle live "
            f"(player={player_endpoint}, user={user_endpoint})"
        )

        # Kick off the sensor publish loop. It runs until the peer is torn down.
        self._sensor_task = asyncio.create_task(
            self._sensor_publish_loop(rid),
            name=f"{self.robot_id}-sensor-publish",
        )

    async def _handle_command(self, topic: str, envelope: dict) -> None:
        """
        Dispatch an incoming command to the robot driver.

        The driver is None when CloudClient is used standalone (e.g. tests
        of the registration flow). When the robot process wires a real
        FakeJetBot in via set_driver(), commands flow through to it.
        """
        if self._driver is None:
            self._log.warning(f"received command {envelope!r} but no driver attached")
            return

        try:
            payload = CommandPayload.model_validate(envelope.get("payload", {}))
        except ValidationError as exc:
            self._log.warning(f"malformed command payload, dropping: {exc}")
            return

        speed = payload.speed if payload.speed is not None else 0.5
        cmd = payload.command
        self._log.info(f"executing command: {cmd} speed={speed}")

        # The driver methods are sync (matches jetbot's API); they don't
        # block long enough to need to be offloaded.
        if cmd == "forward":
            self._driver.forward(speed)
        elif cmd == "backward":
            self._driver.backward(speed)
        elif cmd == "left":
            self._driver.left(speed)
        elif cmd == "right":
            self._driver.right(speed)
        elif cmd == "stop":
            self._driver.stop()
        # Literal type on CommandPayload.command makes "else" unreachable.

    async def _handle_status(self, topic: str, envelope: dict) -> None:
        """Log inbound status messages. Future: react to peer health/alerts."""
        try:
            payload = StatusPayload.model_validate(envelope.get("payload", {}))
        except ValidationError:
            return  # silently drop malformed status
        self._log.info(
            f"status from {payload.source}: [{payload.level}] {payload.message}"
        )

    async def _sensor_publish_loop(self, robot_id: str) -> None:
        """
        Read the driver's sensor state and publish it once per second.

        Runs until cancelled (peer teardown). The peer is the cancellation
        boundary — if peer.close() is called, the next publish() will fail
        and the task exits cleanly via the except clause.
        """
        sensor_topic = Topics.sensor(robot_id)
        try:
            while self._peer is not None:
                if self._driver is not None:
                    payload = SensorPayload.model_validate(self._driver.read_sensor())
                    try:
                        await self._peer.publish(sensor_topic, payload.model_dump())
                    except RuntimeError:
                        # Peer was closed between the check and the publish.
                        return
                await asyncio.sleep(self._sensor_publish_interval_s)
        except asyncio.CancelledError:
            pass

    async def _teardown_session(self, reason: str) -> None:
        """Close the ZmqPeer and clear session state. Idempotent."""
        if self._sensor_task is not None and not self._sensor_task.done():
            self._sensor_task.cancel()
            try:
                await self._sensor_task
            except (asyncio.CancelledError, Exception):
                pass
        self._sensor_task = None

        if self._peer is not None:
            self._log.info(f"tearing down session ({reason})")
            await self._peer.close()
            self._peer = None
        self._current_session_id = None

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with jitter, clamped to [MIN, MAX]."""
        raw = RECONNECT_BACKOFF_MIN_S * (2 ** (attempt - 1))
        jittered = raw * (0.5 + random.random())  # ±50%
        return min(jittered, RECONNECT_BACKOFF_MAX_S)
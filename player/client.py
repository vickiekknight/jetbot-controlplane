"""
Player's cloud client.
 
Unlike the Robot, the Player does not register over HTTP — its existence
is created when the cloud spawns it as a subprocess. Opening its
WebSocket at /ws/player/{session_id} is what tells the cloud the Player
is alive and ready.
 
The Player owns its own ZmqPeer for the data plane: it binds a PUB,
subscribes to the robot's sensor topic, classifies state magnitude into
normal/warning/alert, and publishes the result on the processed topic.
"""

from __future__ import annotations

import asyncio
from typing import Optional

import websockets
from pydantic import TypeAdapter, ValidationError

from common.logging import get_logger
from common.schemas import (
    PeerReadyMessage,
    ProcessedPayload,
    SensorPayload,
    SessionEndMessage,
    SessionLiveMessage,
    SessionStartMessage,
    SignalingMessage,
)
from common.topics import Topics
from common.zmq_peer import ZmqPeer


_signaling_decoder = TypeAdapter(SignalingMessage)


# Thresholds for the sensor-state → status classifier.
# State is the robot's |linear velocity|, bounded by FakeJetBot.max_speed = 0.3.
# Two break points partition the range into three bands.
#
# These are class-level rather than per-instance because they're a property of
# the (Player, RobotDriver) pair. A different driver (PyBullet with a different
# max_speed) would want different bounds — we'd derive them from the driver's
# physical parameters in a production system. For the take-home, hardcoding to
# the FakeJetBot range is fine; documenting the calibration is what matters.
CLASSIFIER_WARNING_THRESHOLD = 0.10  # below: "normal"
CLASSIFIER_ALERT_THRESHOLD = 0.20    # at/above: "alert"; in-between: "warning"


def classify_state(state: float) -> str:
    """
    Map a raw |velocity| value to one of {normal, warning, alert}.

    Pure function so it can be unit-tested without spinning up the Player.
    """
    if state >= CLASSIFIER_ALERT_THRESHOLD:
        return "alert"
    if state >= CLASSIFIER_WARNING_THRESHOLD:
        return "warning"
    return "normal"


class PlayerClient:
    """
    The Player's network layer.

    Lifecycle:
      1. Connect WebSocket to /ws/player/{session_id}.
      2. Receive session_start; bind ZmqPeer's PUB; send peer_ready.
      3. Receive session_live; ZmqPeer.connect_to_peer() for robot and user.
      4. Stay running until session_end or WebSocket disconnect.
    """

    def __init__(self, session_id: str, robot_id: str, cloud_url: str):
        self.session_id = session_id
        self.robot_id = robot_id
        self.cloud_url = cloud_url.rstrip("/")
        self._log = get_logger(f"player.client.{session_id}")
        self._peer: Optional[ZmqPeer] = None
        self._stop = asyncio.Event()
        # Cached robot_id captured from session_live for the publish loop;
        # equals self.robot_id by construction, but reads cleaner in handlers.
        self._robot_id_for_publishing: str = robot_id

    @property
    def peer(self) -> Optional[ZmqPeer]:
        return self._peer

    async def run(self) -> None:
        """Main loop. Returns when session_end arrives or stop() is called."""
        ws_url = self._ws_url()
        self._log.info(f"player connecting to {ws_url}")
        try:
            async with websockets.connect(ws_url) as ws:
                self._log.info("player signaling WebSocket connected")
                await self._handle_messages(ws)
        finally:
            if self._peer is not None:
                await self._peer.close()

    def stop(self) -> None:
        self._stop.set()

    # ----- internals --------------------------------------------------------

    def _ws_url(self) -> str:
        base = self.cloud_url
        if base.startswith("https://"):
            return "wss://" + base.removeprefix("https://") + f"/ws/player/{self.session_id}"
        if base.startswith("http://"):
            return "ws://" + base.removeprefix("http://") + f"/ws/player/{self.session_id}"
        return f"ws://{base}/ws/player/{self.session_id}"

    async def _handle_messages(self, ws) -> None:
        """Read and dispatch signaling messages until session ends."""
        while not self._stop.is_set():
            # Race the next message against stop().
            recv = asyncio.create_task(ws.recv())
            stop = asyncio.create_task(self._stop.wait())
            done, pending = await asyncio.wait(
                [recv, stop], return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
            if stop in done:
                return
            try:
                raw = recv.result()
            except websockets.exceptions.ConnectionClosed:
                self._log.info("cloud closed the player signaling WebSocket")
                return
            try:
                msg = _signaling_decoder.validate_json(raw)
            except ValidationError as exc:
                self._log.warning(f"malformed signaling message: {exc}")
                continue

            if isinstance(msg, SessionStartMessage):
                await self._on_session_start(ws, msg)
            elif isinstance(msg, SessionLiveMessage):
                await self._on_session_live(msg)
            elif isinstance(msg, SessionEndMessage):
                self._log.info(f"session_end received: {msg.reason}")
                return
            else:
                self._log.debug(f"ignoring signaling message: {msg.type}")

    async def _on_session_start(self, ws, msg: SessionStartMessage) -> None:
        """Bind ZmqPeer's PUB socket and reply with peer_ready."""
        self._peer = ZmqPeer(name=f"player-{self.session_id}")
        endpoint = await self._peer.bind()
        reply = PeerReadyMessage(
            session_id=self.session_id,
            role="player",
            bind_endpoint=endpoint,
        )
        await ws.send(reply.model_dump_json())
        self._log.info(f"player peer bound at {endpoint}; peer_ready sent")

    async def _on_session_live(self, msg: SessionLiveMessage) -> None:
        """
        Connect SUBs to the robot's and user's PUB endpoints.

        Subscription topics (per spec table):
          - robot/{id}/sensor from Robot (we classify and republish)
          - robot/{id}/status from anyone (we log; useful for debugging)

        We do NOT subscribe to robot/{id}/command — that's User→Robot only.
        We do NOT subscribe to our own publication robot/{id}/processed.
        """
        if self._peer is None:
            self._log.error("session_live received before session_start; refusing")
            return

        robot_endpoint = msg.topology["robot"]
        user_endpoint = msg.topology["user"]
        rid = msg.robot_id

        sensor_topic = Topics.sensor(rid)
        status_topic = Topics.status(rid)

        # Robot publishes sensor + status; subscribe to both from Robot's PUB.
        await self._peer.connect_to_peer(
            robot_endpoint, subscribe_to=[sensor_topic, status_topic]
        )
        # User publishes command (not for us) + status; subscribe to status only.
        await self._peer.connect_to_peer(
            user_endpoint, subscribe_to=[status_topic]
        )

        # Register handlers.
        self._peer.on(sensor_topic, self._handle_sensor)

        await self._peer.start()
        self._robot_id_for_publishing = rid
        self._log.info(
            f"player triangle live: robot={robot_endpoint}, user={user_endpoint}"
        )

    async def _handle_sensor(self, topic: str, envelope: dict) -> None:
        """
        Incoming sensor → classify → republish on processed.

        This is the Player's entire data-plane responsibility for step 7.
        In a production system, this slot would hold an inference workload
        (object detection on camera frames, RL policy inference, etc.) — the
        threshold classifier is a placeholder that demonstrates the
        "sensor in, processed out" pattern.
        """
        try:
            payload = SensorPayload.model_validate(envelope.get("payload", {}))
        except ValidationError as exc:
            self._log.warning(f"malformed sensor payload, dropping: {exc}")
            return

        status = classify_state(payload.state)
        processed = ProcessedPayload(
            state=payload.state,
            status=status,
            source_publish_ts_ns=envelope.get("publish_ts_ns", 0),
        )
        try:
            await self._peer.publish(
                Topics.processed(self._robot_id_for_publishing),
                processed.model_dump(),
            )
        except RuntimeError:
            # Peer closed mid-publish (session ending). Drop quietly.
            return
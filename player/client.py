"""
Player's cloud client.

Unlike the Robot, the Player does NOT register over HTTP — its existence
is created by the cloud spawning it as a subprocess. The Player's
"registration" is implicit: opening its WebSocket at /ws/player/{session_id}
is what tells the cloud "I am alive and ready."

The Player's WebSocket carries three message types:
  - Incoming session_start: cloud asking the Player to bind its ZMQ peer.
  - Outgoing peer_ready: Player reporting its bind endpoint.
  - Incoming session_live: cloud broadcasting the full topology.
  - Incoming session_end: cloud telling the Player to tear down.

The Player owns its own ZmqPeer (the data-plane abstraction from common/).
Step 6 wires the signaling lifecycle; step 7 will add the actual data flow
(subscribing to sensor, publishing processed messages).
"""

from __future__ import annotations

import asyncio
from typing import Optional

import websockets
from pydantic import TypeAdapter, ValidationError

from common.logging import get_logger
from common.schemas import (
    PeerReadyMessage,
    SessionEndMessage,
    SessionLiveMessage,
    SessionStartMessage,
    SignalingMessage,
)
from common.zmq_peer import ZmqPeer


_signaling_decoder = TypeAdapter(SignalingMessage)


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
        Connect SUBs to the robot's and user's PUB endpoints. Subscriptions
        are stubbed in step 6 — we subscribe to "" (everything) so the test
        can verify the connections were established. Step 7 narrows the
        subscriptions to robot/{id}/sensor and robot/{id}/status.
        """
        if self._peer is None:
            self._log.error("session_live received before session_start; refusing")
            return
        robot_endpoint = msg.topology["robot"]
        user_endpoint = msg.topology["user"]
        await self._peer.connect_to_peer(robot_endpoint, subscribe_to=[""])
        await self._peer.connect_to_peer(user_endpoint, subscribe_to=[""])
        await self._peer.start()
        self._log.info(
            f"player triangle live: robot={robot_endpoint}, user={user_endpoint}"
        )
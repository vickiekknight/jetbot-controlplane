"""
Robot's client for talking to the cloud service.

Two responsibilities:
  1. REST register on startup — announce existence, get the WebSocket URL.
  2. Maintain a long-lived WebSocket to the cloud, sending heartbeats
     every HEARTBEAT_INTERVAL_S seconds.

The signaling WebSocket also carries session_start / session_live messages
in step 6 — for now, this client only sends heartbeats and ignores
inbound messages. The receive loop exists so we observe disconnection
promptly (otherwise we'd send forever into a closed socket).

RECONNECTION:
On WebSocket disconnect, the client reconnects with exponential backoff
(1s, 2s, 4s, capped at 30s). Each reconnect retries the full register +
WebSocket flow, since the cloud may have evicted the robot during the
outage. This is what makes the robot self-healing across cloud restarts,
network blips, and transient host issues.
"""

from __future__ import annotations

import asyncio
import random

import httpx
import websockets
from pydantic import ValidationError

from common.logging import get_logger
from common.schemas import (
    HeartbeatMessage,
    RobotRegisterRequest,
    RobotRegisterResponse,
)


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

        self._stop = asyncio.Event()

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
        receiver is mostly a sentinel — it reads inbound messages (today
        none of interest; step 6 will dispatch session_start etc.) and
        ensures we notice a closed socket promptly.
        """
        async with websockets.connect(ws_url) as ws:
            self._log.info(f"signaling WebSocket connected to {ws_url}")
            send_task = asyncio.create_task(
                self._send_heartbeats(ws), name=f"{self.robot_id}-heartbeat-send"
            )
            recv_task = asyncio.create_task(
                self._receive_loop(ws), name=f"{self.robot_id}-heartbeat-recv"
            )
            try:
                # Exit as soon as either task finishes; the other gets cancelled.
                done, pending = await asyncio.wait(
                    [send_task, recv_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                # Surface any exception from the completed task.
                for task in done:
                    task.result()
            finally:
                for task in (send_task, recv_task):
                    if not task.done():
                        task.cancel()

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
        """Read inbound messages. Step 6 will dispatch; today we log and drop."""
        async for raw in ws:
            try:
                # Won't actually do anything useful in step 4 since the cloud
                # doesn't send anything besides heartbeats. Once step 6 lands,
                # this is where session_start / session_live get dispatched.
                self._log.debug(f"received inbound signaling message: {raw!r}")
            except ValidationError as exc:
                self._log.warning(f"received malformed signaling message: {exc}")

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with jitter, clamped to [MIN, MAX]."""
        raw = RECONNECT_BACKOFF_MIN_S * (2 ** (attempt - 1))
        jittered = raw * (0.5 + random.random())  # ±50%
        return min(jittered, RECONNECT_BACKOFF_MAX_S)
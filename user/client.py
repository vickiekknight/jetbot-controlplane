"""
HTTP and WebSocket client logic for the user CLI.

Separated from cli.py so this module can be tested in isolation — pure
async functions that take a cloud URL and parameters and return parsed
results, no terminal output, no Typer dependency.

The CLI layer handles:
  - argument parsing
  - rendering tables / status messages
  - signal handling for SIGINT
  - exit codes

This layer handles:
  - HTTP requests and response parsing
  - WebSocket connection lifecycle
  - error translation from raw exceptions to typed results

In step 6, this module will gain the signaling handlers (peer_ready,
session_live dispatch). Today the connect() function just opens the
WebSocket and yields incoming messages — the CLI prints them and waits.
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator

import httpx
import websockets

from common.logging import get_logger
from common.schemas import (
    RobotInfo,
    RobotListResponse,
    SessionRequest,
    SessionResponse,
)


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
        # The cloud's error response has a `detail` field we want to surface.
        try:
            detail = exc.response.json().get("detail", exc.response.text)
        except Exception:
            detail = exc.response.text
        raise UserClientError(
            f"cloud rejected session request ({exc.response.status_code}): {detail}"
        ) from exc
    except httpx.RequestError as exc:
        raise UserClientError(f"could not reach cloud at {url}: {exc}") from exc


async def open_user_signaling(
    websocket_url: str,
    stop: asyncio.Event,
) -> AsyncIterator[str]:
    """
    Open the user-side signaling WebSocket and yield raw inbound messages
    until `stop` is set or the socket is closed by the cloud.

    Today the cloud doesn't send anything on this channel — the connection
    is just a placeholder demonstrating that the connect command reaches
    the right endpoint. In step 6 this channel carries session_start /
    session_live messages, and yields them for the CLI to display.

    The function is an async generator so the caller can iterate it with
    `async for` and apply its own cancellation policy. The `stop` event is
    a soft-shutdown signal; the function exits cleanly when set.
    """
    try:
        async with websockets.connect(websocket_url) as ws:
            _log.info(f"user signaling WebSocket connected to {websocket_url}")
            # Two concurrent waits: a message from the server, OR a stop signal.
            while not stop.is_set():
                recv_task = asyncio.create_task(ws.recv())
                stop_task = asyncio.create_task(stop.wait())
                done, pending = await asyncio.wait(
                    [recv_task, stop_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                if stop_task in done:
                    break
                # recv_task completed — either a message or the socket closed.
                try:
                    msg = recv_task.result()
                except websockets.exceptions.ConnectionClosed:
                    _log.info("cloud closed the user signaling WebSocket")
                    break
                yield msg
    except websockets.exceptions.InvalidURI as exc:
        raise UserClientError(f"invalid WebSocket URL: {exc}") from exc
    except websockets.exceptions.WebSocketException as exc:
        raise UserClientError(f"WebSocket connection failed: {exc}") from exc
    except OSError as exc:
        # DNS failure, connection refused, etc.
        raise UserClientError(f"could not reach cloud WebSocket: {exc}") from exc
"""
Shared Pydantic message schemas.

This module is the single source of truth for the wire format of every
inter-entity message in the system. It covers three transport layers:

  1. REST API:        cloud_service HTTP endpoints (robot/user → cloud)
  2. WebSocket:       signaling protocol (cloud ↔ entities)
  3. ZeroMQ pub/sub:  the data plane payloads (entities ↔ entities)

Keeping these in one place ensures the contract stays consistent across the
four independently-developed entities (cloud_service, robot, user, player).

Design note: signaling messages share a tagged-union shape via the `type`
discriminator field so a single Pydantic parse call can decode any incoming
WebSocket message.
"""

from __future__ import annotations

import time
from typing import Literal, Optional, Union

from pydantic import BaseModel, Field


# =============================================================================
# REST API: cloud_service endpoints
# =============================================================================

class RobotRegisterRequest(BaseModel):
    """POST /robots/register — robot announces itself on startup."""

    robot_id: str
    metadata: dict = Field(default_factory=dict)


class RobotRegisterResponse(BaseModel):
    """Cloud's reply: tells the robot where to open its signaling WebSocket."""

    robot_id: str
    websocket_url: str
    heartbeat_interval_s: float = 2.0


RobotStatus = Literal["online", "offline", "in_session"]


class RobotInfo(BaseModel):
    """Returned by GET /robots and stored in the cloud's in-memory registry."""

    robot_id: str
    status: RobotStatus
    last_heartbeat_ts: float
    metadata: dict = Field(default_factory=dict)


class RobotListResponse(BaseModel):
    robots: list[RobotInfo]


class SessionRequest(BaseModel):
    """POST /sessions — user asks the cloud to connect them to a robot."""

    robot_id: str
    user_id: str


class SessionResponse(BaseModel):
    """Cloud's reply: tells the user where to open its signaling WebSocket."""

    session_id: str
    robot_id: str
    websocket_url: str


# =============================================================================
# WebSocket signaling protocol: cloud ↔ entities
# =============================================================================

EntityRole = Literal["robot", "user", "player"]


class HeartbeatMessage(BaseModel):
    """Sent periodically by robot (and player) to cloud."""

    type: Literal["heartbeat"] = "heartbeat"
    ts: float = Field(default_factory=time.time)


class SessionStartMessage(BaseModel):
    """
    Cloud → entity: 'session is starting. Bind your PUB socket and reply with
    peer_ready containing your endpoint.'
    """

    type: Literal["session_start"] = "session_start"
    session_id: str
    robot_id: str


class PeerReadyMessage(BaseModel):
    """
    Entity → cloud: 'I have bound my PUB socket at this endpoint.'
    Cloud aggregates these from all three peers before broadcasting session_live.
    """

    type: Literal["peer_ready"] = "peer_ready"
    session_id: str
    role: EntityRole
    bind_endpoint: str


class SessionLiveMessage(BaseModel):
    """
    Cloud → all three peers: 'all peers are bound, here is the full topology.
    Open SUB sockets to the other two endpoints.'
    """

    type: Literal["session_live"] = "session_live"
    session_id: str
    robot_id: str
    topology: dict[EntityRole, str]


class SessionEndMessage(BaseModel):
    """Cloud → all peers: 'session ending, tear down your peer.'"""

    type: Literal["session_end"] = "session_end"
    session_id: str
    reason: str


# Tagged union covering every message sent over the signaling WebSocket.
# Parse with `pydantic.TypeAdapter(SignalingMessage).validate_python(...)`.
SignalingMessage = Union[
    HeartbeatMessage,
    SessionStartMessage,
    PeerReadyMessage,
    SessionLiveMessage,
    SessionEndMessage,
]


# =============================================================================
# Pub/Sub payloads: ZMQ messages over the triangle mesh
# =============================================================================
# These describe the contents of the `payload` field inside the ZMQ envelope
# (see common/zmq_peer.py for the envelope structure).

class SensorPayload(BaseModel):
    """Published by Robot → robot/{id}/sensor."""

    state: float
    pose: Optional[dict] = None
    battery: Optional[float] = None
    temperature: Optional[float] = None


class CommandPayload(BaseModel):
    """Published by User → robot/{id}/command."""

    command: Literal["forward", "backward", "left", "right", "stop"]
    speed: Optional[float] = None


class ProcessedPayload(BaseModel):
    """Published by Player → robot/{id}/processed."""

    state: float
    status: Literal["normal", "warning", "alert"]
    source_publish_ts_ns: int  # original sensor publish timestamp for latency tracing


class StatusPayload(BaseModel):
    """Published by any entity → robot/{id}/status."""

    source: EntityRole
    message: str
    level: Literal["info", "warn", "error"] = "info"

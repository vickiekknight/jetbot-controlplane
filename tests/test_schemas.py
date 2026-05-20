"""Sanity tests for Pydantic schemas."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from common.schemas import (
    CommandPayload,
    PeerReadyMessage,
    RobotInfo,
    SensorPayload,
    SessionLiveMessage,
    SessionStartMessage,
)


def test_command_payload_rejects_invalid_command():
    with pytest.raises(ValidationError):
        CommandPayload(command="dance")  # not in the Literal set


def test_command_payload_accepts_valid_command():
    cp = CommandPayload(command="forward", speed=0.5)
    assert cp.command == "forward"
    assert cp.speed == 0.5


def test_sensor_payload_minimal():
    sp = SensorPayload(state=25.5)
    assert sp.state == 25.5
    assert sp.pose is None
    assert sp.battery is None


def test_robot_info_has_default_metadata():
    ri = RobotInfo(robot_id="r1", status="online", last_heartbeat_ts=0.0)
    assert ri.metadata == {}


def test_session_start_has_discriminator():
    msg = SessionStartMessage(session_id="s1", robot_id="r1")
    assert msg.type == "session_start"


def test_session_live_topology_keys():
    msg = SessionLiveMessage(
        session_id="s1",
        robot_id="r1",
        topology={
            "robot": "tcp://127.0.0.1:5000",
            "user": "tcp://127.0.0.1:5001",
            "player": "tcp://127.0.0.1:5002",
        },
    )
    assert set(msg.topology.keys()) == {"robot", "user", "player"}


def test_peer_ready_role_validated():
    with pytest.raises(ValidationError):
        PeerReadyMessage(
            session_id="s1",
            role="banana",  # invalid role
            bind_endpoint="tcp://127.0.0.1:5000",
        )

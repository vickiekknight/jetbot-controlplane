"""Unit tests for the Registry. No HTTP — pure state-machine testing."""

from __future__ import annotations

import time

from cloud_service.registry import Registry


def test_empty_registry_lists_no_robots():
    r = Registry()
    assert r.list_robots() == []
    assert len(r) == 0
    assert "any-robot" not in r


def test_register_adds_robot_with_online_status():
    r = Registry()
    info = r.register("robot-1", metadata={"location": "lab"})
    assert info.robot_id == "robot-1"
    assert info.status == "online"
    assert info.metadata == {"location": "lab"}
    assert info.last_heartbeat_ts > 0
    assert len(r) == 1
    assert "robot-1" in r


def test_get_returns_robot_or_none():
    r = Registry()
    r.register("robot-1", metadata={})
    assert r.get("robot-1").robot_id == "robot-1"
    assert r.get("nonexistent") is None


def test_list_returns_all_registered_robots():
    r = Registry()
    r.register("robot-1", metadata={})
    r.register("robot-2", metadata={})
    r.register("robot-3", metadata={})
    robots = r.list_robots()
    assert {x.robot_id for x in robots} == {"robot-1", "robot-2", "robot-3"}


def test_duplicate_registration_replaces():
    """The 'replace on duplicate' policy: new registration wins."""
    r = Registry()
    first = r.register("robot-1", metadata={"version": "1.0"})
    time.sleep(0.001)  # ensure distinguishable timestamps
    second = r.register("robot-1", metadata={"version": "2.0"})

    # Only one entry exists
    assert len(r) == 1
    # The stored version is the second registration
    stored = r.get("robot-1")
    assert stored.metadata == {"version": "2.0"}
    assert stored.last_heartbeat_ts >= first.last_heartbeat_ts


def test_remove_drops_robot():
    r = Registry()
    r.register("robot-1", metadata={})
    assert r.remove("robot-1") is True
    assert "robot-1" not in r
    # Subsequent remove returns False — idempotent.
    assert r.remove("robot-1") is False


def test_mark_offline_changes_status_but_keeps_entry():
    r = Registry()
    r.register("robot-1", metadata={})
    assert r.mark_offline("robot-1") is True
    assert r.get("robot-1").status == "offline"
    assert len(r) == 1  # still in registry


def test_mark_offline_for_nonexistent_returns_false():
    r = Registry()
    assert r.mark_offline("nonexistent") is False


def test_touch_heartbeat_updates_timestamp():
    r = Registry()
    r.register("robot-1", metadata={})
    t0 = r.get("robot-1").last_heartbeat_ts
    time.sleep(0.01)
    assert r.touch_heartbeat("robot-1") is True
    assert r.get("robot-1").last_heartbeat_ts > t0


def test_touch_heartbeat_revives_offline_robot():
    """A heartbeat from a previously-offline robot brings it back online."""
    r = Registry()
    r.register("robot-1", metadata={})
    r.mark_offline("robot-1")
    assert r.get("robot-1").status == "offline"
    r.touch_heartbeat("robot-1")
    assert r.get("robot-1").status == "online"


def test_touch_heartbeat_for_nonexistent_returns_false():
    r = Registry()
    assert r.touch_heartbeat("nonexistent") is False
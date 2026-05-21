"""
Unit tests for the SessionManager state machine.

These tests exercise the state transitions directly — no HTTP, no
subprocesses. The goal is to lock down the invariants: which transitions
are legal, what happens on duplicate peer_ready, what end() does.
"""

from __future__ import annotations

import asyncio

import pytest

from cloud_service.session_manager import (
    InvalidTransition,
    Session,
    SessionManager,
    SessionState,
)


# ----- Session: state transitions ---------------------------------------------

def test_session_starts_in_requested():
    s = Session(session_id="sess_1", robot_id="r1", user_id="u1")
    assert s.state == SessionState.REQUESTED
    assert s.endpoints == {}


def test_happy_path_state_progression():
    s = Session(session_id="sess_1", robot_id="r1", user_id="u1")
    s.mark_spawning()
    assert s.state == SessionState.SPAWNING
    s.mark_awaiting_peers()
    assert s.state == SessionState.AWAITING_PEERS

    assert s.record_peer_ready("robot", "ipc:///tmp/r.sock") is False
    assert s.record_peer_ready("user", "ipc:///tmp/u.sock") is False
    assert s.record_peer_ready("player", "ipc:///tmp/p.sock") is True

    s.mark_live()
    assert s.state == SessionState.LIVE
    assert s.is_topology_complete()


def test_cannot_skip_states():
    s = Session(session_id="sess_1", robot_id="r1", user_id="u1")
    # Cannot go REQUESTED → AWAITING_PEERS directly via mark_spawning (wrong op)
    with pytest.raises(InvalidTransition):
        s.mark_live()

    s.mark_spawning()
    with pytest.raises(InvalidTransition):
        s.mark_live()  # need AWAITING_PEERS first

    s.mark_awaiting_peers()
    with pytest.raises(InvalidTransition):
        s.mark_live()  # need all three peers ready


def test_cannot_mark_spawning_twice():
    s = Session(session_id="sess_1", robot_id="r1", user_id="u1")
    s.mark_spawning()
    with pytest.raises(InvalidTransition):
        s.mark_spawning()


def test_duplicate_peer_ready_replaces_endpoint():
    """Defensive: a peer reconnecting with a new endpoint should not crash."""
    s = Session(session_id="sess_1", robot_id="r1", user_id="u1")
    s.mark_spawning()
    s.mark_awaiting_peers()
    s.record_peer_ready("robot", "ipc:///tmp/r1.sock")
    s.record_peer_ready("robot", "ipc:///tmp/r2.sock")  # duplicate
    assert s.endpoints["robot"] == "ipc:///tmp/r2.sock"


def test_record_peer_ready_outside_awaiting_peers_raises():
    s = Session(session_id="sess_1", robot_id="r1", user_id="u1")
    with pytest.raises(InvalidTransition):
        s.record_peer_ready("robot", "ipc:///tmp/r.sock")


def test_end_is_idempotent():
    s = Session(session_id="sess_1", robot_id="r1", user_id="u1")
    s.end("test-1")
    assert s.state == SessionState.ENDED
    assert s.end_reason == "test-1"
    # Second end is a no-op; reason is preserved from the first end.
    s.end("test-2")
    assert s.end_reason == "test-1"


def test_end_from_any_state():
    """end() is always allowed regardless of current state."""
    for prep in [
        lambda x: None,
        lambda x: x.mark_spawning(),
        lambda x: (x.mark_spawning(), x.mark_awaiting_peers()),
        lambda x: (
            x.mark_spawning(),
            x.mark_awaiting_peers(),
            x.record_peer_ready("robot", "a"),
        ),
    ]:
        s = Session(session_id="s", robot_id="r1", user_id="u1")
        prep(s)
        s.end("test")
        assert s.state == SessionState.ENDED


# ----- SessionManager ---------------------------------------------------------

def test_manager_create_assigns_unique_session_ids():
    mgr = SessionManager()
    ids = {mgr.create("r1", "u1").session_id for _ in range(50)}
    assert len(ids) == 50


def test_manager_get_returns_session_or_none():
    mgr = SessionManager()
    session = mgr.create("r1", "u1")
    assert mgr.get(session.session_id) is session
    assert mgr.get("nonexistent") is None


def test_manager_end_signals_waiters():
    """end() unblocks anyone waiting in wait_for_live so they don't hang."""
    mgr = SessionManager()
    session = mgr.create("r1", "u1")

    async def run():
        wait_task = asyncio.create_task(
            mgr.wait_for_live(session.session_id, timeout_s=2.0)
        )
        await asyncio.sleep(0.05)
        mgr.end(session.session_id, "test")
        result = await wait_task
        # Did NOT reach LIVE; result is False.
        return result

    assert asyncio.run(run()) is False


def test_manager_signal_live_unblocks_waiters():
    """signal_live unblocks waiters once the session has been marked LIVE."""
    mgr = SessionManager()
    session = mgr.create("r1", "u1")

    async def run():
        # Drive it to LIVE through the state machine.
        session.mark_spawning()
        session.mark_awaiting_peers()
        session.record_peer_ready("robot", "a")
        session.record_peer_ready("user", "b")
        session.record_peer_ready("player", "c")
        session.mark_live()

        wait_task = asyncio.create_task(
            mgr.wait_for_live(session.session_id, timeout_s=2.0)
        )
        await asyncio.sleep(0.05)
        mgr.signal_live(session.session_id)
        return await wait_task

    assert asyncio.run(run()) is True


def test_manager_wait_for_live_times_out():
    mgr = SessionManager()
    session = mgr.create("r1", "u1")

    async def run():
        return await mgr.wait_for_live(session.session_id, timeout_s=0.1)

    assert asyncio.run(run()) is False


def test_manager_list_sessions():
    mgr = SessionManager()
    mgr.create("r1", "u1")
    mgr.create("r2", "u2")
    mgr.create("r3", "u3")
    assert len(mgr.list_sessions()) == 3
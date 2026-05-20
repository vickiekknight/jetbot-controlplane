"""Tests for ZmqPeer: pub/sub behaviour, topic filtering, triangle mesh, shutdown."""

from __future__ import annotations

import asyncio

import pytest

from common.topics import Topics
from common.zmq_peer import ZmqPeer


# ZMQ SUB needs a brief moment after connect() to complete the TCP handshake
# and register subscription filters with the PUB. In real use this is hidden
# by the signaling handshake; in tests we sleep briefly.
SUB_SETTLE_S = 0.15
DELIVERY_TIMEOUT_S = 1.0


async def _wait_for(predicate, timeout=DELIVERY_TIMEOUT_S, interval=0.02):
    """Spin-wait until predicate() is truthy or timeout. Returns the final value."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        v = predicate()
        if v:
            return v
        await asyncio.sleep(interval)
    return predicate()


@pytest.mark.asyncio
async def test_two_peers_exchange_messages():
    """Peer A publishes, peer B (subscribed) receives."""
    robot_id = "test-robot"
    a = ZmqPeer("A")
    b = ZmqPeer("B")
    try:
        endpoint_a = await a.bind()
        await b.connect_to_peer(endpoint_a, subscribe_to=[Topics.all_for_robot(robot_id)])

        received: list[tuple[str, dict]] = []

        async def handler(topic: str, envelope: dict) -> None:
            received.append((topic, envelope))

        b.on(Topics.all_for_robot(robot_id), handler)
        await b.start()
        await asyncio.sleep(SUB_SETTLE_S)

        await a.publish(Topics.sensor(robot_id), {"state": 25.5})
        await _wait_for(lambda: len(received) >= 1)

        assert len(received) == 1
        topic, env = received[0]
        assert topic == Topics.sensor(robot_id)
        assert env["sender"] == "A"
        assert env["payload"] == {"state": 25.5}
        assert isinstance(env["publish_ts_ns"], int)
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_topic_filtering_isolates_robots():
    """A SUB filtered to robot-1 must not receive robot-2's messages."""
    a = ZmqPeer("A")
    b = ZmqPeer("B")
    try:
        endpoint_a = await a.bind()
        await b.connect_to_peer(endpoint_a, subscribe_to=[Topics.all_for_robot("robot-1")])

        received: list[str] = []

        async def handler(topic: str, envelope: dict) -> None:
            received.append(topic)

        b.on(Topics.all_for_robot("robot-1"), handler)
        await b.start()
        await asyncio.sleep(SUB_SETTLE_S)

        await a.publish(Topics.sensor("robot-1"), {"state": 1.0})
        await a.publish(Topics.sensor("robot-2"), {"state": 2.0})
        await asyncio.sleep(0.3)

        assert received == [Topics.sensor("robot-1")]
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_triangle_mesh():
    """
    Three peers fully connected: A publishes once; B and C both receive.

    This mirrors the final state the cloud's signaling produces — exactly the
    triangle the spec requires.
    """
    robot_id = "rb"
    a, b, c = ZmqPeer("A"), ZmqPeer("B"), ZmqPeer("C")
    try:
        ea = await a.bind()
        eb = await b.bind()
        ec = await c.bind()

        # Each peer subscribes to the other two
        await a.connect_to_peer(eb, [Topics.all_for_robot(robot_id)])
        await a.connect_to_peer(ec, [Topics.all_for_robot(robot_id)])
        await b.connect_to_peer(ea, [Topics.all_for_robot(robot_id)])
        await b.connect_to_peer(ec, [Topics.all_for_robot(robot_id)])
        await c.connect_to_peer(ea, [Topics.all_for_robot(robot_id)])
        await c.connect_to_peer(eb, [Topics.all_for_robot(robot_id)])

        b_received: list[str] = []
        c_received: list[str] = []

        async def b_handler(topic: str, env: dict) -> None:
            b_received.append(env["sender"])

        async def c_handler(topic: str, env: dict) -> None:
            c_received.append(env["sender"])

        b.on(Topics.all_for_robot(robot_id), b_handler)
        c.on(Topics.all_for_robot(robot_id), c_handler)

        await a.start()
        await b.start()
        await c.start()
        await asyncio.sleep(SUB_SETTLE_S * 2)  # six SUB connections to settle

        await a.publish(Topics.sensor(robot_id), {"state": 1.0})
        await _wait_for(lambda: b_received and c_received)

        assert b_received == ["A"]
        assert c_received == ["A"]
    finally:
        await a.close()
        await b.close()
        await c.close()


@pytest.mark.asyncio
async def test_publish_before_bind_raises():
    p = ZmqPeer("P")
    try:
        with pytest.raises(RuntimeError, match="cannot publish before bind"):
            await p.publish("topic", {"x": 1})
    finally:
        await p.close()


@pytest.mark.asyncio
async def test_close_is_safe_without_start():
    """close() must not raise even when start() was never called."""
    p = ZmqPeer("P")
    await p.bind()
    await p.close()  # should not raise


@pytest.mark.asyncio
async def test_ipc_socket_file_is_cleaned_up():
    """For IPC transport, close() must unlink the socket file."""
    from pathlib import Path

    peer = ZmqPeer("cleanup-test", transport="ipc")
    endpoint = await peer.bind()
    # endpoint is "ipc:///tmp/zmq-peer-cleanup-test-XXXX.sock"
    sock_path = Path(endpoint.removeprefix("ipc://"))
    assert sock_path.exists(), "IPC socket file should exist after bind"
    await peer.close()
    assert not sock_path.exists(), "IPC socket file should be unlinked after close"


@pytest.mark.asyncio
async def test_tcp_transport_works_explicitly():
    """Explicit transport='tcp' overrides the IPC default."""
    a = ZmqPeer("A", transport="tcp")
    b = ZmqPeer("B", transport="tcp")
    try:
        endpoint_a = await a.bind()
        assert endpoint_a.startswith("tcp://"), f"expected tcp endpoint, got {endpoint_a}"
        await b.connect_to_peer(endpoint_a, subscribe_to=[""])

        received: list[str] = []

        async def handler(topic: str, env: dict) -> None:
            received.append(topic)

        b.on("", handler)
        await b.start()
        await asyncio.sleep(SUB_SETTLE_S)

        await a.publish("topic", {"x": 1})
        await _wait_for(lambda: len(received) >= 1)
        assert received == ["topic"]
    finally:
        await a.close()
        await b.close()
    """A raising handler should be logged and the receive loop must continue."""
    a = ZmqPeer("A")
    b = ZmqPeer("B")
    try:
        endpoint_a = await a.bind()
        await b.connect_to_peer(endpoint_a, subscribe_to=[""])  # all topics

        good_received: list[str] = []

        async def bad_handler(topic: str, env: dict) -> None:
            raise RuntimeError("intentional")

        async def good_handler(topic: str, env: dict) -> None:
            good_received.append(topic)

        b.on("", bad_handler)
        b.on("", good_handler)
        await b.start()
        await asyncio.sleep(SUB_SETTLE_S)

        await a.publish("t1", {"x": 1})
        await a.publish("t2", {"x": 2})
        await _wait_for(lambda: len(good_received) >= 2)

        assert good_received == ["t1", "t2"]
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_handler_exception_does_not_kill_loop():
    """A raising handler should be logged and the receive loop must continue."""
    a = ZmqPeer("A")
    b = ZmqPeer("B")
    try:
        endpoint_a = await a.bind()
        await b.connect_to_peer(endpoint_a, subscribe_to=[""])  # all topics

        good_received: list[str] = []

        async def bad_handler(topic: str, env: dict) -> None:
            raise RuntimeError("intentional")

        async def good_handler(topic: str, env: dict) -> None:
            good_received.append(topic)

        b.on("", bad_handler)
        b.on("", good_handler)
        await b.start()
        await asyncio.sleep(SUB_SETTLE_S)

        await a.publish("t1", {"x": 1})
        await a.publish("t2", {"x": 2})
        await _wait_for(lambda: len(good_received) >= 2)

        assert good_received == ["t1", "t2"]
    finally:
        await a.close()
        await b.close()
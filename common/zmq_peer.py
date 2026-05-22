"""
ZmqPeer: one peer in the triangle pub/sub mesh.
 
Each entity (robot, user, player) owns exactly one ZmqPeer. The peer
binds a PUB socket, connects N SUB sockets to other peers, and
dispatches received messages to handlers registered by topic prefix.
 
Transport is selected by ZMQ_TRANSPORT env var:
  - "ipc" (default): Unix domain sockets at /tmp/zmq-peer-<name>-<rand>.sock.
    Same-machine only, faster than TCP loopback, matches the spec's
    single-machine scope.
  - "tcp": TCP on the wildcard interface, OS-assigned ephemeral port.
    Required for multi-host deployments.
 
The transport choice is opaque to the rest of the system — peer_ready
messages carry the full endpoint string ("ipc://..." or "tcp://...") and
SUB sockets connect to whatever was advertised.
 
Wire format: each ZMQ message is 2 frames.
  Frame 0:  topic string, e.g. b"robot/robot-1/sensor".
            Subscription filtering is prefix-match on this frame only.
  Frame 1:  JSON envelope: {sender, publish_ts_ns, payload}.
 
publish_ts_ns is recorded at publish time and used by the latency
benchmark to measure end-to-end delivery without inter-host clock sync.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
from pathlib import Path
from typing import Awaitable, Callable, Literal, Optional

import zmq
import zmq.asyncio

from common.logging import get_logger


# Handler signature: (topic, envelope) -> awaitable
MessageHandler = Callable[[str, dict], Awaitable[None]]

Transport = Literal["ipc", "tcp"]

# Default transport read from environment, allowing reviewers to override
# without modifying code. IPC is the default because it works everywhere
# and matches the spec's single-machine demo scenario; TCP is opt-in for
# cross-host deployments.
DEFAULT_TRANSPORT: Transport = os.environ.get("ZMQ_TRANSPORT", "ipc").lower()  # type: ignore[assignment]
if DEFAULT_TRANSPORT not in ("ipc", "tcp"):
    raise ValueError(f"ZMQ_TRANSPORT must be 'ipc' or 'tcp', got {DEFAULT_TRANSPORT!r}")
 
# Directory for IPC sockets. Kept short to avoid OS limits on socket path
# length (108 chars on Linux, 104 on macOS). /tmp is universal and writable.
IPC_DIR = Path("/tmp")

class ZmqPeer:
    """A pub/sub peer with one PUB socket and N SUB sockets."""

    def __init__(self, name: str, transport: Optional[Transport] = None, bind_host: str = "127.0.0.1"):
        """
        Args:
            name:        human-readable identifier, used in logs and as the
                         `sender` field of outgoing message envelopes.
            transport:   "ipc" or "tcp". Defaults to the value of the
                         ZMQ_TRANSPORT environment variable, which defaults
                         to "ipc".
            bind_host:   only used when transport == "tcp". The address peers
                         will see in the advertised endpoint. Default "127.0.0.1"
                         for same-host TCP; use a LAN IP or hostname for
                         multi-host deployments.
        """
        self.name = name
        self.transport: Transport = transport or DEFAULT_TRANSPORT
        self.bind_host = bind_host
        self.bind_endpoint: Optional[str] = None
        self._ipc_path: Optional[Path] = None  # tracked for cleanup at close
 
        self._context = zmq.asyncio.Context()
        self._pub: Optional[zmq.asyncio.Socket] = None
        self._subs: list[zmq.asyncio.Socket] = []
        self._tasks: list[asyncio.Task] = []
        # List of (topic_prefix, handler). Order = invocation order on dispatch.
        self._handlers: list[tuple[str, MessageHandler]] = []
        self._running = False
        self._log = get_logger(f"zmq_peer.{name}")

    # ------------------------------------------------------------------ bind
    async def bind(self) -> str:
        """
        Bind PUB socket on an OS-assigned endpoint.
        Returns the bind endpoint that peers should connect to.
 
        For "ipc" transport: creates a Unix domain socket at
            /tmp/zmq-peer-<name>-<8hexchars>.sock
        The random suffix prevents collisions between concurrent peers with
        the same name (which shouldn't happen, but defensive). The path is
        tracked so close() can unlink the socket file.
 
        For "tcp" transport: binds to the wildcard interface on an
        OS-assigned ephemeral port (POSIX "let the kernel pick" idiom).
        The advertised endpoint uses self.bind_host so peers connect to a
        usable address (the wildcard would expose "0.0.0.0:port", which is
        a bind address, not a connect target).
        """
        if self._pub is not None:
            raise RuntimeError(f"{self.name}: PUB already bound at {self.bind_endpoint}")
 
        sock = self._context.socket(zmq.PUB)
        sock.setsockopt(zmq.SNDHWM, 1000)
 
        if self.transport == "ipc":
            # Suffix with random hex to prevent collisions if two peers share a name
            suffix = secrets.token_hex(4)
            self._ipc_path = IPC_DIR / f"zmq-peer-{self.name}-{suffix}.sock"
            sock.bind(f"ipc://{self._ipc_path}")
            self.bind_endpoint = f"ipc://{self._ipc_path}"
        else:  # tcp
            sock.bind("tcp://*:0")
            # LAST_ENDPOINT returns "tcp://0.0.0.0:PORT" after a wildcard bind;
            # rewrite the host to bind_host so peers have a usable connect target.
            actual = sock.getsockopt(zmq.LAST_ENDPOINT).decode()
            port = actual.rsplit(":", 1)[1]
            self.bind_endpoint = f"tcp://{self.bind_host}:{port}"
 
        self._pub = sock
        self._log.info(f"PUB bound, advertising {self.bind_endpoint}")
        return self.bind_endpoint

    # ------------------------------------------------------------------ connect
    async def connect_to_peer(
        self,
        peer_endpoint: str,
        subscribe_to: list[str],
    ) -> None:
        """
        Open a SUB socket to a peer's PUB endpoint, filtered by topic prefixes.

        Multiple calls accumulate: a triangle peer calls this twice (once for
        each of the other two peers). The same peer can be connected to with
        different filter sets if desired.

        Note: prefix subscription is bytes-prefix; "robot/r1/" matches every
        topic starting with that string.
        """
        sock = self._context.socket(zmq.SUB)
        sock.setsockopt(zmq.RCVHWM, 1000)
        sock.connect(peer_endpoint)
        for topic in subscribe_to:
            sock.setsockopt(zmq.SUBSCRIBE, topic.encode())
        self._subs.append(sock)
        self._log.info(f"SUB connected to {peer_endpoint}, subscribed={subscribe_to}")

    # ------------------------------------------------------------------ handlers
    def on(self, topic_prefix: str, handler: MessageHandler) -> None:
        """
        Register a handler for messages whose topic starts with `topic_prefix`.

        Handlers are async functions of (topic, envelope) -> None. Multiple
        handlers may match the same message; all are invoked in registration
        order. Exceptions raised by a handler are logged but do not stop the
        receive loop or affect other handlers.
        """
        self._handlers.append((topic_prefix, handler))

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        """Spawn the per-SUB receive loops. Idempotent."""
        if self._running:
            return
        self._running = True
        for sock in self._subs:
            task = asyncio.create_task(self._sub_loop(sock), name=f"{self.name}-sub-loop")
            self._tasks.append(task)
        self._log.info(f"started {len(self._subs)} receive loop(s)")

    async def close(self) -> None:
        """
        Shut down cleanly: cancel receive tasks, close sockets, terminate
        the context. Safe to call multiple times and safe to call before
        start() has been invoked.
        """
        self._running = False

        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        for sock in self._subs:
            sock.close(linger=0)
        self._subs.clear()

        if self._pub is not None:
            self._pub.close(linger=0)
            self._pub = None

        # term() blocks if sockets are still open; we've closed them above
        self._context.term()
        
        # Clean up the IPC socket file (ZMQ doesn't unlink it on close).
        # Best-effort: missing_ok handles double-close and crash-before-bind.
        if self._ipc_path is not None:
            try:
                self._ipc_path.unlink(missing_ok=True)
            except OSError as exc:
                self._log.warning(f"could not unlink IPC socket {self._ipc_path}: {exc}")
            self._ipc_path = None
 
        self._log.info("closed")
 

    # ------------------------------------------------------------------ publish
    async def publish(self, topic: str, payload: dict) -> None:
        """
        Publish a message. Wraps the payload in an envelope with sender and
        timestamp before sending.

        Raises RuntimeError if bind() has not been called.
        """
        if self._pub is None:
            raise RuntimeError(f"{self.name}: cannot publish before bind()")
        envelope = {
            "sender": self.name,
            "publish_ts_ns": time.time_ns(),
            "payload": payload,
        }
        await self._pub.send_multipart([
            topic.encode(),
            json.dumps(envelope).encode(),
        ])

    # ------------------------------------------------------------------ receive (private)
    async def _sub_loop(self, sock: zmq.asyncio.Socket) -> None:
        """Receive on one SUB socket forever; dispatch each message to handlers."""
        try:
            while self._running:
                try:
                    frames = await sock.recv_multipart()
                except asyncio.CancelledError:
                    break
                except zmq.error.ZMQError as exc:
                    if exc.errno == zmq.ETERM:
                        break  # context was terminated; expected during shutdown
                    self._log.exception(f"recv error (continuing): {exc}")
                    continue

                if len(frames) < 2:
                    self._log.warning(f"discarding malformed message ({len(frames)} frames)")
                    continue

                try:
                    topic = frames[0].decode()
                    envelope = json.loads(frames[1].decode())
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    self._log.warning(f"discarding undecodable message: {exc}")
                    continue

                await self._dispatch(topic, envelope)
        except asyncio.CancelledError:
            pass

    async def _dispatch(self, topic: str, envelope: dict) -> None:
        for prefix, handler in self._handlers:
            if topic.startswith(prefix):
                try:
                    await handler(topic, envelope)
                except Exception:
                    self._log.exception(f"handler for prefix {prefix!r} raised")

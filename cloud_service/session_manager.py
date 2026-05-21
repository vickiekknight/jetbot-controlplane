"""
Session lifecycle state machine.

A Session is the cloud's record of a "user wants to talk to a robot" request
and its progression through the signaling handshake. Sessions are created
when the user POSTs /sessions and torn down when the triangle dies.

STATE MACHINE
=============

  REQUESTED
     │  cloud has accepted the user's request, allocated a session_id,
     │  and is about to spawn a Player subprocess.
     │
     ▼  on Player subprocess launch
  SPAWNING
     │  Player is starting up. Cloud is waiting for the Player to open
     │  its WebSocket (proving it's alive).
     │
     ▼  on Player WS attach
  AWAITING_PEERS
     │  Cloud has sent session_start to all three peers. Each peer is
     │  binding its ZMQ PUB socket and replying with peer_ready containing
     │  its bind endpoint.
     │
     ▼  on third peer_ready
  LIVE
     │  Cloud has broadcast session_live (full topology) to all peers.
     │  Each peer has connected its SUBs to the other two PUBs.
     │  The data plane is up. The cloud is now out of the data path.
     │
     ▼  on disconnect / timeout / explicit end
  ENDED
        Cloud has sent session_end to surviving peers and killed the
        Player subprocess. Session record is retained briefly for
        debuggability, then evicted.

DESIGN NOTES
============

State transitions are gated by methods on Session (mark_spawning,
record_peer_ready, end) — never by direct attribute mutation. This
prevents bypassing invariants ("you can only enter LIVE after all three
peer_ready arrived"). Invalid transitions raise InvalidTransition rather
than silently drifting.

The SessionManager owns all sessions and is the only place that creates
or ends them. The signaling layer (signaling.py) calls into it; the
HTTP layer (POST /sessions) calls into it; both never touch Session
state directly.
"""

from __future__ import annotations

import asyncio
import secrets
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from common.logging import get_logger
from common.schemas import EntityRole


_log = get_logger("session_manager")


# How long to wait for a peer_ready from each entity. If the timeout fires,
# the session is ended. Generous because Player spawn (subprocess + import
# + bind) can take a couple of seconds on a cold machine.
PEER_READY_TIMEOUT_S: float = 10.0


class SessionState(str, Enum):
    REQUESTED = "requested"
    SPAWNING = "spawning"
    AWAITING_PEERS = "awaiting_peers"
    LIVE = "live"
    ENDED = "ended"


class InvalidTransition(Exception):
    """Raised when a state transition is attempted from an inconsistent state."""


@dataclass
class Session:
    """One row of session state. Owned by the SessionManager."""

    session_id: str
    robot_id: str
    user_id: str
    state: SessionState = SessionState.REQUESTED

    # Populated as peer_ready messages arrive. Keys are entity roles
    # ("robot" / "user" / "player"); values are ZMQ endpoint strings.
    endpoints: dict[EntityRole, str] = field(default_factory=dict)

    # The OS process ID of the Player subprocess, recorded for teardown.
    player_pid: Optional[int] = None

    created_at: float = field(default_factory=time.time)
    ended_at: Optional[float] = None
    end_reason: Optional[str] = None

    # ----- state transitions ------------------------------------------------

    def mark_spawning(self) -> None:
        self._require(SessionState.REQUESTED, "mark_spawning")
        self.state = SessionState.SPAWNING
        _log.info(f"session {self.session_id} → SPAWNING")

    def mark_awaiting_peers(self) -> None:
        # Reachable from SPAWNING (Player attached its WebSocket) or
        # directly from REQUESTED in scenarios where the cloud sends
        # session_start before Player spawn is confirmed (we don't do this
        # today, but defensive).
        if self.state not in (SessionState.SPAWNING, SessionState.REQUESTED):
            raise InvalidTransition(
                f"cannot transition to AWAITING_PEERS from {self.state}"
            )
        self.state = SessionState.AWAITING_PEERS
        _log.info(f"session {self.session_id} → AWAITING_PEERS")

    def record_peer_ready(self, role: EntityRole, endpoint: str) -> bool:
        """
        Record a peer_ready from `role` with `endpoint`.
        Returns True iff this completes the set (all three peers ready).
        Idempotent: a duplicate peer_ready is logged but does not advance state.
        """
        if self.state != SessionState.AWAITING_PEERS:
            raise InvalidTransition(
                f"cannot record peer_ready in {self.state}"
            )
        if role in self.endpoints:
            _log.warning(
                f"session {self.session_id}: duplicate peer_ready for {role!r}; "
                f"replacing endpoint {self.endpoints[role]!r} with {endpoint!r}"
            )
        self.endpoints[role] = endpoint
        _log.info(
            f"session {self.session_id}: peer_ready from {role!r} at {endpoint}"
        )
        return self.is_topology_complete()

    def is_topology_complete(self) -> bool:
        return set(self.endpoints.keys()) == {"robot", "user", "player"}

    def mark_live(self) -> None:
        if self.state != SessionState.AWAITING_PEERS:
            raise InvalidTransition(f"cannot transition to LIVE from {self.state}")
        if not self.is_topology_complete():
            raise InvalidTransition(
                f"cannot mark LIVE; only {len(self.endpoints)}/3 peers ready"
            )
        self.state = SessionState.LIVE
        _log.info(f"session {self.session_id} → LIVE")

    def end(self, reason: str) -> None:
        """Terminal transition. Idempotent — already-ended sessions are no-op."""
        if self.state == SessionState.ENDED:
            return
        self.state = SessionState.ENDED
        self.ended_at = time.time()
        self.end_reason = reason
        _log.info(f"session {self.session_id} → ENDED ({reason})")

    # ----- helpers ----------------------------------------------------------

    def _require(self, expected: SessionState, op: str) -> None:
        if self.state != expected:
            raise InvalidTransition(
                f"cannot {op} from state {self.state} (expected {expected})"
            )


class SessionManager:
    """Cloud's registry of active sessions, plus lifecycle helpers."""

    def __init__(self):
        self._sessions: dict[str, Session] = {}
        # Per-session asyncio.Event signalling LIVE; used by the test
        # harness and by the HTTP path if it ever wants to await readiness.
        self._live_events: dict[str, asyncio.Event] = {}

    def create(self, robot_id: str, user_id: str) -> Session:
        """Allocate a new session ID and record. Returns the Session."""
        session_id = f"sess_{secrets.token_hex(6)}"
        session = Session(session_id=session_id, robot_id=robot_id, user_id=user_id)
        self._sessions[session_id] = session
        self._live_events[session_id] = asyncio.Event()
        return session

    def get(self, session_id: str) -> Optional[Session]:
        return self._sessions.get(session_id)

    def list_sessions(self) -> list[Session]:
        return list(self._sessions.values())

    def end(self, session_id: str, reason: str) -> bool:
        """Idempotently end a session. Returns True if the session existed."""
        session = self._sessions.get(session_id)
        if session is None:
            return False
        session.end(reason)
        # Signal anything that was waiting for LIVE (so it unblocks even
        # though the session never made it).
        event = self._live_events.get(session_id)
        if event is not None:
            event.set()
        return True

    def remove(self, session_id: str) -> bool:
        """Drop a session record entirely. Should only be called after end()."""
        self._live_events.pop(session_id, None)
        return self._sessions.pop(session_id, None) is not None

    async def wait_for_live(
        self,
        session_id: str,
        timeout_s: float = PEER_READY_TIMEOUT_S,
    ) -> bool:
        """
        Wait for this session to reach LIVE state.
        Returns True if it reached LIVE; False on timeout or end-before-live.
        """
        event = self._live_events.get(session_id)
        if event is None:
            return False
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout_s)
        except asyncio.TimeoutError:
            return False
        session = self._sessions.get(session_id)
        return session is not None and session.state == SessionState.LIVE

    def signal_live(self, session_id: str) -> None:
        """Wake up anyone awaiting the LIVE transition for this session."""
        event = self._live_events.get(session_id)
        if event is not None:
            event.set()

    def __contains__(self, session_id: str) -> bool:
        return session_id in self._sessions

    def __len__(self) -> int:
        return len(self._sessions)
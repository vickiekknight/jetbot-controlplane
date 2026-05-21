"""
Robot registry: in-memory tracking of which robots are currently registered
with the cloud service.

This is deliberately kept simple — a thread-safe dict keyed by robot_id.
The complexity of "online vs offline" lifecycle (heartbeat tracking, dead-
robot eviction) is added in step 4 when the WebSocket layer lands. For
now, registration creates an entry; lookup and listing are O(1) and O(N)
respectively.

WHY IN-MEMORY:
For a take-home, in-memory state is the right call. The spec scenario
(one cloud, single host, demo on one machine) doesn't need persistence,
and adding Redis/Postgres would be 80% setup overhead and 20% added
correctness. The Registry is designed so a future swap to a backing
store (Redis, etcd, Postgres) would change only this file — the FastAPI
handlers and signaling code take a Registry-shaped dependency and don't
know it's a dict.

WHY THREAD-SAFE:
FastAPI runs handlers concurrently. Even though we use async handlers
(which run on a single event loop thread), uvicorn's threadpool can
execute sync code on worker threads, and dependency injection can run
sync code. Defensive locking costs near-nothing and removes a class of
"works in tests, breaks under load" failure modes.
"""

from __future__ import annotations

import threading
import time
from typing import Optional

from common.logging import get_logger
from common.schemas import RobotInfo


_log = get_logger("registry")


class DuplicateRegistrationError(Exception):
    """Raised if a strict registration policy rejects a duplicate."""


class Registry:
    """
    Thread-safe robot registry.

    All public methods acquire an internal lock. Returned RobotInfo
    instances are owned by the registry (mutating one in place would
    affect the stored record); for safety, callers should treat them as
    read-only or call model_copy() if they need to modify.
    """

    def __init__(self):
        self._robots: dict[str, RobotInfo] = {}
        self._lock = threading.Lock()

    # ----- mutations ---------------------------------------------------------

    def register(self, robot_id: str, metadata: dict) -> RobotInfo:
        """
        Register or re-register a robot.

        REGISTRATION POLICY: replace-with-warning. If a robot with this ID
        is already registered, its record is overwritten and a warning is
        logged. The alternative (strict rejection) would force every robot
        restart to deregister first, which the spec doesn't define and
        which would deadlock a robot that crashed without clean shutdown.
        Replace is more forgiving and matches how device fleets behave in
        practice — a robot that comes back online owns its identity.

        Returns the newly-stored RobotInfo.
        """
        now = time.time()
        info = RobotInfo(
            robot_id=robot_id,
            status="online",
            last_heartbeat_ts=now,
            metadata=metadata,
        )
        with self._lock:
            existed = robot_id in self._robots
            self._robots[robot_id] = info
        if existed:
            _log.warning(f"robot {robot_id!r} re-registered, replacing prior entry")
        else:
            _log.info(f"robot {robot_id!r} registered")
        return info

    def remove(self, robot_id: str) -> bool:
        """Drop a robot from the registry. Returns True if it was present."""
        with self._lock:
            present = self._robots.pop(robot_id, None) is not None
        if present:
            _log.info(f"robot {robot_id!r} removed from registry")
        return present

    def mark_offline(self, robot_id: str) -> bool:
        """
        Mark a robot offline without removing it from the registry.

        Used (in step 4) when the heartbeat times out — we keep the entry
        visible so operators can see who *was* connected, but its status
        reflects reality. Returns True if the robot existed.
        """
        with self._lock:
            r = self._robots.get(robot_id)
            if r is None:
                return False
            r.status = "offline"
        _log.info(f"robot {robot_id!r} marked offline")
        return True

    def touch_heartbeat(self, robot_id: str) -> bool:
        """
        Update last_heartbeat_ts to now and flip status to online if it was
        offline. Used by the WebSocket heartbeat handler (step 4). Returns
        True if the robot existed.
        """
        with self._lock:
            r = self._robots.get(robot_id)
            if r is None:
                return False
            r.last_heartbeat_ts = time.time()
            if r.status == "offline":
                r.status = "online"
        return True

    # ----- reads -------------------------------------------------------------

    def get(self, robot_id: str) -> Optional[RobotInfo]:
        """Returns the RobotInfo for this robot, or None if not registered."""
        with self._lock:
            return self._robots.get(robot_id)

    def list_robots(self) -> list[RobotInfo]:
        """Returns a snapshot list of all registered robots."""
        with self._lock:
            return list(self._robots.values())

    def __contains__(self, robot_id: str) -> bool:
        with self._lock:
            return robot_id in self._robots

    def __len__(self) -> int:
        with self._lock:
            return len(self._robots)
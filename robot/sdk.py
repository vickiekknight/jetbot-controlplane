"""
Robot SDK: the boundary between the network layer and the simulated
hardware.
 
  - RobotDriver: a Protocol that mirrors NVIDIA's jetbot.Robot public
    API (forward / backward / left / right / stop / set_motors). Any
    implementation of this protocol can plug into the rest of the
    system — FakeJetBot, real JetBot hardware, PyBullet, Isaac Sim.
 
  - FakeJetBot: a pure-Python RobotDriver. Differential-drive kinematics
    (two motors with normalized speeds in [-1, 1]) integrating planar
    pose (x, y, theta) over time using a textbook unicycle model.
 
This module is deliberately I/O-free: no networking, no async, no
logging beyond what the caller wires up. The robot process is
responsible for calling step(dt) periodically and dispatching incoming
commands. Keeping I/O out of the SDK is what makes it trivially unit-
testable and swappable.
 
jetbot reference: github.com/NVIDIA-AI-IOT/jetbot/blob/master/jetbot/robot.py
"""

from __future__ import annotations

import math
import threading
import time
from typing import Protocol, runtime_checkable


# Normalized motor speed range, mirroring jetbot's convention.
MOTOR_MIN = -1.0
MOTOR_MAX = 1.0


def _clamp(value: float, lo: float = MOTOR_MIN, hi: float = MOTOR_MAX) -> float:
    """Clamp value to [lo, hi]. Defensive against out-of-range commands."""
    return max(lo, min(hi, value))


@runtime_checkable
class RobotDriver(Protocol):
    """
    Interface the robot process expects from its underlying driver.

    Implementations must be thread-safe with respect to concurrent calls
    from the command dispatcher (which sets motor values from incoming
    network messages) and the tick loop (which integrates physics).

    Method semantics mirror NVIDIA's jetbot.Robot. Speed values are
    normalized to [-1.0, 1.0]; the convenience methods accept a positive
    speed and apply the appropriate sign internally.
    """

    def forward(self, speed: float = 0.5) -> None: ...
    def backward(self, speed: float = 0.5) -> None: ...
    def left(self, speed: float = 0.5) -> None: ...
    def right(self, speed: float = 0.5) -> None: ...
    def stop(self) -> None: ...
    def set_motors(self, left_speed: float, right_speed: float) -> None: ...
    def step(self, dt: float) -> None: ...
    def read_sensor(self) -> dict: ...


class FakeJetBot:
    """
    A simulated differential-drive robot with the jetbot API.

    Kinematic model: unicycle (the standard reduction of a two-wheeled
    differential-drive vehicle for control purposes).

        linear_velocity   v = (v_left + v_right) / 2
        angular_velocity  ω = (v_right - v_left) / wheel_base

        ẋ = v · cos(θ)
        ẏ = v · sin(θ)
        θ̇ = ω

    where v_left and v_right are the wheel ground speeds, derived from the
    normalized motor commands by scaling with max_speed (the robot's top
    linear speed when both motors are at +1.0).

    THREAD SAFETY:
    Internal state mutations are guarded by a single re-entrant lock.
    Both motor commands (from the network thread) and step() integration
    (from the tick loop) are serialized through it. read_sensor() also
    acquires the lock to ensure a consistent snapshot.
    """
    
    # Maximum dt for a single Euler integration step, in seconds. Matches
    # the design tick rate of 50Hz. Larger dt passed to step() is split
    # into multiple sub-steps internally to keep integration accurate.
    MAX_SUBSTEP_S: float = 0.02

    def __init__(
        self,
        wheel_base: float = 0.1,    # meters between left and right wheels
        max_speed: float = 0.3,     # m/s at motor = 1.0
        initial_pose: tuple[float, float, float] = (0.0, 0.0, 0.0),
    ):
        """
        Args:
            wheel_base:   distance between left and right wheels, meters.
                          Affects how quickly the robot rotates for a given
                          differential between motor speeds.
            max_speed:    top linear speed in m/s when a motor is at 1.0.
                          The default 0.3 m/s ≈ a real jetbot's slow cruise.
            initial_pose: starting (x, y, theta) in (m, m, rad).
        """
        if wheel_base <= 0:
            raise ValueError(f"wheel_base must be positive, got {wheel_base}")
        if max_speed <= 0:
            raise ValueError(f"max_speed must be positive, got {max_speed}")

        self.wheel_base = wheel_base
        self.max_speed = max_speed

        # Motor commands in [-1.0, 1.0]. These are what jetbot.Robot exposes
        # as left_motor.value / right_motor.value.
        self._left_motor: float = 0.0
        self._right_motor: float = 0.0

        # Pose: x (m), y (m), theta (rad). Theta is the heading angle
        # measured CCW from the +x axis, standard robotics convention.
        self._x, self._y, self._theta = initial_pose

        # Tracks the last command that was issued, for the read_sensor
        # output. Useful for the user-facing logs to confirm intent matches
        # actual motor state.
        self._last_command: str = "init"

        # Wall-clock time at startup; sensor readings include uptime so
        # downstream consumers can see freshness without their own clock.
        self._start_time = time.time()

        # All state mutations go through this lock. RLock so step() can
        # call helpers that re-acquire it without deadlocking.
        self._lock = threading.RLock()

    # ----- jetbot API: convenience commands -----------------------------------

    def forward(self, speed: float = 0.5) -> None:
        """Both wheels forward at the given speed. Speed in [0, 1]."""
        s = _clamp(speed, 0.0, MOTOR_MAX)
        with self._lock:
            self._left_motor = s
            self._right_motor = s
            self._last_command = f"forward(speed={s})"

    def backward(self, speed: float = 0.5) -> None:
        """Both wheels backward at the given speed. Speed in [0, 1]."""
        s = _clamp(speed, 0.0, MOTOR_MAX)
        with self._lock:
            self._left_motor = -s
            self._right_motor = -s
            self._last_command = f"backward(speed={s})"

    def left(self, speed: float = 0.5) -> None:
        """
        Turn left in place. Right wheel forward, left wheel backward,
        producing CCW rotation.
        """
        s = _clamp(speed, 0.0, MOTOR_MAX)
        with self._lock:
            self._left_motor = -s
            self._right_motor = s
            self._last_command = f"left(speed={s})"

    def right(self, speed: float = 0.5) -> None:
        """
        Turn right in place. Left wheel forward, right wheel backward,
        producing CW rotation.
        """
        s = _clamp(speed, 0.0, MOTOR_MAX)
        with self._lock:
            self._left_motor = s
            self._right_motor = -s
            self._last_command = f"right(speed={s})"

    def stop(self) -> None:
        """Both wheels to zero. Robot decelerates instantly (no inertia)."""
        with self._lock:
            self._left_motor = 0.0
            self._right_motor = 0.0
            self._last_command = "stop()"

    def set_motors(self, left_speed: float, right_speed: float) -> None:
        """
        Set each wheel independently. The general-purpose primitive that the
        convenience methods above are syntactic sugar for. Both speeds in
        [-1.0, 1.0]; out-of-range values are clamped.
        """
        l = _clamp(left_speed)
        r = _clamp(right_speed)
        with self._lock:
            self._left_motor = l
            self._right_motor = r
            self._last_command = f"set_motors({l}, {r})"

    # ----- jetbot API: state readability -------------------------------------
    # The official jetbot exposes left_motor.value via the `traitlets`
    # library. We use plain properties: same read semantics, no dependency.

    @property
    def left_motor_value(self) -> float:
        with self._lock:
            return self._left_motor

    @property
    def right_motor_value(self) -> float:
        with self._lock:
            return self._right_motor

    # ----- simulation extensions (not in jetbot.Robot) ------------------------
    # These are the methods that make this a *fake* (simulator) rather than
    # just an API stub. A real hardware driver would not need step() at all
    # (the motors integrate physics in the physical world); read_sensor()
    # would be implemented by reading IMU / encoders / battery directly.

    def step(self, dt: float) -> None:
        """
        Advance the simulation by dt seconds.
 
        Internally splits dt into N sub-steps of at most MAX_SUBSTEP_S
        each, then applies Euler integration to each. For the design's
        nominal 50Hz tick rate (dt ≈ 0.02s), this is exactly one
        integration step. For larger dt (caller delayed, event loop
        blocked, etc.), sub-stepping preserves accuracy.
 
        Why this matters: Euler integration assumes velocity and heading
        are constant across the integration interval. For pure forward
        motion that's exactly true. For curved motion (v_left ≠ v_right),
        heading changes throughout dt, and one big Euler step would jump
        the robot along its initial heading rather than along the curved
        path it actually traced. Sub-stepping reduces each step's dt
        until the constant-heading approximation is accurate.
        """
        if dt < 0:
            raise ValueError(f"dt must be non-negative, got {dt}")
        if dt == 0:
            return
 
        # ceil ensures we never under-integrate; max(1, ...) defends against
        # extreme cases where dt is so small that ceil rounds to 0 (shouldn't
        # happen for positive dt with positive MAX_SUBSTEP_S, but defensive).
        n = max(1, math.ceil(dt / self.MAX_SUBSTEP_S))
        sub_dt = dt / n
 
        with self._lock:
            for _ in range(n):
                self._integrate_one_step(sub_dt)
 
    def _integrate_one_step(self, dt: float) -> None:
        """
        Single Euler integration step. Caller must hold self._lock.
 
        Separated from step() so the public method can implement
        sub-stepping while this stays a clean single-shot integrator.
        """
        v = (self._left_motor + self._right_motor) / 2.0 * self.max_speed
        omega = (
            (self._right_motor - self._left_motor) / self.wheel_base
            * self.max_speed
        )
        self._x += v * math.cos(self._theta) * dt
        self._y += v * math.sin(self._theta) * dt
        self._theta = (self._theta + omega * dt) % (2 * math.pi)

    def read_sensor(self) -> dict:
        """
        Return a consistent snapshot of the robot's current state.

        The shape of this dict is what the robot process publishes to
        robot/{id}/sensor. The "state" field is included to match the
        spec's example payload ({"state": 25.5}); we use linear velocity
        magnitude as a representative scalar.
        """
        with self._lock:
            v = (self._left_motor + self._right_motor) / 2.0 * self.max_speed
            return {
                "state": abs(v),                        # spec example: "state"
                "pose": {
                    "x": self._x,
                    "y": self._y,
                    "theta": self._theta,
                },
                "motors": {
                    "left": self._left_motor,
                    "right": self._right_motor,
                },
                "last_command": self._last_command,
                "uptime_s": time.time() - self._start_time,
            }
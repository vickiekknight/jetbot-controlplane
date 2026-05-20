"""
Tests for FakeJetBot.

Coverage strategy:

  - API surface: every public method jetbot.Robot exposes works and changes
    the expected internal state.
  - Kinematics: known-input → known-output for the unicycle model. Pure
    forward motion, pure rotation, no motion at rest, scaling with dt.
  - Edge cases: out-of-range speeds get clamped; invalid construction args
    raise; theta wraps to [0, 2π).
  - Protocol conformance: FakeJetBot satisfies the RobotDriver protocol
    (catches regressions where the protocol drifts from the impl).
  - read_sensor() contract: returned dict has the expected shape and the
    "state" field tracks motor activity.
"""

from __future__ import annotations

import math

import pytest

from robot.sdk import FakeJetBot, RobotDriver


TOL = 1e-9  # numerical tolerance for kinematics assertions


# =============================================================================
# Protocol conformance
# =============================================================================

def test_fake_jetbot_satisfies_robot_driver_protocol():
    """
    A regression guard: if someone adds a method to RobotDriver without
    implementing it on FakeJetBot, this test fails.
    """
    bot = FakeJetBot()
    assert isinstance(bot, RobotDriver)


# =============================================================================
# Construction
# =============================================================================

def test_defaults_start_at_origin_and_zero_motors():
    bot = FakeJetBot()
    s = bot.read_sensor()
    assert s["pose"] == {"x": 0.0, "y": 0.0, "theta": 0.0}
    assert s["motors"] == {"left": 0.0, "right": 0.0}
    assert s["state"] == 0.0
    assert s["last_command"] == "init"


def test_initial_pose_is_respected():
    bot = FakeJetBot(initial_pose=(1.0, 2.0, math.pi / 4))
    s = bot.read_sensor()
    assert s["pose"]["x"] == 1.0
    assert s["pose"]["y"] == 2.0
    assert s["pose"]["theta"] == pytest.approx(math.pi / 4)


def test_invalid_wheel_base_raises():
    with pytest.raises(ValueError, match="wheel_base must be positive"):
        FakeJetBot(wheel_base=0)
    with pytest.raises(ValueError, match="wheel_base must be positive"):
        FakeJetBot(wheel_base=-0.1)


def test_invalid_max_speed_raises():
    with pytest.raises(ValueError, match="max_speed must be positive"):
        FakeJetBot(max_speed=0)


# =============================================================================
# Convenience commands set motor state correctly
# =============================================================================

def test_forward_sets_both_motors_positive():
    bot = FakeJetBot()
    bot.forward(0.7)
    assert bot.left_motor_value == 0.7
    assert bot.right_motor_value == 0.7


def test_backward_sets_both_motors_negative():
    bot = FakeJetBot()
    bot.backward(0.4)
    assert bot.left_motor_value == -0.4
    assert bot.right_motor_value == -0.4


def test_left_spins_ccw_left_negative_right_positive():
    """Left turn = right wheel forward, left wheel backward → CCW."""
    bot = FakeJetBot()
    bot.left(0.5)
    assert bot.left_motor_value == -0.5
    assert bot.right_motor_value == 0.5


def test_right_spins_cw_left_positive_right_negative():
    bot = FakeJetBot()
    bot.right(0.5)
    assert bot.left_motor_value == 0.5
    assert bot.right_motor_value == -0.5


def test_stop_zeros_both_motors():
    bot = FakeJetBot()
    bot.forward(1.0)
    bot.stop()
    assert bot.left_motor_value == 0.0
    assert bot.right_motor_value == 0.0


def test_set_motors_independent_control():
    bot = FakeJetBot()
    bot.set_motors(0.3, -0.7)
    assert bot.left_motor_value == 0.3
    assert bot.right_motor_value == -0.7


# =============================================================================
# Out-of-range commands are clamped (defensive against bad upstream input)
# =============================================================================

def test_forward_speed_above_one_clamps():
    bot = FakeJetBot()
    bot.forward(5.0)
    assert bot.left_motor_value == 1.0
    assert bot.right_motor_value == 1.0


def test_set_motors_clamps_negative_below_minus_one():
    bot = FakeJetBot()
    bot.set_motors(-5.0, -2.0)
    assert bot.left_motor_value == -1.0
    assert bot.right_motor_value == -1.0


def test_forward_with_negative_speed_clamps_to_zero():
    """forward() takes positive speeds; a negative is clamped to 0, not negated."""
    bot = FakeJetBot()
    bot.forward(-0.5)
    assert bot.left_motor_value == 0.0
    assert bot.right_motor_value == 0.0


# =============================================================================
# Kinematics: known input → known output
# =============================================================================

def test_no_motion_when_stopped():
    bot = FakeJetBot()
    bot.stop()
    bot.step(1.0)
    s = bot.read_sensor()
    assert s["pose"] == {"x": 0.0, "y": 0.0, "theta": 0.0}


def test_pure_forward_motion_along_x_axis():
    """
    With theta=0 and both motors at full speed, after 1 second the robot
    should be at x = max_speed, y = 0, theta = 0.
    """
    bot = FakeJetBot(max_speed=0.3)
    bot.set_motors(1.0, 1.0)
    bot.step(1.0)
    s = bot.read_sensor()
    assert s["pose"]["x"] == pytest.approx(0.3, abs=TOL)
    assert s["pose"]["y"] == pytest.approx(0.0, abs=TOL)
    assert s["pose"]["theta"] == pytest.approx(0.0, abs=TOL)


def test_pure_rotation_in_place():
    """
    Equal-and-opposite motor speeds produce pure rotation: x and y stay
    at zero, theta changes by omega * dt.
    """
    bot = FakeJetBot(wheel_base=0.1, max_speed=0.3)
    bot.set_motors(-1.0, 1.0)  # CCW
    bot.step(0.1)
    s = bot.read_sensor()
    # omega = (1 - (-1)) / 0.1 * 0.3 = 6 rad/s
    # delta_theta = 6 * 0.1 = 0.6 rad
    assert s["pose"]["x"] == pytest.approx(0.0, abs=TOL)
    assert s["pose"]["y"] == pytest.approx(0.0, abs=TOL)
    assert s["pose"]["theta"] == pytest.approx(0.6, abs=TOL)


def test_motion_in_direction_of_heading():
    """
    If we set theta=pi/2 (facing +y) and drive forward, motion should be
    along +y, not +x.
    """
    bot = FakeJetBot(max_speed=0.3, initial_pose=(0.0, 0.0, math.pi / 2))
    bot.forward(1.0)
    bot.step(1.0)
    s = bot.read_sensor()
    assert s["pose"]["x"] == pytest.approx(0.0, abs=1e-9)
    assert s["pose"]["y"] == pytest.approx(0.3, abs=TOL)


def test_theta_wraps_to_zero_two_pi():
    """Continuous rotation should keep theta in [0, 2π), not grow unbounded."""
    bot = FakeJetBot(wheel_base=0.1, max_speed=0.3)
    bot.set_motors(-1.0, 1.0)
    for _ in range(100):
        bot.step(0.1)  # 6 rad/s for 0.1s = 0.6 rad per step
    s = bot.read_sensor()
    assert 0 <= s["pose"]["theta"] < 2 * math.pi


def test_step_with_zero_dt_is_noop():
    bot = FakeJetBot()
    bot.forward(1.0)
    bot.step(0.0)
    s = bot.read_sensor()
    assert s["pose"] == {"x": 0.0, "y": 0.0, "theta": 0.0}


def test_step_with_negative_dt_raises():
    bot = FakeJetBot()
    with pytest.raises(ValueError, match="dt must be non-negative"):
        bot.step(-0.1)


# =============================================================================
# Sub-stepping: large dt is internally split for accuracy
# =============================================================================

def test_large_dt_equals_many_small_dt():
    """
    A single step(N * MAX_SUBSTEP) should produce the same pose as N
    successive step(MAX_SUBSTEP) calls — proving sub-stepping correctly
    subdivides the interval rather than just integrating once.

    Uses curved motion (left ≠ right) because pure forward motion is
    independent of sub-stepping; the test would pass trivially with no
    sub-stepping for that input.
    """
    n_steps = 5
    big_dt = FakeJetBot.MAX_SUBSTEP_S * n_steps

    a = FakeJetBot()
    a.set_motors(0.4, 1.0)  # curved (different L/R)
    a.step(big_dt)

    b = FakeJetBot()
    b.set_motors(0.4, 1.0)
    for _ in range(n_steps):
        b.step(FakeJetBot.MAX_SUBSTEP_S)

    pa = a.read_sensor()["pose"]
    pb = b.read_sensor()["pose"]
    assert pa["x"] == pytest.approx(pb["x"], abs=1e-12)
    assert pa["y"] == pytest.approx(pb["y"], abs=1e-12)
    assert pa["theta"] == pytest.approx(pb["theta"], abs=1e-12)


def test_sub_stepping_more_accurate_than_single_euler_step():
    """
    For curved motion, sub-stepping should be closer to the true circular
    trajectory than a hypothetical single Euler step would be.

    Uses motors=(0.5, 1.0) so v > 0 AND omega > 0 — the robot traces an
    arc. Compares both sub-stepped step() and the bypass single-Euler
    integration against the closed-form arc solution for unicycle motion
    with constant v, omega:

        x_new = x + (v/omega) * (sin(theta + omega*dt) - sin(theta))
        y_new = y - (v/omega) * (cos(theta + omega*dt) - cos(theta))
        theta_new = theta + omega*dt

    Sub-stepped Euler is asymptotically exact as substep size → 0;
    a single Euler step undershoots the curvature.
    """
    left, right = 0.5, 1.0
    dt = 0.5
    # Parameters used inside FakeJetBot:
    bot_for_params = FakeJetBot()
    v = (left + right) / 2.0 * bot_for_params.max_speed
    omega = (right - left) / bot_for_params.wheel_base * bot_for_params.max_speed

    # Closed-form (exact) endpoint
    true_x = (v / omega) * (math.sin(omega * dt) - math.sin(0))
    true_y = -(v / omega) * (math.cos(omega * dt) - math.cos(0))
    true_theta = omega * dt

    # Sub-stepped (public step)
    accurate = FakeJetBot()
    accurate.set_motors(left, right)
    accurate.step(dt)
    a = accurate.read_sensor()["pose"]

    # Single Euler step (bypass sub-stepping by calling the private helper)
    naive = FakeJetBot()
    naive.set_motors(left, right)
    with naive._lock:
        naive._integrate_one_step(dt)
    n = naive.read_sensor()["pose"]

    accurate_err = math.hypot(a["x"] - true_x, a["y"] - true_y)
    naive_err = math.hypot(n["x"] - true_x, n["y"] - true_y)

    assert accurate_err < naive_err, (
        f"sub-stepping should be more accurate: "
        f"accurate_err={accurate_err:.5f}, naive_err={naive_err:.5f}"
    )
    # And the sub-stepped result should be reasonably close to truth
    assert accurate_err < 0.01, f"accurate_err={accurate_err} unexpectedly large"

    # Theta advances linearly under constant omega, so both should match
    # the closed-form exactly.
    assert a["theta"] == pytest.approx(true_theta, abs=1e-9)
    assert n["theta"] == pytest.approx(true_theta, abs=1e-9)


def test_sub_stepping_handles_very_large_dt():
    """A pathologically large dt (e.g. event loop hung for 5 seconds) must
    still integrate without error and produce a sensible pose."""
    bot = FakeJetBot()
    bot.forward(1.0)
    bot.step(5.0)  # 250 sub-steps internally
    s = bot.read_sensor()
    # 5 seconds at max_speed=0.3 m/s = 1.5m along +x
    assert s["pose"]["x"] == pytest.approx(1.5, abs=1e-6)
    assert s["pose"]["y"] == pytest.approx(0.0, abs=1e-9)


# =============================================================================
# read_sensor() contract
# =============================================================================

def test_read_sensor_returns_expected_keys():
    bot = FakeJetBot()
    s = bot.read_sensor()
    assert set(s.keys()) >= {"state", "pose", "motors", "last_command", "uptime_s"}
    assert set(s["pose"].keys()) == {"x", "y", "theta"}
    assert set(s["motors"].keys()) == {"left", "right"}


def test_state_is_speed_magnitude():
    """The "state" field is meant to be a single representative scalar."""
    bot = FakeJetBot(max_speed=0.3)
    bot.forward(0.5)
    s = bot.read_sensor()
    # v = (0.5 + 0.5) / 2 * 0.3 = 0.15
    assert s["state"] == pytest.approx(0.15)


def test_state_is_zero_when_pure_rotation():
    """Pure rotation has zero linear velocity, so state = 0."""
    bot = FakeJetBot()
    bot.set_motors(-0.5, 0.5)
    s = bot.read_sensor()
    assert s["state"] == 0.0


def test_last_command_tracks_most_recent():
    bot = FakeJetBot()
    bot.forward(0.5)
    assert "forward" in bot.read_sensor()["last_command"]
    bot.stop()
    assert bot.read_sensor()["last_command"] == "stop()"
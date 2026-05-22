"""Unit tests for the Player's threshold classifier."""

from __future__ import annotations

from player.client import (
    CLASSIFIER_ALERT_THRESHOLD,
    CLASSIFIER_WARNING_THRESHOLD,
    classify_state,
)


def test_zero_state_classifies_normal():
    """A stopped robot should be 'normal'."""
    assert classify_state(0.0) == "normal"


def test_state_below_warning_threshold_classifies_normal():
    assert classify_state(CLASSIFIER_WARNING_THRESHOLD - 0.001) == "normal"


def test_state_at_warning_threshold_classifies_warning():
    """The boundary is inclusive at the lower end of the band."""
    assert classify_state(CLASSIFIER_WARNING_THRESHOLD) == "warning"


def test_state_between_thresholds_classifies_warning():
    midpoint = (CLASSIFIER_WARNING_THRESHOLD + CLASSIFIER_ALERT_THRESHOLD) / 2
    assert classify_state(midpoint) == "warning"


def test_state_at_alert_threshold_classifies_alert():
    assert classify_state(CLASSIFIER_ALERT_THRESHOLD) == "alert"


def test_state_above_alert_threshold_classifies_alert():
    assert classify_state(0.5) == "alert"


def test_fake_jetbot_speed_range_covers_all_three_bands():
    """
    Sanity check: the classifier's bands are calibrated to FakeJetBot's
    range [0, 0.3]. All three statuses should be reachable within it.
    """
    from robot.sdk import FakeJetBot
    bot = FakeJetBot()  # max_speed = 0.3

    bot.stop()
    assert classify_state(bot.read_sensor()["state"]) == "normal"

    bot.forward(0.5)  # v = 0.5 * 0.3 = 0.15 → warning band
    assert classify_state(bot.read_sensor()["state"]) == "warning"

    bot.forward(1.0)  # v = 1.0 * 0.3 = 0.30 → alert band
    assert classify_state(bot.read_sensor()["state"]) == "alert"
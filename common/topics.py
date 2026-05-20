"""
Topic naming helpers.

All topic strings flow through this module to prevent typos from causing
silent communication failures (a misspelled topic just looks like a topic
that nobody publishes to, which is hard to debug).

Format: robot/{robot_id}/{kind}

ZMQ topic filtering is byte-prefix based, so subscribing to "robot/" matches
all topics for all robots, "robot/robot-1/" matches everything for one robot,
and "robot/robot-1/sensor" matches exactly one topic.
"""

from __future__ import annotations


class Topics:
    """Construct topic strings scoped by robot_id."""

    @staticmethod
    def sensor(robot_id: str) -> str:
        return f"robot/{robot_id}/sensor"

    @staticmethod
    def command(robot_id: str) -> str:
        return f"robot/{robot_id}/command"

    @staticmethod
    def processed(robot_id: str) -> str:
        return f"robot/{robot_id}/processed"

    @staticmethod
    def status(robot_id: str) -> str:
        return f"robot/{robot_id}/status"

    @staticmethod
    def all_for_robot(robot_id: str) -> str:
        """Prefix matching every topic for a given robot."""
        return f"robot/{robot_id}/"

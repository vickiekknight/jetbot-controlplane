"""
Player process entry point.

Spawned by the cloud's SessionOrchestrator with three required args:
    --session-id sess_XYZ
    --robot-id   robot-X
    --cloud-url  http://localhost:8000

The Player connects to /ws/player/{session_id}, participates in the
three-phase handshake, and stays running until session_end or SIGTERM.
"""

from __future__ import annotations

import argparse
import asyncio
import signal

from common.logging import configure_logging, get_logger
from player.client import PlayerClient


async def main_async(args: argparse.Namespace) -> None:
    log = get_logger("player.main")
    log.info(
        f"starting player session={args.session_id} robot={args.robot_id} "
        f"cloud_url={args.cloud_url}"
    )

    client = PlayerClient(
        session_id=args.session_id,
        robot_id=args.robot_id,
        cloud_url=args.cloud_url,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, client.stop)
        except NotImplementedError:
            pass

    try:
        await client.run()
    finally:
        log.info("player stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a session-scoped Player")
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--robot-id", required=True)
    parser.add_argument("--cloud-url", required=True)
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()
    configure_logging(args.log_level)
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
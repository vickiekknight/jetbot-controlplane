"""
Robot process entry point.

Usage:
    python -m robot --id robot-1 --cloud-url http://localhost:8000

Wires together a FakeJetBot, the CloudClient (registration + heartbeat),
and a tick loop that integrates simulation physics. The data plane (ZMQ
peer for sensor publishing and command subscription) is added in step 6
once the cloud's session signaling exists.

LIFECYCLE:
On startup, the robot's main task is:
  1. Construct a FakeJetBot instance.
  2. Start the CloudClient (it registers with the cloud and begins heartbeats).
  3. Start the tick loop integrating FakeJetBot physics.
  4. Wait for SIGINT/SIGTERM. On signal, cancel all tasks and exit cleanly.
"""

from __future__ import annotations

import argparse
import asyncio
import signal

from common.logging import configure_logging, get_logger
from robot.client import CloudClient
from robot.sdk import FakeJetBot


# Tick rate for the FakeJetBot integration. 50Hz is fast enough that
# Euler integration with sub-stepping is accurate, slow enough that the
# CPU cost is negligible. Tune via CLI if a benchmark wants different.
DEFAULT_TICK_HZ = 50.0


async def tick_loop(bot: FakeJetBot, hz: float, stop: asyncio.Event) -> None:
    """
    Call bot.step(dt) every 1/hz seconds until stop is set.

    Uses wall-clock time deltas rather than fixed dt so that even if a
    cycle runs late, the integration still reflects real elapsed time.
    FakeJetBot.step() handles large dt internally via sub-stepping.
    """
    log = get_logger("robot.tick")
    period = 1.0 / hz
    last = asyncio.get_event_loop().time()
    while not stop.is_set():
        await asyncio.sleep(period)
        now = asyncio.get_event_loop().time()
        bot.step(now - last)
        last = now
    log.info("tick loop stopped")


async def main_async(args: argparse.Namespace) -> None:
    log = get_logger("robot.main")
    log.info(f"starting robot {args.id!r}; cloud_url={args.cloud_url}")

    bot = FakeJetBot()
    client = CloudClient(
        robot_id=args.id,
        cloud_url=args.cloud_url,
        metadata={"driver": "FakeJetBot"},
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    # Translate SIGINT/SIGTERM into our stop event.
    def _request_stop():
        log.info("shutdown signal received")
        stop.set()
        client.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            # Windows doesn't support signal handlers in asyncio; fine for dev.
            pass

    client_task = asyncio.create_task(client.run(), name="cloud-client")
    tick_task = asyncio.create_task(
        tick_loop(bot, args.tick_hz, stop), name="tick-loop"
    )

    try:
        await stop.wait()
    finally:
        log.info("stopping; cancelling tasks")
        for task in (client_task, tick_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(client_task, tick_task, return_exceptions=True)
        log.info("robot stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a simulated robot")
    parser.add_argument("--id", required=True, help="Unique robot identifier")
    parser.add_argument(
        "--cloud-url",
        default="http://localhost:8000",
        help="Cloud service base URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--tick-hz",
        type=float,
        default=DEFAULT_TICK_HZ,
        help=f"Simulation tick rate in Hz (default: {DEFAULT_TICK_HZ})",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    configure_logging(args.log_level)
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
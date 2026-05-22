"""
Latency benchmark for the data plane.

Measures two end-to-end paths that the take-home's design optimizes for:

  Robot → User (direct):
    sensor published by Robot → received by User
    one ZMQ hop, zero processing

  Robot → Player → User (pipelined):
    sensor → Player classifies → processed → User
    two ZMQ hops, one classification

Reports p50, p95, p99, and max for each path, plus throughput.

Both measurements use timestamps that were ALREADY embedded in the wire
protocol (no benchmark-specific instrumentation):

  - Robot's ZmqPeer.publish() puts publish_ts_ns into the envelope.
  - Player's ProcessedPayload includes source_publish_ts_ns — the
    original sensor's publish_ts_ns, preserved across the classification
    hop.

This lets us measure the same metric on both paths apples-to-apples: the
clock difference between when the Robot published the underlying sensor
event and when the User received the (possibly processed) result.

The benchmark runs in-process — cloud + robot + player(subprocess) + user
all sharing the local host's wall clock so timestamp arithmetic is valid.
This is the same scope the spec describes ("single computer").

Run with:
    python -m benchmarks.latency
    python -m benchmarks.latency --duration 30 --rate 50

Defaults: 15s run at 10Hz sensor publish rate. The sensor publish rate is
turned up from the demo's 1Hz to get a meaningful sample size in a short
benchmark run.
"""

from __future__ import annotations

import argparse
import asyncio
import socket
import statistics
import threading
import time
from typing import Optional

import uvicorn
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from cloud_service.app import create_app
from robot.client import CloudClient
from robot.sdk import FakeJetBot
from user.client import UserSession, request_session


console = Console()


# =============================================================================
# In-process cloud (same pattern as test_session_e2e.py)
# =============================================================================

def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _LiveCloud:
    def __init__(self):
        self.port = _free_port()
        self.app = create_app(public_url=f"http://localhost:{self.port}")
        self._config = uvicorn.Config(
            self.app,
            host="127.0.0.1",
            port=self.port,
            log_level="warning",
            lifespan="on",
            ws="wsproto",
        )
        self._server = uvicorn.Server(self._config)
        self._thread: Optional[threading.Thread] = None

    @property
    def base_url(self) -> str:
        return f"http://localhost:{self.port}"

    def __enter__(self):
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        for _ in range(100):
            if self._server.started:
                return self
            time.sleep(0.02)
        raise TimeoutError("cloud did not start within 2s")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=5.0)


# =============================================================================
# Statistics helpers
# =============================================================================

def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile. Returns 0 for empty input."""
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(pct / 100.0 * (len(s) - 1)))))
    return s[k]


def format_us(us: float) -> str:
    """Format microseconds with appropriate unit."""
    if us < 1000:
        return f"{us:.0f}µs"
    if us < 1_000_000:
        return f"{us / 1000:.2f}ms"
    return f"{us / 1_000_000:.3f}s"


def summarize(name: str, latencies_us: list[float]) -> dict:
    if not latencies_us:
        return {"name": name, "count": 0}
    return {
        "name": name,
        "count": len(latencies_us),
        "p50": percentile(latencies_us, 50),
        "p95": percentile(latencies_us, 95),
        "p99": percentile(latencies_us, 99),
        "max": max(latencies_us),
        "min": min(latencies_us),
        "mean": statistics.mean(latencies_us),
    }


# =============================================================================
# Benchmark driver
# =============================================================================

async def run_benchmark(duration_s: float, sensor_rate_hz: float) -> tuple[list, list]:
    """
    Run a single benchmark session and return (robot_to_user_us, robot_to_player_to_user_us).

    The bot is driven forward so sensor.state > 0; this ensures Player
    classifies as warning/alert and publishes processed messages. Without
    motion, classifier produces no work for the Player to exercise.
    """
    with _LiveCloud() as cloud:
        # 1. Robot in-process so we control its sensor publish rate.
        bot = FakeJetBot()
        robot = CloudClient(robot_id="robot-1", cloud_url=cloud.base_url)
        robot.set_driver(bot)
        # Override the default 1Hz publish rate.
        robot._sensor_publish_interval_s = 1.0 / sensor_rate_hz

        robot_task = asyncio.create_task(robot.run(), name="robot")

        # Wait for the robot to register and its WebSocket to attach.
        for _ in range(100):
            await asyncio.sleep(0.05)
            if (
                "robot-1" in cloud.app.state.registry
                and cloud.app.state.connections.get_robot("robot-1") is not None
            ):
                break

        # 2. Drive the bot forward so sensor.state > 0 → Player has work.
        bot.forward(0.7)

        # 3. Open a session — this triggers Player subprocess spawn.
        session_resp = await request_session(
            cloud.base_url, robot_id="robot-1", user_id="bench"
        )

        # 4. Wire up user session callbacks.
        robot_to_user_us: list[float] = []
        robot_to_player_to_user_us: list[float] = []
        stop = asyncio.Event()

        user_session = UserSession(
            session_resp.websocket_url,
            session_resp.session_id,
            robot_id="robot-1",
        )

        async def on_sensor(env):
            """Robot → User direct path latency."""
            now_ns = time.time_ns()
            pub_ns = env.get("publish_ts_ns", 0)
            if pub_ns > 0:
                latency_us = (now_ns - pub_ns) / 1000.0
                robot_to_user_us.append(latency_us)

        async def on_processed(env):
            """Robot → Player → User pipelined latency."""
            now_ns = time.time_ns()
            payload = env.get("payload", {})
            source_ns = payload.get("source_publish_ts_ns", 0)
            if source_ns > 0:
                latency_us = (now_ns - source_ns) / 1000.0
                robot_to_player_to_user_us.append(latency_us)

        user_session.on_sensor = on_sensor
        user_session.on_processed = on_processed

        # 5. Drive the session and wait for it to go LIVE.
        live_seen = asyncio.Event()

        async def consume():
            async for evt in user_session.events(stop):
                if evt == "live":
                    live_seen.set()
                if evt.startswith("ended:"):
                    break

        user_task = asyncio.create_task(consume(), name="user")
        await asyncio.wait_for(live_seen.wait(), timeout=15.0)

        # Brief settle for ZMQ subscription propagation.
        await asyncio.sleep(0.5)

        console.print(f"[dim]benchmark running for {duration_s:.0f}s "
                      f"at {sensor_rate_hz:.0f}Hz sensor publish rate...[/dim]")

        # Reset measurements collected during warm-up.
        robot_to_user_us.clear()
        robot_to_player_to_user_us.clear()

        await asyncio.sleep(duration_s)

        # 6. Teardown.
        stop.set()
        try:
            await asyncio.wait_for(user_task, timeout=3.0)
        except asyncio.TimeoutError:
            user_task.cancel()
        robot.stop()
        try:
            await asyncio.wait_for(robot_task, timeout=3.0)
        except asyncio.TimeoutError:
            robot_task.cancel()

    return robot_to_user_us, robot_to_player_to_user_us


# =============================================================================
# Reporting
# =============================================================================

def render_results(
    direct: list[float],
    pipelined: list[float],
    duration_s: float,
    sensor_rate_hz: float,
):
    direct_stats = summarize("Robot → User (direct)", direct)
    pipelined_stats = summarize("Robot → Player → User", pipelined)

    table = Table(title="End-to-end latency", show_lines=True)
    table.add_column("Path", style="cyan", no_wrap=True)
    table.add_column("Count", justify="right")
    table.add_column("p50", justify="right")
    table.add_column("p95", justify="right")
    table.add_column("p99", justify="right")
    table.add_column("max", justify="right")
    table.add_column("Effective rate", justify="right")

    for s in (direct_stats, pipelined_stats):
        if s["count"] == 0:
            table.add_row(s["name"], "0", "—", "—", "—", "—", "—")
            continue
        rate = s["count"] / duration_s
        table.add_row(
            s["name"],
            str(s["count"]),
            format_us(s["p50"]),
            format_us(s["p95"]),
            format_us(s["p99"]),
            format_us(s["max"]),
            f"{rate:.1f}/s",
        )

    console.print(table)

    # Interpretation panel.
    if direct_stats["count"] > 0 and pipelined_stats["count"] > 0:
        overhead_p50 = pipelined_stats["p50"] - direct_stats["p50"]
        console.print(Panel(
            f"[bold]Reading the results:[/bold]\n\n"
            f"[cyan]Robot → User (direct)[/cyan] is one ZMQ hop: the cost of "
            f"serialization + IPC + deserialization + Python dispatch.\n\n"
            f"[cyan]Robot → Player → User[/cyan] adds a second hop (Player's "
            f"classifier + republish). The p50 overhead vs the direct path is "
            f"[bold]{format_us(overhead_p50)}[/bold] — the cost of doing "
            f"inference and an extra IPC round-trip.\n\n"
            f"For comparison, a hypothetical broker-routed design "
            f"(Robot → Cloud → User) would add the cloud's round-trip-time "
            f"to every message; in this peer-to-peer design the cloud sees "
            f"none of the data-plane traffic, so its load is independent of "
            f"throughput.",
            border_style="cyan",
            title="Interpretation",
        ))


# =============================================================================
# Entry point
# =============================================================================

async def main_async(args):
    console.print(Panel.fit(
        "[bold cyan]JetBot Control Plane — Latency Benchmark[/bold cyan]\n\n"
        f"Duration:    {args.duration}s\n"
        f"Sensor rate: {args.rate}Hz\n"
        f"Transport:   ZMQ IPC (Unix domain sockets)",
        border_style="cyan",
    ))

    direct, pipelined = await run_benchmark(
        duration_s=args.duration, sensor_rate_hz=args.rate,
    )

    render_results(direct, pipelined, args.duration, args.rate)


def main():
    parser = argparse.ArgumentParser(description="Latency benchmark")
    parser.add_argument(
        "--duration", type=float, default=15.0,
        help="Seconds of measurement after warm-up (default: 15)",
    )
    parser.add_argument(
        "--rate", type=float, default=10.0,
        help="Robot's sensor publish rate in Hz (default: 10)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        console.print("\n[yellow]interrupted[/yellow]")


if __name__ == "__main__":
    main()
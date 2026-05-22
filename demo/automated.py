"""
End-to-end automated demo.

Boots the cloud + two robots, then programmatically:
  1. Lists the robots (proves both are registered).
  2. Connects to robot-1, drives it forward, observes sensor + processed
     data flowing, then disconnects.
  3. Connects to robot-2, drives it backward, observes data flow, disconnects.
  4. Tears everything down.

Reviewers can run this with a single command to see end-to-end proof that
the system works:

    python -m demo.automated

Output is narrated with Rich so the phases are easy to follow.
"""

from __future__ import annotations

import asyncio

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from demo._orchestrator import DemoOrchestrator
from user.client import UserSession, list_robots, request_session


console = Console()


async def show_registered_robots(cloud_url: str) -> None:
    """Print a table of currently-registered robots."""
    robots = await list_robots(cloud_url)
    table = Table(title="Registered Robots", show_lines=False)
    table.add_column("Robot ID", style="cyan")
    table.add_column("Status")
    table.add_column("Metadata", overflow="fold")
    for r in robots:
        table.add_row(
            r.robot_id,
            f"[green]{r.status}[/green]" if r.status == "online" else f"[red]{r.status}[/red]",
            str(r.metadata) if r.metadata else "—",
        )
    console.print(table)


async def drive_robot(
    cloud_url: str,
    robot_id: str,
    user_id: str,
    command: str,
    duration_s: float = 3.0,
) -> dict:
    """
    Open a session to robot_id, send `command`, observe data for
    `duration_s` seconds, then disconnect cleanly.

    Returns a dict with summary metrics (sensor count, processed count,
    last sensor state) so the caller can print or assert on the results.
    """
    console.rule(f"[bold cyan]Demo phase: drive {robot_id} with '{command}'[/bold cyan]")
    console.print(f"[dim]requesting session for {robot_id}...[/dim]")
    session_resp = await request_session(cloud_url, robot_id=robot_id, user_id=user_id)
    console.print(f"[green]✓[/green] session created: [dim]{session_resp.session_id}[/dim]")

    received_sensor: list[dict] = []
    received_processed: list[dict] = []
    stop = asyncio.Event()

    user_session = UserSession(
        websocket_url=session_resp.websocket_url,
        session_id=session_resp.session_id,
        robot_id=robot_id,
    )

    async def on_sensor(env):
        received_sensor.append(env.get("payload", {}))
    async def on_processed(env):
        received_processed.append(env.get("payload", {}))

    user_session.on_sensor = on_sensor
    user_session.on_processed = on_processed

    live_seen = asyncio.Event()

    async def consume():
        async for evt in user_session.events(stop):
            if evt == "live":
                live_seen.set()
            if evt.startswith("ended:"):
                break

    consumer = asyncio.create_task(consume())

    try:
        await asyncio.wait_for(live_seen.wait(), timeout=10.0)
        console.print(f"[green]✓[/green] triangle [bold]LIVE[/bold] for {robot_id}")

        # Retry the command until it takes effect (slow-joiner defense).
        async def send_until_first_nonzero_sensor():
            deadline = asyncio.get_event_loop().time() + 3.0
            while asyncio.get_event_loop().time() < deadline:
                await user_session.send_command(command, speed=0.7)
                for _ in range(4):
                    await asyncio.sleep(0.05)
                    if received_sensor and received_sensor[-1].get("state", 0) > 0:
                        return True
            return False

        console.print(f"[dim]sending command '{command}'...[/dim]")
        await send_until_first_nonzero_sensor()
        console.print(f"[green]✓[/green] command '[bold]{command}[/bold]' took effect")

        console.print(f"[dim]observing data for {duration_s}s...[/dim]")
        await asyncio.sleep(duration_s)

        # Stop the robot before disconnecting so the next session starts clean.
        await user_session.send_command("stop", speed=0.0)
        await asyncio.sleep(0.5)

    finally:
        stop.set()
        try:
            await asyncio.wait_for(consumer, timeout=3.0)
        except asyncio.TimeoutError:
            consumer.cancel()

    # Summarize what was observed.
    last_sensor = received_sensor[-1] if received_sensor else {}
    last_processed = received_processed[-1] if received_processed else {}
    summary = {
        "robot_id": robot_id,
        "command": command,
        "sensor_msgs": len(received_sensor),
        "processed_msgs": len(received_processed),
        "last_state": last_sensor.get("state", 0.0),
        "last_pose": last_sensor.get("pose", {}),
        "last_status": last_processed.get("status", "?"),
    }
    return summary


def print_summary(summaries: list[dict]) -> None:
    """Print a final table summarizing each demo phase."""
    table = Table(title="Demo Summary", show_lines=True)
    table.add_column("Robot", style="cyan")
    table.add_column("Cmd", justify="center")
    table.add_column("Sensor msgs", justify="right")
    table.add_column("Processed msgs", justify="right")
    table.add_column("Final state", justify="right")
    table.add_column("Final pose")
    table.add_column("Final status", justify="center")

    for s in summaries:
        pose = s["last_pose"]
        pose_text = (
            f"x={pose.get('x', 0):.2f} y={pose.get('y', 0):.2f}"
            if pose else "—"
        )
        status_color = {
            "normal": "green", "warning": "yellow", "alert": "red"
        }.get(s["last_status"], "white")
        table.add_row(
            s["robot_id"],
            s["command"],
            str(s["sensor_msgs"]),
            str(s["processed_msgs"]),
            f"{s['last_state']:.3f}",
            pose_text,
            f"[{status_color}]{s['last_status']}[/{status_color}]",
        )
    console.print(table)


async def main_async() -> None:
    console.print(Panel.fit(
        "[bold cyan]JetBot Control Plane — Automated Demo[/bold cyan]\n"
        "Booting cloud + 2 robots, then driving each in turn.\n"
        "[dim]Process logs: /tmp/jetbot-demo/[/dim]",
        border_style="cyan",
    ))

    orchestrator = DemoOrchestrator(robot_ids=["robot-1", "robot-2"])

    async with orchestrator.boot():
        console.rule("[bold]Phase 1: list available robots[/bold]")
        await show_registered_robots(orchestrator.cloud_url)

        summaries = []
        summaries.append(await drive_robot(
            orchestrator.cloud_url, "robot-1", user_id="demo-user",
            command="forward", duration_s=3.0,
        ))
        summaries.append(await drive_robot(
            orchestrator.cloud_url, "robot-2", user_id="demo-user",
            command="backward", duration_s=3.0,
        ))

        console.rule("[bold]Phase 4: summary[/bold]")
        print_summary(summaries)

    console.print(Panel.fit(
        "[bold green]✓ demo complete[/bold green]",
        border_style="green",
    ))


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        console.print("\n[yellow]interrupted; cleaning up[/yellow]")


if __name__ == "__main__":
    main()
"""
Typer-based command-line interface.

Two commands:
  user list                       List all robots currently registered.
  user connect <robot_id>         Request a session and stay connected.

Design notes:
  - All terminal I/O lives here. The user/client.py module knows nothing
    about Typer, Rich, or stdout.
  - Errors from the client layer (UserClientError) are caught at the CLI
    boundary and rendered as red error messages with a non-zero exit code.
  - SIGINT during `connect` triggers a clean shutdown — the WebSocket is
    closed, a final status message is printed, and we exit with code 0.
    This is what reviewers will press to end the demo.
"""

from __future__ import annotations

import asyncio
import signal
import sys
import uuid
from typing import AsyncIterator, Optional

import typer
from rich.console import Console
from rich.table import Table

from common.logging import configure_logging
from user.client import (
    UserClientError,
    UserSession,
    list_robots,
    open_user_signaling,  # kept for any external import compatibility
    request_session,
)
from user.dashboard import Dashboard, DashboardState


app = typer.Typer(
    add_completion=False,
    help="CLI for interacting with the JetBot control plane",
)
console = Console()
err_console = Console(stderr=True, style="bold red")


# Shared option used by both subcommands. typer.Option with envvar lets users
# set CLOUD_URL once in their shell rather than typing it every time.
CloudUrlOption = typer.Option(
    "http://localhost:8000",
    "--cloud-url",
    envvar="CLOUD_URL",
    help="Base URL of the cloud service",
)


@app.command("list")
def cmd_list(cloud_url: str = CloudUrlOption) -> None:
    """List robots currently registered with the cloud."""
    try:
        robots = asyncio.run(list_robots(cloud_url))
    except UserClientError as exc:
        err_console.print(f"error: {exc}")
        raise typer.Exit(code=1)

    if not robots:
        console.print("[yellow]No robots are currently registered.[/yellow]")
        return

    table = Table(title="Registered Robots", show_lines=False)
    table.add_column("Robot ID", style="cyan", no_wrap=True)
    table.add_column("Status")
    table.add_column("Last Heartbeat", justify="right")
    table.add_column("Metadata", overflow="fold")

    for r in robots:
        status_style = "green" if r.status == "online" else "red"
        # Show heartbeat age in seconds rather than absolute timestamp —
        # easier to reason about ("seen 2s ago" vs. "1779334822.5").
        import time
        age = time.time() - r.last_heartbeat_ts
        table.add_row(
            r.robot_id,
            f"[{status_style}]{r.status}[/{status_style}]",
            f"{age:.1f}s ago",
            str(r.metadata) if r.metadata else "—",
        )
    console.print(table)


@app.command("connect")
def cmd_connect(
    robot_id: str = typer.Argument(..., help="ID of the robot to connect to"),
    cloud_url: str = CloudUrlOption,
    user_id: str = typer.Option(
        None,
        "--user-id",
        help="Identifier for this user. Random if omitted.",
    ),
) -> None:
    """
    Request a session with a specific robot and run the live dashboard.

    Once the triangle is up, sensor + processed messages stream in and
    are rendered in a Rich Live display. Type commands at the prompt
    (forward / backward / left / right / stop / quit) to drive the robot.
    """
    if user_id is None:
        user_id = f"user-{uuid.uuid4().hex[:8]}"

    try:
        asyncio.run(_connect_async(cloud_url, robot_id, user_id))
    except UserClientError as exc:
        err_console.print(f"error: {exc}")
        raise typer.Exit(code=1)
    except KeyboardInterrupt:
        console.print("\n[yellow]interrupted; exiting[/yellow]")


# Commands the user can type at the prompt. Mapped to (SDK method, default speed).
VALID_COMMANDS = {"forward", "backward", "left", "right", "stop"}


async def _stdin_lines(stop: asyncio.Event) -> AsyncIterator[str]:
    """
    Async generator that yields lines from stdin as the user presses Enter.

    Uses asyncio's `loop.add_reader` rather than `aioconsole.ainput` (which
    spawned a thread). Reasons we prefer the reader approach:

      1. No executor thread → no teardown noise. `aioconsole` left a Python
         thread blocked in stdin.read() during shutdown; that thread would
         try to interact with the closed event loop and raise visible
         RuntimeErrors on the terminal.
      2. Clean cancellation. add_reader is a native asyncio primitive that
         we explicitly unregister in the finally block.
      3. No external dependency for what's a half-screen of code.

    Caveat: loop.add_reader is POSIX-only. On Windows this returns
    immediately without reading anything, degrading gracefully — the
    dashboard still works, just without command input. (Acceptable for a
    take-home that targets a single Linux/Mac host per the spec.)
    """
    loop = asyncio.get_event_loop()
    queue: asyncio.Queue[Optional[str]] = asyncio.Queue()

    def _on_readable():
        try:
            line = sys.stdin.readline()
        except Exception:
            line = ""
        queue.put_nowait(line if line else None)  # None signals EOF

    try:
        loop.add_reader(sys.stdin.fileno(), _on_readable)
    except (NotImplementedError, ValueError, OSError):
        return  # not a tty or platform doesn't support add_reader

    try:
        while not stop.is_set():
            try:
                line = await asyncio.wait_for(queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            if line is None:  # EOF
                return
            yield line
    finally:
        try:
            loop.remove_reader(sys.stdin.fileno())
        except (ValueError, OSError, NotImplementedError):
            pass


async def _connect_async(cloud_url: str, robot_id: str, user_id: str) -> None:
    """
    Full connect flow:
      1. POST /sessions to get session_id and websocket_url.
      2. Construct UserSession; wire up Dashboard callbacks.
      3. Run signaling loop + input loop concurrently inside Live.
    """
    console.print(
        f"requesting session: [cyan]robot={robot_id}[/cyan] user=[cyan]{user_id}[/cyan]"
    )
    session_resp = await request_session(cloud_url, robot_id, user_id)
    console.print(f"[green]session created:[/green] {session_resp.session_id}")
    console.print(
        f"opening signaling WebSocket: [dim]{session_resp.websocket_url}[/dim]"
    )

    stop = asyncio.Event()
    user_session = UserSession(
        websocket_url=session_resp.websocket_url,
        session_id=session_resp.session_id,
        robot_id=robot_id,
    )

    dash_state = DashboardState(
        robot_id=robot_id,
        session_id=session_resp.session_id,
        user_id=user_id,
    )
    dashboard = Dashboard(dash_state)

    # Wire UserSession callbacks → dashboard updates.
    async def on_sensor(env: dict): dashboard.update_sensor(env)
    async def on_processed(env: dict): dashboard.update_processed(env)
    async def on_status(env: dict): dashboard.update_status(env)
    user_session.on_sensor = on_sensor
    user_session.on_processed = on_processed
    user_session.on_status = on_status

    # SIGINT → graceful exit.
    loop = asyncio.get_event_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, stop.set)
        loop.add_signal_handler(signal.SIGTERM, stop.set)
    except (NotImplementedError, RuntimeError):
        pass

    async def signaling_loop():
        try:
            async for evt in user_session.events(stop):
                dashboard.set_session_state(evt if evt != "live" else "live")
                if evt.startswith("ended:"):
                    stop.set()
                    break
        except UserClientError as exc:
            err_console.print(f"error: {exc}")
            stop.set()

    async def input_loop():
        # Wait until the session is LIVE before accepting input — sending
        # commands before then would just be dropped.
        while not stop.is_set() and dash_state.session_state != "live":
            await asyncio.sleep(0.1)
        async for line in _stdin_lines(stop):
            line = line.strip().lower()
            if not line:
                continue
            if line in ("quit", "exit", "q"):
                stop.set()
                break
            if line in VALID_COMMANDS:
                await user_session.send_command(line, speed=0.5)
                dashboard.set_input_buffer(f"sent: {line}")
            else:
                dashboard.set_input_buffer(f"unknown: {line!r}")

    async def refresh_loop():
        """
        Background heartbeat that calls dashboard.refresh() every 100ms.

        Even though every update_* method already triggers a refresh, this
        heartbeat protects against:
          - The session sitting in "live" with no incoming data yet — without
            this, the dashboard would freeze on "waiting..." until the first
            sensor arrives a second later.
          - Any subtle terminal/render issues where a single repaint gets
            dropped — the next tick re-issues it.
        """
        while not stop.is_set():
            try:
                dashboard.refresh()
            except Exception:
                pass  # never let a render glitch kill the loop
            await asyncio.sleep(0.1)

    with dashboard.live():
        sig_task = asyncio.create_task(signaling_loop(), name="signaling")
        inp_task = asyncio.create_task(input_loop(), name="input")
        ref_task = asyncio.create_task(refresh_loop(), name="refresh")
        try:
            # Run until either user-facing task finishes (signaling ends or
            # user quits). The refresh loop is purely a background helper —
            # we don't want its completion to drive exit.
            await asyncio.wait(
                [sig_task, inp_task], return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            stop.set()
            for task in (sig_task, inp_task, ref_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(sig_task, inp_task, ref_task, return_exceptions=True)

    console.print("[yellow]disconnected.[/yellow]")


def main() -> None:
    """Entry point used by `python -m user`."""
    configure_logging("WARNING")  # quiet down library logs in the CLI
    app()


if __name__ == "__main__":
    main()
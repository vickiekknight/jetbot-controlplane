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
import uuid

import typer
from rich.console import Console
from rich.table import Table

from common.logging import configure_logging
from user.client import (
    UserClientError,
    list_robots,
    open_user_signaling,
    request_session,
)


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
    Request a session with a specific robot and stay connected.

    Today this opens the user's signaling WebSocket and waits. Once the
    triangle handshake is implemented (step 6), it will additionally:
      - Display a status indicator while waiting for the triangle to come up.
      - Start the user's ZMQ peer once `session_live` arrives.
      - Accept interactive command input (forward, left, stop, etc.).
    """
    if user_id is None:
        # Generate a short random user_id so the cloud's logs can distinguish
        # concurrent users without forcing the operator to invent one.
        user_id = f"user-{uuid.uuid4().hex[:8]}"

    try:
        asyncio.run(_connect_async(cloud_url, robot_id, user_id))
    except UserClientError as exc:
        err_console.print(f"error: {exc}")
        raise typer.Exit(code=1)
    except KeyboardInterrupt:
        # asyncio.run propagates KeyboardInterrupt out cleanly; we just
        # ensure the user sees a final message rather than a stack trace.
        console.print("\n[yellow]interrupted; exiting[/yellow]")


async def _connect_async(cloud_url: str, robot_id: str, user_id: str) -> None:
    """
    Inner async flow:
      1. POST /sessions to get a session_id and user-side websocket_url.
      2. Open the WebSocket and stream inbound messages until stopped.
    """
    console.print(
        f"requesting session: [cyan]robot={robot_id}[/cyan] user=[cyan]{user_id}[/cyan]"
    )
    session = await request_session(cloud_url, robot_id, user_id)
    console.print(f"[green]session created:[/green] {session.session_id}")
    console.print(f"opening signaling WebSocket: [dim]{session.websocket_url}[/dim]")

    stop = asyncio.Event()
    loop = asyncio.get_event_loop()

    # Translate SIGINT into our stop event so the WebSocket closes cleanly.
    # On Windows asyncio doesn't support add_signal_handler; we fall back to
    # KeyboardInterrupt propagation.
    try:
        loop.add_signal_handler(signal.SIGINT, stop.set)
        loop.add_signal_handler(signal.SIGTERM, stop.set)
    except (NotImplementedError, RuntimeError):
        pass

    console.print(
        "[green]connected.[/green] "
        "[dim]Triangle setup arrives in step 6. "
        "Press Ctrl+C to disconnect.[/dim]"
    )

    async for msg in open_user_signaling(session.websocket_url, stop):
        console.print(f"[cyan]<- cloud:[/cyan] {msg}")

    console.print("[yellow]disconnected.[/yellow]")


def main() -> None:
    """Entry point used by `python -m user`."""
    configure_logging("WARNING")  # quiet down library logs in the CLI
    app()


if __name__ == "__main__":
    main()
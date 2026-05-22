"""
Interactive demo: boot the system and hand off to a human reviewer.

This script boots the cloud + two robots, then prints instructions for
the reviewer to open another terminal and run the user CLI. The script
stays running until SIGINT, at which point it tears everything down.

Usage:
    python -m demo.interactive

Then, in a second terminal:
    python -m user list  --cloud-url http://127.0.0.1:<port>
    python -m user connect robot-1 --cloud-url http://127.0.0.1:<port>
"""

from __future__ import annotations

import asyncio
import signal

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

from demo._orchestrator import DemoOrchestrator


console = Console()


async def main_async() -> None:
    orchestrator = DemoOrchestrator(robot_ids=["robot-1", "robot-2"])

    async with orchestrator.boot():
        cloud_url = orchestrator.cloud_url

        console.print(Panel.fit(
            f"[bold green]✓ system is ready[/bold green]\n\n"
            f"Cloud:  [cyan]{cloud_url}[/cyan]\n"
            f"Robots: [cyan]robot-1[/cyan], [cyan]robot-2[/cyan]\n\n"
            f"[dim]Process logs: /tmp/jetbot-demo/[/dim]",
            border_style="green",
            title="Multi-Robot Demo",
        ))

        commands = (
            f"# Terminal 2: see what's registered\n"
            f"python -m user list --cloud-url {cloud_url}\n\n"
            f"# Terminal 2: connect to robot-1\n"
            f"python -m user connect robot-1 --cloud-url {cloud_url}\n\n"
            f"# After disconnecting, switch to robot-2\n"
            f"python -m user connect robot-2 --cloud-url {cloud_url}\n\n"
            f"# Inside the connect session, type commands:\n"
            f"#   forward | backward | left | right | stop | quit"
        )
        console.print(Panel(
            Syntax(commands, "bash", theme="monokai", line_numbers=False),
            title="Try this in another terminal",
            border_style="cyan",
        ))
        console.print(
            "[dim]Press Ctrl+C here to tear down the cloud and robots.[/dim]\n"
        )

        # Wait until interrupted.
        stop = asyncio.Event()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except (NotImplementedError, RuntimeError):
                pass
        await stop.wait()


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        console.print("\n[yellow]interrupted; cleaning up[/yellow]")


if __name__ == "__main__":
    main()
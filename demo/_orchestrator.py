"""
Shared orchestration helpers for the demo scripts.

Both automated.py and interactive.py need to:
  1. Spawn a cloud subprocess on an ephemeral or fixed port.
  2. Spawn N robot subprocesses connected to the cloud.
  3. Wait until all robots are registered.
  4. Tear everything down cleanly on exit.

This module factors that into a DemoOrchestrator class with a context-
manager interface, so the demo scripts themselves can focus on what makes
each unique.
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys
import time
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from rich.console import Console


def find_free_port() -> int:
    """Ask the OS for an unused TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class DemoOrchestrator:
    """
    Boots and tears down the cloud + N robots as subprocesses.

    Subprocess stdout/stderr is captured to files so the demo's own
    narration isn't drowned out, but the files are tail-able if the user
    wants to see what's happening under the hood.
    """

    def __init__(
        self,
        port: int | None = None,
        robot_ids: list[str] | None = None,
        log_dir: str = "/tmp/jetbot-demo",
    ):
        self.port = port or find_free_port()
        self.robot_ids = robot_ids or ["robot-1", "robot-2"]
        self.log_dir = log_dir
        self.cloud_proc: Optional[asyncio.subprocess.Process] = None
        self.robot_procs: dict[str, asyncio.subprocess.Process] = {}
        self.console = Console()

        os.makedirs(self.log_dir, exist_ok=True)

    @property
    def cloud_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @asynccontextmanager
    async def boot(self):
        """
        Async context manager that boots cloud + robots on entry and tears
        them down on exit. Use with:
            async with orchestrator.boot():
                ...
        """
        try:
            await self._start_cloud()
            await self._wait_until_cloud_ready()
            await self._start_robots()
            await self._wait_until_robots_registered()
            yield
        finally:
            await self._teardown()

    # ----- private --------------------------------------------------------------

    async def _start_cloud(self) -> None:
        self.console.print(
            f"[cyan]›[/cyan] booting cloud on port {self.port} "
            f"([dim]log: {self.log_dir}/cloud.log[/dim])"
        )
        log_path = os.path.join(self.log_dir, "cloud.log")
        log_file = open(log_path, "w")
        self.cloud_proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "cloud_service", "--port", str(self.port),
            stdout=log_file, stderr=asyncio.subprocess.STDOUT,
            env={**os.environ},
        )

    async def _wait_until_cloud_ready(self, timeout_s: float = 10.0) -> None:
        """Poll GET /robots until it returns 200 — proves the cloud is up."""
        deadline = time.time() + timeout_s
        async with httpx.AsyncClient(timeout=1.0) as http:
            while time.time() < deadline:
                try:
                    r = await http.get(f"{self.cloud_url}/robots")
                    if r.status_code == 200:
                        return
                except (httpx.RequestError, httpx.HTTPError):
                    pass
                await asyncio.sleep(0.1)
        raise TimeoutError(f"cloud did not come up within {timeout_s}s")

    async def _start_robots(self) -> None:
        for rid in self.robot_ids:
            self.console.print(
                f"[cyan]›[/cyan] booting robot [bold]{rid}[/bold] "
                f"([dim]log: {self.log_dir}/{rid}.log[/dim])"
            )
            log_path = os.path.join(self.log_dir, f"{rid}.log")
            log_file = open(log_path, "w")
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "robot",
                "--id", rid,
                "--cloud-url", self.cloud_url,
                stdout=log_file, stderr=asyncio.subprocess.STDOUT,
                env={**os.environ},
            )
            self.robot_procs[rid] = proc

    async def _wait_until_robots_registered(self, timeout_s: float = 10.0) -> None:
        """Poll GET /robots until all robot_ids appear."""
        deadline = time.time() + timeout_s
        wanted = set(self.robot_ids)
        async with httpx.AsyncClient(timeout=1.0) as http:
            while time.time() < deadline:
                try:
                    r = await http.get(f"{self.cloud_url}/robots")
                    if r.status_code == 200:
                        seen = {robot["robot_id"] for robot in r.json()["robots"]}
                        if wanted.issubset(seen):
                            self.console.print(
                                f"[green]✓[/green] all {len(wanted)} robots registered"
                            )
                            return
                except (httpx.RequestError, httpx.HTTPError):
                    pass
                await asyncio.sleep(0.1)
        raise TimeoutError(f"robots did not register within {timeout_s}s")

    async def _teardown(self) -> None:
        self.console.print("[cyan]›[/cyan] tearing down...")
        # Stop robots first so they have a chance to deregister cleanly.
        for rid, proc in list(self.robot_procs.items()):
            if proc.returncode is None:
                proc.terminate()
        await asyncio.gather(
            *(self._wait_subprocess(p) for p in self.robot_procs.values()),
            return_exceptions=True,
        )
        if self.cloud_proc is not None and self.cloud_proc.returncode is None:
            self.cloud_proc.terminate()
            await self._wait_subprocess(self.cloud_proc)
        self.console.print("[green]✓[/green] all processes stopped")

    async def _wait_subprocess(self, proc: asyncio.subprocess.Process) -> None:
        try:
            await asyncio.wait_for(proc.wait(), timeout=3.0)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
"""
Self-contained diagnostic for the subprocess data-flow problem.

Run with:
    python diagnose_mac.py

It will:
  1. Spawn a cloud subprocess on an ephemeral port.
  2. Spawn a robot subprocess.
  3. Connect a UserSession in-process.
  4. Send a `forward` command and watch for sensor messages.
  5. Print every event with timestamps to stderr.

If sensor messages flow, the data layer is fine and the demo just has a UI
quirk. If they don't flow, the diagnostic prints what was attempted so we
can narrow down whether it's a binding, subscription, or IPC connectivity
issue.

Cloud/robot stdout is captured to /tmp/diag-*.log so we can grep their
internal state too.
"""
from __future__ import annotations

import asyncio
import os
import socket
import sys
import time
import httpx

from user.client import UserSession, request_session


def log(msg: str) -> None:
    """Timestamped stderr print so it stays visible regardless of stdout."""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def main():
    port = free_port()
    cloud_url = f"http://127.0.0.1:{port}"

    log(f"environment: ZMQ_TRANSPORT={os.environ.get('ZMQ_TRANSPORT', '(unset, defaults to ipc)')}")
    log(f"cwd: {os.getcwd()}")
    log(f"python: {sys.executable}")

    cloud_log = open("/tmp/diag-cloud.log", "w")
    robot_log = open("/tmp/diag-robot.log", "w")

    log(f"spawning cloud on port {port} (output → /tmp/diag-cloud.log)")
    cloud_proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "cloud_service", "--port", str(port),
        "--log-level", "INFO",
        stdout=cloud_log, stderr=asyncio.subprocess.STDOUT,
        env={**os.environ},
    )

    # Wait for cloud to be ready.
    for _ in range(50):
        try:
            async with httpx.AsyncClient(timeout=1.0) as http:
                r = await http.get(f"{cloud_url}/robots")
                if r.status_code == 200:
                    log("cloud is responding")
                    break
        except Exception:
            pass
        await asyncio.sleep(0.1)
    else:
        log("FAIL: cloud never came up")
        return

    log("spawning robot subprocess (output → /tmp/diag-robot.log)")
    robot_proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "robot",
        "--id", "robot-1",
        "--cloud-url", cloud_url,
        "--log-level", "INFO",
        stdout=robot_log, stderr=asyncio.subprocess.STDOUT,
        env={**os.environ},
    )

    # Wait for robot to register.
    for _ in range(100):
        try:
            async with httpx.AsyncClient(timeout=1.0) as http:
                r = await http.get(f"{cloud_url}/robots")
                robots = r.json().get("robots", [])
                if any(rob["robot_id"] == "robot-1" and rob["status"] == "online" for rob in robots):
                    log(f"robot-1 registered as online")
                    break
        except Exception:
            pass
        await asyncio.sleep(0.1)
    else:
        log("FAIL: robot never registered")

    try:
        log("requesting session...")
        session_resp = await request_session(cloud_url, robot_id="robot-1", user_id="diag")
        log(f"session_id={session_resp.session_id}")

        stop = asyncio.Event()
        session = UserSession(
            websocket_url=session_resp.websocket_url,
            session_id=session_resp.session_id,
            robot_id="robot-1",
        )

        sensor_count = 0
        processed_count = 0

        async def on_sensor(env):
            nonlocal sensor_count
            sensor_count += 1
            log(f"SENSOR #{sensor_count}: state={env.get('payload', {}).get('state'):.3f}")

        async def on_processed(env):
            nonlocal processed_count
            processed_count += 1
            log(f"PROCESSED #{processed_count}: status={env.get('payload', {}).get('status')}")

        async def on_status(env):
            log(f"STATUS: {env.get('payload', {})}")

        session.on_sensor = on_sensor
        session.on_processed = on_processed
        session.on_status = on_status

        async def driver():
            # Wait until session is live before doing anything.
            while not stop.is_set():
                if session.peer is not None and len(session.peer._subs) >= 2:
                    log(f"data plane up: peer.bind_endpoint={session.peer.bind_endpoint}")
                    log(f"  SUB sockets: {len(session.peer._subs)}")
                    break
                await asyncio.sleep(0.05)

            await asyncio.sleep(1.0)
            log("sending 'forward' command")
            await session.send_command("forward", speed=0.5)
            log("waiting 5 seconds for sensor messages to arrive...")
            await asyncio.sleep(5.0)
            log(f"final tally: {sensor_count} sensor, {processed_count} processed")
            stop.set()

        driver_task = asyncio.create_task(driver())

        async for evt in session.events(stop):
            log(f"EVENT: {evt}")
            if evt.startswith("ended:"):
                break

        await asyncio.wait_for(driver_task, timeout=2.0)

    finally:
        log("tearing down")
        for proc in (robot_proc, cloud_proc):
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    proc.kill()
        cloud_log.close()
        robot_log.close()
        log("=== check /tmp/diag-cloud.log and /tmp/diag-robot.log for subprocess logs ===")


if __name__ == "__main__":
    asyncio.run(main())
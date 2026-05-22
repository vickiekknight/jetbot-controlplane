"""
Live terminal dashboard for the user CLI.

Renders four panels with Rich:

  ┌─ Session ────────────────────────────────────────┐
  │ robot-1 (sess_abc123)   user=test   state=live   │
  └──────────────────────────────────────────────────┘
  ┌─ Telemetry ──────────────────────────────────────┐
  │ Sensor     │ state=0.150 pose=(0.32, 0.00, 0.0)  │
  │ Processed  │ status=warning  (from Player)       │
  │ Status     │ all systems nominal                 │
  └──────────────────────────────────────────────────┘
  ┌─ Trail ──────────────────────────────────────────┐
  │ ......................r..............            │
  │ .........................r...........            │
  │ (recent positions on a small ASCII grid)         │
  └──────────────────────────────────────────────────┘
  ┌─ Command ────────────────────────────────────────┐
  │ forward | backward | left | right | stop | quit  │
  │ >>> sent: forward                                │
  └──────────────────────────────────────────────────┘

Dashboard is the view layer. The CLI updates it by calling update_*
methods, which trigger an immediate redraw. The trail uses a fixed-size
character grid that auto-rescales to keep both the origin and the
robot's current position visible.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass, field
from typing import Deque, Optional

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


# Trail grid dimensions. Width is generous for terminals; height kept short
# so the whole dashboard (header + telemetry + trail + prompt) fits in a
# standard 24-row terminal without scrolling.
TRAIL_WIDTH = 60
TRAIL_HEIGHT = 8
TRAIL_HISTORY = 100  # number of recent poses to remember

# Minimum visible extent of the trail's coordinate window, in meters. The
# window always shows at least this much of the world; it auto-expands
# when the robot moves further. The asymmetric defaults (wider X than Y)
# reflect the terminal grid's aspect ratio (60 cols × 8 rows).
TRAIL_MIN_HALF_WIDTH_X = 1.0   # ±1m visible by default
TRAIL_MIN_HALF_WIDTH_Y = 0.25  # ±0.25m visible by default
TRAIL_PADDING_FACTOR = 1.2     # 20% padding around the extreme positions


@dataclass
class DashboardState:
    """Mutable state the Dashboard reads to render its panels."""

    robot_id: str
    session_id: str
    user_id: str
    session_state: str = "connecting"  # "connecting" / "live" / "ended:..."
    last_sensor: Optional[dict] = None
    last_processed: Optional[dict] = None
    last_status: Optional[dict] = None
    # Trail of recent (x, y, theta) poses; newest last.
    trail: Deque[tuple[float, float, float]] = field(
        default_factory=lambda: collections.deque(maxlen=TRAIL_HISTORY)
    )
    # The user's pending input. Drawn at the bottom of the dashboard so
    # users can see what they're typing without the renderer overwriting it.
    input_buffer: str = ""


class Dashboard:
    """
    Live terminal renderer. Driven by a single DashboardState instance.

    Usage:
        dash = Dashboard(state)
        with dash.live():
            ...   # call state.update_*, then dash.refresh()
    """

    def __init__(self, state: DashboardState):
        self.state = state
        self._live: Optional[Live] = None
        self._console = Console()

    def live(self) -> Live:
        """Returns a Rich Live context manager configured for this dashboard."""
        self._live = Live(
            self._render(),
            console=self._console,
            # Drive all refreshes from the asyncio loop via dashboard.refresh()
            # rather than Rich's background thread. Reasons:
            #   1. One refresh source eliminates the auto-refresh-vs-explicit
            #      race that Rich's lock has to mediate every cycle.
            #   2. macOS terminals sometimes don't repaint reliably in
            #      screen=True mode when the auto-refresh thread is running
            #      concurrently with explicit updates.
            #   3. Refresh timing follows actual events (sensor arrival,
            #      command sent) rather than a fixed 4Hz cadence.
            auto_refresh=False,
            # Use the terminal's alternate screen buffer. This isolates the
            # dashboard from anything else writing to stdout/stderr (asyncio
            # teardown noise, library warnings, the user's typed input). On
            # exit, the previous terminal contents are restored automatically.
            screen=True,
        )
        return self._live

    def refresh(self) -> None:
        """
        Re-render the dashboard. Call after each state update.

        Passes refresh=True to force an immediate repaint rather than
        relying on the auto-refresh thread alone. Without this, Rich Live
        stores the new renderable but waits until the next auto-refresh
        tick (every 250ms) to paint it. On some terminals and platforms,
        the auto-refresh thread can lag or miss updates in screen=True
        mode — explicit refresh sidesteps that entire class of issue.
        """
        if self._live is not None:
            self._live.update(self._render(), refresh=True)

    # ----- state-update helpers (so CLI doesn't poke DashboardState directly) -

    def update_sensor(self, envelope: dict) -> None:
        payload = envelope.get("payload", {})
        self.state.last_sensor = payload
        pose = payload.get("pose")
        if pose is not None:
            self.state.trail.append((pose["x"], pose["y"], pose["theta"]))
        self.refresh()

    def update_processed(self, envelope: dict) -> None:
        self.state.last_processed = envelope.get("payload", {})
        self.refresh()

    def update_status(self, envelope: dict) -> None:
        self.state.last_status = envelope.get("payload", {})
        self.refresh()

    def set_session_state(self, session_state: str) -> None:
        self.state.session_state = session_state
        self.refresh()

    def set_input_buffer(self, text: str) -> None:
        self.state.input_buffer = text
        self.refresh()

    # ----- rendering --------------------------------------------------------

    def _render(self) -> Group:
        return Group(
            self._header_panel(),
            self._telemetry_panel(),
            self._trail_panel(),
            self._prompt_panel(),
        )

    def _header_panel(self) -> Panel:
        state_color = {
            "connecting": "yellow",
            "live": "green",
        }.get(self.state.session_state, "red")
        text = Text.from_markup(
            f"[cyan]{self.state.robot_id}[/cyan] "
            f"[dim]({self.state.session_id})[/dim]   "
            f"user=[cyan]{self.state.user_id}[/cyan]   "
            f"state=[{state_color}]{self.state.session_state}[/{state_color}]"
        )
        return Panel(text, title="Session", border_style="blue")

    def _telemetry_panel(self) -> Panel:
        table = Table.grid(padding=(0, 2))
        table.add_column(style="bold cyan", no_wrap=True)
        table.add_column()

        # Sensor row
        if self.state.last_sensor is not None:
            s = self.state.last_sensor
            pose = s.get("pose", {})
            sensor_text = (
                f"state=[yellow]{s.get('state', 0):.3f}[/yellow]   "
                f"pose=({pose.get('x', 0):.2f}, {pose.get('y', 0):.2f}, "
                f"{pose.get('theta', 0):.2f})   "
                f"last_cmd=[dim]{s.get('last_command', 'n/a')}[/dim]"
            )
        else:
            sensor_text = "[dim]waiting...[/dim]"
        table.add_row("Sensor", sensor_text)

        # Processed row
        if self.state.last_processed is not None:
            p = self.state.last_processed
            status = p.get("status", "n/a")
            status_color = {
                "normal": "green",
                "warning": "yellow",
                "alert": "red",
            }.get(status, "white")
            processed_text = (
                f"status=[{status_color}]{status}[/{status_color}]   "
                f"(from Player)"
            )
        else:
            processed_text = "[dim]waiting...[/dim]"
        table.add_row("Processed", processed_text)

        # Status row
        if self.state.last_status is not None:
            st = self.state.last_status
            level = st.get("level", "info")
            level_color = {"info": "cyan", "warn": "yellow", "error": "red"}.get(
                level, "white"
            )
            status_text = (
                f"[dim]{st.get('source', '?')}[/dim]: "
                f"[{level_color}]{st.get('message', '')}[/{level_color}]"
            )
        else:
            status_text = "[dim]none[/dim]"
        table.add_row("Status", status_text)

        return Panel(table, title="Telemetry", border_style="cyan")

    def _trail_panel(self) -> Panel:
        """ASCII visualization of recent robot positions.

        Coordinate window auto-rescales: bounds are computed from the
        trail plus the origin (0, 0), with a small padding factor and a
        minimum extent. This keeps both the magenta '+' origin and the
        green current-position glyph in frame even as the robot moves
        far from the start.
        """
        # Collect all points we want visible: every trail entry + origin.
        xs = [0.0] + [p[0] for p in self.state.trail]
        ys = [0.0] + [p[1] for p in self.state.trail]
        cx = (max(xs) + min(xs)) / 2.0
        cy = (max(ys) + min(ys)) / 2.0
        half_x = max(TRAIL_MIN_HALF_WIDTH_X,
                     (max(xs) - min(xs)) / 2.0 * TRAIL_PADDING_FACTOR)
        half_y = max(TRAIL_MIN_HALF_WIDTH_Y,
                     (max(ys) - min(ys)) / 2.0 * TRAIL_PADDING_FACTOR)
        min_x, max_x = cx - half_x, cx + half_x
        min_y, max_y = cy - half_y, cy + half_y

        def to_grid_x(x: float) -> int:
            return int((x - min_x) / (max_x - min_x) * (TRAIL_WIDTH - 1))

        def to_grid_y(y: float) -> int:
            # Invert so positive y is "up" on the terminal.
            return TRAIL_HEIGHT - 1 - int(
                (y - min_y) / (max_y - min_y) * (TRAIL_HEIGHT - 1)
            )

        grid = [["·"] * TRAIL_WIDTH for _ in range(TRAIL_HEIGHT)]

        # Origin marker.
        ox, oy = to_grid_x(0.0), to_grid_y(0.0)
        if 0 <= ox < TRAIL_WIDTH and 0 <= oy < TRAIL_HEIGHT:
            grid[oy][ox] = "+"

        # Trail points. Older first so the newest can overwrite collisions.
        for i, (x, y, theta) in enumerate(self.state.trail):
            gx, gy = to_grid_x(x), to_grid_y(y)
            if not (0 <= gx < TRAIL_WIDTH and 0 <= gy < TRAIL_HEIGHT):
                continue
            is_newest = (i == len(self.state.trail) - 1)
            grid[gy][gx] = _heading_glyph(theta) if is_newest else "o"

        # Compose into a styled Text. The newest position is bold green,
        # the origin is magenta, the rest of the trail is cyan.
        newest_gx, newest_gy = -1, -1
        if self.state.trail:
            nx, ny, _ = self.state.trail[-1]
            newest_gx, newest_gy = to_grid_x(nx), to_grid_y(ny)

        body = Text()
        for ri, row in enumerate(grid):
            for ci, ch in enumerate(row):
                if ri == newest_gy and ci == newest_gx:
                    body.append(ch, style="bold green")
                elif ch == "o":
                    body.append(ch, style="cyan")
                elif ch == "+":
                    body.append(ch, style="magenta")
                else:
                    body.append(ch, style="dim")
            body.append("\n")
        return Panel(
            body,
            title="Trail (newest in green, origin in magenta)",
            border_style="cyan",
        )

    def _prompt_panel(self) -> Panel:
        commands = "[dim]forward | backward | left | right | stop | quit[/dim]"
        prompt = Text.from_markup(
            f"{commands}\n"
            f"[dim](keystrokes hidden while dashboard is active — "
            f"type your command and press Enter)[/dim]\n"
            f"[cyan]>>>[/cyan] {self.state.input_buffer}"
        )
        return Panel(prompt, title="Command", border_style="green")


def _heading_glyph(theta: float) -> str:
    """Arrow glyph closest to the given heading (radians)."""
    # Discretize to 8 directions.
    import math
    sectors = [
        (0.0, ">"),
        (math.pi / 4, "↗"),
        (math.pi / 2, "^"),
        (3 * math.pi / 4, "↖"),
        (math.pi, "<"),
        (-3 * math.pi / 4, "↙"),
        (-math.pi / 2, "v"),
        (-math.pi / 4, "↘"),
    ]
    # Normalize theta to (-pi, pi]
    t = ((theta + math.pi) % (2 * math.pi)) - math.pi
    best = min(sectors, key=lambda s: abs(_angle_diff(s[0], t)))
    return best[1]


def _angle_diff(a: float, b: float) -> float:
    import math
    d = a - b
    while d > math.pi:
        d -= 2 * math.pi
    while d < -math.pi:
        d += 2 * math.pi
    return d
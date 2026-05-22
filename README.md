# JetBot Control Plane

A small distributed system that orchestrates a triangle of pub/sub peers — robot, user, and per-session inference worker ("player") — connected through a thin cloud orchestrator. Built as a take-home for General Robotics; modeled on the GRID architecture (cloud control plane + per-robot inference workers + direct peer-to-peer data plane).

## Quickstart

```bash
make install        # install dependencies (one time)
make test           # run the full test suite (~13s, 114 tests)
make demo           # fully-automated multi-robot demo
make interactive    # boot cloud + 2 robots, then drive from a 2nd terminal
make bench          # latency benchmark
```

`make demo` is the fastest way to see the whole system work. It boots the cloud and two robots, then programmatically drives each one in turn, ending with a summary table:

```
┏━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Robot   ┃   Cmd    ┃ Sensor msgs ┃ Processed msgs ┃ Final state ┃ Final pose ┃ Final status ┃
┡━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
│ robot-1 │ forward  │           4 │              4 │       0.210 │ x=0.84     │    alert     │
│ robot-2 │ backward │           4 │              5 │       0.210 │ x=-0.84    │    alert     │
└─────────┴──────────┴─────────────┴────────────────┴─────────────┴────────────┴──────────────┘
```

`make interactive` boots the same system and prints instructions for connecting a user CLI from another terminal — the operator can type `forward`, `left`, `right`, `backward`, `stop` and watch the live dashboard react. The CLI shows real-time telemetry, a classifier verdict, and an ASCII trail of recent robot positions.

## Architecture

```
                  ┌──────────────────────────────────┐
                  │     Cloud Service                │   control plane only:
                  │  (REST + Registry + Signaling)   │   register, heartbeat,
                  │                                  │   list, session setup
                  └──────┬────┬────┬─────────────────┘
                         │    │    │
                         │    │    │
                    ┌────┘    │    └────┐
                    │         │         │
                  Robot      User    Player              ← per-session
                                                          subprocess
                  data plane (after handshake):
                  Robot ━━━ sensor ━━▶ Player
                  Robot ━━━ sensor ━━▶ User
                  Player ━━ processed ▶ User
                  User ━━━━ command ━▶ Robot

                    direct peer-to-peer over ZMQ;
                    the cloud never sees the data
```

**Three entities, four topics, one triangle.** Topics follow the spec's `robot/{id}/<kind>` scheme: `sensor` (Robot→Player,User), `command` (User→Robot), `processed` (Player→User), `status` (anyone→anyone).

**Cloud as introducer, not broker.** The cloud authenticates session requests, orchestrates a three-phase signaling handshake (`session_start` → `peer_ready` → `session_live`), and then steps out of the data path. Sensor messages, commands, and processed messages all travel directly between peers over ZMQ. The cloud's load is independent of throughput — it sees one message per session lifecycle event, not one message per sensor reading.

**Player as inference worker.** When a user requests a session, the cloud spawns a Player subprocess scoped to that session. The Player subscribes to the robot's sensor stream, classifies state magnitude into `normal`/`warning`/`alert`, and publishes back on `processed`. This stands in for the production case where the Player runs an actual model (object detection, RL policy, etc).

## Components

| Module | Purpose |
|---|---|
| `cloud_service/` | FastAPI app: HTTP API (`/robots`, `/sessions`), WebSocket signaling (`/ws/robot/{id}`, `/ws/user/{id}`, `/ws/player/{id}`), session state machine, Player subprocess lifecycle. |
| `robot/` | The robot process: FakeJetBot SDK (forward/backward/left/right/stop with unicycle kinematics + sub-stepping), CloudClient that registers and heartbeats, ZMQ peer for the data plane, command handler, 1Hz sensor publish loop. |
| `player/` | The Player subprocess: WebSocket-driven signaling, ZMQ peer, threshold classifier (`state >= 0.20 → alert`, `>= 0.10 → warning`, else `normal`). |
| `user/` | The user-facing CLI (Typer + Rich): `user list`, `user connect <id>` with a Live dashboard that streams sensor/processed/status and accepts typed commands. |
| `common/` | Shared infrastructure: Pydantic schemas (tagged union for signaling messages), Topics helper, ZmqPeer wrapper (PUB+SUBs with handler dispatch). |
| `demo/` | The reviewer-facing demo orchestrator (`automated.py` and `interactive.py`). |
| `benchmarks/` | Latency measurement. |
| `tests/` | 114 tests, ~13s total. Unit tests on individual modules, one full end-to-end test that drives the entire handshake through a real Player subprocess. |

## Latency

`make bench` runs an in-process cloud + robot + subprocess Player + in-process user, drives the robot at 20Hz sensor publish rate for 20 seconds, and reports percentile latencies for two paths:

```
                                  End-to-end latency
┏━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━┓
┃ Path                  ┃ Count ┃   p50 ┃   p95 ┃    p99 ┃    max ┃   Effective┃
┡━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━┩
│ Robot → User (direct) │   394 │ 538µs │ 925µs │ 1.05ms │ 1.60ms │     19.7/s │
│ Robot → Player → User │   394 │ 477µs │ 807µs │  997µs │ 1.51ms │     19.7/s │
└───────────────────────┴───────┴───────┴───────┴────────┴────────┴────────────┘
```

(numbers from one representative run on the development laptop)

**Reading these.** Both paths are sub-millisecond at p50. The Robot→Player→User path goes through one extra ZMQ hop and a classification step, which in absolute terms adds something like 100-200µs; in the run shown it happened to be slightly faster than the direct path, because asyncio event-loop scheduling between the user's two SUB sockets can reorder arrivals by tens of microseconds when both messages arrive together. The takeaway isn't that the pipelined path is "faster" — it's that **both paths are in the same sub-millisecond regime, and the cost of the inference hop is a small fraction of a millisecond on the same host**.

For comparison: a broker-routed design (Robot → Cloud → User) would add the cloud's serve-time to every message, putting throughput proportional to cloud capacity rather than peer capacity. The peer-to-peer design scales horizontally: each triangle is independent, and the cloud handles only signaling.

## Design decisions

The decisions that mattered most, in roughly the order they were made.

**ZeroMQ over MQTT/NATS/ROS2.** The spec describes a peer-to-peer triangle of pub/sub "servers." ZMQ matches that wording literally. MQTT bridges would have been asymmetric and required broker config; NATS clustering hides routing inside opaque cluster state; ROS2/DDS auto-discovery contradicts explicit cloud orchestration. ZMQ has PUB/SUB sockets that bind/connect explicitly and a transport-agnostic endpoint string (`ipc://` or `tcp://`) that fits perfectly into the signaling protocol.

**Control plane / data plane separation.** The cloud handles session setup; peers do everything else. This is the single most consequential architectural choice. It means the cloud's load is bounded by signaling traffic (low-frequency, low-volume) rather than sensor traffic; throughput scales with peer capacity, not cloud capacity; and a cloud restart mid-session doesn't interrupt data flow in established triangles.

**Three-phase signaling handshake** (`session_start` → `peer_ready` → `session_live`). Each peer binds its PUB and reports the endpoint; once the cloud has all three endpoints, it broadcasts the full topology. This eliminates the classic ZMQ slow-joiner problem (publishers sending before subscribers have connected) without resorting to XPUB subscription events: by the time any peer publishes its first message, the cloud has confirmed all SUBs are connected.

**Replace-on-duplicate identity.** A robot that crashes and restarts should be able to come back online without manual deregistration. Strict rejection ("robot already exists") would deadlock that scenario, since the cloud cannot reliably distinguish "robot is still alive" from "robot has crashed" without wall-clock heartbeat timeouts (which would create unavoidable post-crash blackout windows). Replace-on-duplicate trades a theoretical impersonation risk (acceptable in this unauthenticated take-home) for self-healing crash recovery. A production system with cryptographic identities would invert this.

**Marking offline instead of removing.** A dead robot stays in the registry with `status: offline` rather than being deleted. Operators running `user list` after a network blip see "robot-1 was here, currently offline" and know to investigate, rather than seeing nothing and wondering if their query is broken. Re-registration brings it back online via `touch_heartbeat`.

**RobotDriver Protocol decouples the data plane from the SDK.** `CloudClient` calls `self._driver.read_sensor()` and `self._driver.forward(speed)` against a `RobotDriver` protocol, not a concrete `FakeJetBot`. Swapping in `PyBulletDriver` or `JetBotHardwareDriver` requires no changes to networking code. This is the GRID multi-form-factor scaling story made concrete.

**Multi-robot via process replication, not code replication.** Running N robots means running `python -m robot --id robot-X` N times. The single SDK file (`FakeJetBot`) supports any number of instances; the registry's `dict[robot_id, RobotInfo]` keys them independently; topic scoping (`robot/{id}/sensor`) means each robot's data plane is isolated. The `demo/automated.py` script exercises this by booting two robots and driving each in turn.

**Player as subprocess.** Spawning the Player as a separate Python process (via `asyncio.create_subprocess_exec("python", "-m", "player", ...)`) keeps it isolated from the cloud's event loop, makes its resource usage observable in standard OS tooling, and gives it a clean failure boundary — if the Player crashes mid-session, the cloud notices the WebSocket disconnect and ends the session cleanly rather than corrupting cloud state. The cloud also streams the Player's stderr to its own log with a session_id prefix; without that, Player crashes would be silent.

**IPC transport default, TCP opt-in.** ZMQ IPC over Unix domain sockets in `/tmp/` is the default; setting `ZMQ_TRANSPORT=tcp` switches to TCP loopback. IPC is faster (no IP stack) and matches the spec's "single computer" scope; TCP is preserved for multi-host deployment. The ZmqPeer abstraction handles both behind a single API.

**State machine with guarded transitions.** Sessions go through five states (`REQUESTED` → `SPAWNING` → `AWAITING_PEERS` → `LIVE` → `ENDED`); each transition is gated by a method (`mark_spawning`, `record_peer_ready`, `mark_live`) that raises `InvalidTransition` on inconsistent state. Bypass-mutation is impossible — anyone moving a session through the lifecycle has to use the methods, which means the invariants ("can only enter LIVE after all three peer_ready arrived") are enforced at the point of attempt.

## Testing notes

114 tests, ~13 seconds. Split across:

- **Schema and serialization tests** lock down the tagged-union signaling protocol.
- **ZmqPeer tests** cover topic filtering, the slow-joiner-safe handshake assumption, IPC socket file cleanup, both transports.
- **FakeJetBot tests** validate the unicycle kinematics, sub-stepping, motor clamping, sensor shape.
- **Registry tests** validate replace-on-duplicate, heartbeat tracking, status transitions.
- **Cloud API tests** cover the HTTP routes (`/robots`, `/sessions`) including error paths.
- **Signaling tests** drive the WebSocket protocol directly with `TestClient`, exercising heartbeat, disconnect-on-evict, and the dead-robot eviction loop.
- **Robot/User client tests** spin up a real local cloud and exercise the clients against it — small, fast integration tests.
- **Session manager tests** lock down the state machine in isolation.
- **One end-to-end test** spawns a real Player subprocess and drives the entire handshake plus a forward/stop command cycle, verifying sensor messages reach the user, processed messages reach the user with the right classification, and command messages reach the robot's driver.

A couple of testing decisions worth flagging:

**Test doubles at the subprocess boundary.** HTTP and CLI unit tests replace `orchestrator._spawn_player` with a fake that satisfies the `asyncio.subprocess.Process` interface without forking. This keeps the unit suite fast and deterministic. One dedicated end-to-end test uses a real subprocess to validate the actual integration. The split mirrors the test-pyramid pattern: many fast isolated tests, few slow integration tests at the seams.

**"Don't sleep, signal" for async timing.** Tests that cross the ZMQ subscription-propagation boundary (notoriously timing-dependent) use observable signals rather than fixed durations: send the command in a retry loop until the motor state changes, then clear the buffer and wait for a fresh sensor message that reflects post-command state. This eliminates flakiness from event-loop pressure without relying on conservative-but-arbitrary sleeps.

**A testing gap worth calling out.** The end-to-end test imports `CloudClient` and wires up `set_driver(bot)` directly, mirroring the production launcher `robot/__main__.py` by hand. This duplication meant that for a period during development, the launcher was missing the `set_driver` call but the test setup still had it — so the test continued to pass even though `make demo` was silently broken (commands arrived at the robot but `_driver was None`, sensor publishes were skipped). A cleaner design would extract a shared `build_robot(args)` factory used by both the launcher and the test, making the test guarantee parity with production. Captured here as a lesson rather than a fix because the bug is now squashed; in a longer-lived codebase the refactor would be worth it.

## Limitations and future work

**Authentication is deferred.** Robots self-declare their `--id`, users self-declare their `--user-id`, and the cloud accepts any string. This matches the spec's bonus-section scope (security is optional). A production version would have the cloud allocate IDs, sign them, and tie them to hardware tokens. The registry's API is shaped to accept this change without disturbing the rest of the system.

**Single-host scope.** ZMQ IPC over Unix domain sockets in `/tmp/` works only on a single machine. Multi-host deployment would use the existing TCP transport (`ZMQ_TRANSPORT=tcp`), but would also need real IP-or-hostname endpoints in `peer_ready` messages instead of `127.0.0.1`-implicit binds. About 10 lines of change in `common/zmq_peer.py`.

**Player is a stub classifier.** It applies threshold rules to the sensor `state` scalar. The production case is loading a real model. The Player's structure already supports this — it's a long-lived subprocess with its own event loop and ZMQ peer; the only change needed is replacing `classify_state()` with a model invocation.

**No persistence.** Registry and session state are in-memory; restarting the cloud forgets everything. For multi-tenant or long-running deployments, the Registry interface is the natural place to slot in a backing store (Postgres, Redis) without changing its callers.

**User CLI input is line-buffered.** Typing a command and pressing Enter dispatches it; per-keystroke feedback would require a raw-mode terminal reader, which is a significant escalation in complexity for marginal UX gain. The dashboard's "sent: forward" feedback after Enter is the compromise.

## Notes on running this

**Process logs land in `/tmp/jetbot-demo/`** when `make demo` or `make interactive` is run. If something looks broken, `cat /tmp/jetbot-demo/cloud.log` and `cat /tmp/jetbot-demo/robot-1.log` will usually show why.

**The interactive dashboard uses the terminal's alternate screen buffer** so it doesn't pollute scrollback. When you press Ctrl+C to disconnect, your previous terminal contents are restored cleanly.

**Keystrokes aren't echoed while the dashboard is active.** This is intentional — Rich's Live renderer and the terminal's character echo would interleave. Type the command and press Enter; the `Command` panel will show what was sent.

**Robots and the cloud run as subprocesses spawned by the demo scripts.** If you want to run them by hand instead, use three terminals:

```bash
# Terminal 1
python -m cloud_service --port 8000

# Terminal 2
python -m robot --id robot-1 --cloud-url http://localhost:8000

# Terminal 3
python -m user list --cloud-url http://localhost:8000
python -m user connect robot-1 --cloud-url http://localhost:8000
```

## Layout

```
.
├── Makefile                    # convenience targets
├── README.md                   # this file
├── requirements.txt
├── pytest.ini
├── cloud_service/
│   ├── __main__.py             # `python -m cloud_service`
│   ├── app.py                  # FastAPI app factory + routes
│   ├── registry.py             # robot registry (in-memory)
│   ├── signaling.py            # WebSocket handlers + ConnectionManager
│   │                           # + SessionOrchestrator + heartbeat eviction
│   └── session_manager.py      # session state machine
├── common/
│   ├── schemas.py              # Pydantic models (signaling + data)
│   ├── topics.py               # topic-string helpers
│   ├── zmq_peer.py             # PUB+SUBs wrapper with handler dispatch
│   └── logging.py
├── robot/
│   ├── __main__.py             # `python -m robot --id ... --cloud-url ...`
│   ├── client.py               # CloudClient (register + WS + ZMQ + commands)
│   └── sdk.py                  # FakeJetBot + RobotDriver protocol
├── player/
│   ├── __main__.py             # spawned by cloud as a subprocess
│   └── client.py               # signaling lifecycle + classifier
├── user/
│   ├── __main__.py             # `python -m user list / connect ...`
│   ├── cli.py                  # Typer commands + Rich Live dashboard wiring
│   ├── client.py               # UserSession + HTTP/WS helpers
│   └── dashboard.py            # the Live dashboard renderer
├── demo/
│   ├── _orchestrator.py        # boots cloud + robot subprocesses
│   ├── automated.py            # `make demo`
│   └── interactive.py          # `make interactive`
├── benchmarks/
│   └── latency.py              # `make bench`
└── tests/
    ├── test_schemas.py
    ├── test_zmq_peer.py
    ├── test_fake_jetbot.py
    ├── test_registry.py
    ├── test_cloud_api.py
    ├── test_signaling.py
    ├── test_robot_client.py
    ├── test_user_cli.py
    ├── test_session_manager.py
    ├── test_session_e2e.py
    └── test_player_classifier.py
```
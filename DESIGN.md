# JetBot Control Plane — Design Document

**For setup instructions, running examples, and the test/benchmark commands, see [README.md](README.md).**

## 1. Framing

### 1.1 What this system is
Jetbot Controlplane is a cloud-orchestrated, peer-to-peer pub/sub network that dynamically connects robots, users, and per-session cloud players for real-time data exchange. This implementation delivers the required cloud-orchestrated, three party architecture by establishing a dynamic control plane and a decentralized data plane. When a robot boots and registeres via a POST request, the Cloud Service tracks its connection status in a managed Registry using a 2 second WebSocket heartbeat loop. Remote control terminal initialization triggers the cloud control plane to dynamically spawn a cloud-side inference player as an independent process, subsequently broadcasting a session topology that directs the Robot, User, and Player to spin up ZeroMQ-backend pub/sub endpoints. These three servers exchange bind coordinates to automatically form a direct, triangle peer-to-peer network mesh, entirely cutting out central bottlenecks to allow the edge device, user dashboard, and cloud worker to directly publish and subscribe to the real-time sensor, processed, and command topic channels.

### 1.2 The three entities and the triangle
The data plane operates as a decentralized triangle network mesh composed of three ZeroMQ pub/sub peers: the edge-based Robot, the User terminal, and the cloud-side Player. While there are four distinct processes total, the central Cloud Service lives entirely outside this data triangle as an administrative orchestrator. The Cloud Service manages connection lifecycles and dynamically spawns the Player process on demand, but never touches raw sensor data. Choosing a direct triangle topology over a centralized star or bus broker (which both require a middleman to relay data) ensures that topic-specific data streams map directly to their consumers with minimal latency, allowing the robot to communicate instantly with both the user and its cloud inference player. Having the Cloud and the Player as separated processes ensures critical resource isolation, so a buggy inference crash will not bring down the orchestrator and kill every other robot's session; supports horizontal scaling, since Players can be spawned on different machines, containers, or Kubernetes pods while the orchestrator stays one logical thing; provides lifecycle clarity, since the orchestrator outlives sessions while Players are session-scoped; and makes multi-robot/multi-user trivial, since one orchestrator manages N Players, one per active session.

### 1.3 Control plane vs. data plane
The Cloud Service lives in the control plane and is responsible for orchestration: maintaining the Registry, accepting session requests, spawning Player subprocesses, broadcasting signaling messages, and detecting dead robots through heartbeat eviction. The data plane is everything else — sensor readings, commands, and processed classifications flowing directly between the three ZMQ peers without ever crossing the cloud. The cloud sees signaling messages and nothing else: "robot registered," "heartbeat received," "user wants a session," "session is up."

This separation matters for three concrete reasons. First, scalability: one cloud instance can orchestrate thousands of robots because it only handles control-plane traffic, which is low-volume and infrequent. If sensor data flowed through the cloud, throughput would be bottlenecked by the cloud's bandwidth and CPU. The peer-to-peer data plane scales horizontally because every triangle is independent. Second, latency: Robot to Player message latency is one ZMQ hop, roughly 50 microseconds on the same host. Routing through the cloud would add two extra hops, two extra serialization steps, and two extra context switches, which is unacceptable for real-time robotics. Third, reliability: if the cloud crashes mid-session, the triangle keeps working because peers don't need the cloud to deliver messages, only to establish the topology. New sessions can't be created until the cloud comes back, but ongoing ones survive. The cloud is essentially a phone book plus an introducer — it knows who's around and connects two parties so they can talk directly, then steps aside.

---

## 2. Core Design Principles

### 2.1 Single source of truth per concern
Three coordination concerns live in three separate modules, each owning exactly one thing. The Registry owns robot identity and status — which robots exist in the world right now, whether they're online or offline, when they last heartbeated. The SessionManager owns session lifecycle — which sessions exist, what state each one is in, when they were created and ended. The ConnectionManager owns active WebSocket references — which robots, users, and players currently have open signaling channels, indexed by their respective IDs.

Splitting these is better than one god-object for two reasons. First, each module has a single reason to change: if I ever swap the in-memory Registry for Redis, the SessionManager and ConnectionManager don't need to know. Second, the boundaries between modules are explicit in code: when the SessionOrchestrator needs to send a message to a robot's WebSocket, it has to go through the ConnectionManager, which makes the data flow visible and auditable. One conflated class would hide all of that behind attribute access.

### 2.2 Idempotent identity assertions
- Replace-on-duplicate registration.
- Replace-on-duplicate WebSocket attachment.
- Why "last write wins" is the right call when identity is unauthenticated.
- When it would be wrong (authenticated identities) and how the design accommodates that future change.

Registration is replace-on-duplicate: a robot claiming an ID always succeeds, overwriting any prior entry. The same principle applies one layer up at WebSocket level. For instance, if a robot opens a new WebSocket while a prior one was still attached, the prior one is closed and the new one takes its place. The justificaiton is that crashes happen, network partitions happen, and the cloud cannot reliably distinguish "robot is still alive" from "robot has crashed" without wall-clock heartbeat timeouts. Forcing strict rejection on duplicate would mean a restarted robot is locked out of the system because the previous incarnation of itself didn't clean up properly. The only escape from that bind is heartbeat-driven eviction, which produces unacceptable post-crash blackout windows.

Replace-on-duplicate trades a theoretical impersonation vulnerability for a robust crash-recovery story. The vulnerability is theoretical because identity here is unauthenticated, as robots self-declare their `--id` via CLI, and the cloud accepts any string. A production system with cryptographic identities would invert this trade-off: if a robot's identity is signed by a hardware token, replace-on-duplicate becomes a denial-of-service vector, and strict rejection becomes the right call. The registry's API would accept that change without disturbing the rest of the system.

### 2.3 Failure-oriented design
The infrastructure layers (topics, peer wrapper, schemas, state machine) are designed to make wrong behavior hard rather than to optimize for happy-path elegance. Centralized topic construction in `common/topics.py` eliminates string-typo bugs where one publisher writes `robots/{id}/sensor` and one subscriber writes `robot/{id}/sensor` and nothing surfaces an error. Signaling-mediated session establishment eliminates ZMQ slow-joiner bugs by ensuring SUBs are connected before any PUB publishes. State-machine guards on the Session class raise `InvalidTransition` rather than silently corrupting state when a transition is attempted from an inconsistent place.

Three failure-handling choices reinforce this. Mark-offline rather than remove on disconnect, so operators triaging an incident can see "robot-1 was here, currently down" rather than seeing nothing and wondering if their query is broken. Idempotent `end()` — the terminal session transition is reachable from any state and safe to call twice, so cleanup code doesn't have to reason about which state things are in. Subprocess crash boundaries for the Player — a Player segfault sends a SIGCHLD that the cloud's `_terminate_player` handles cleanly, rather than corrupting cloud state through a shared address space. Each of these is justified by a specific class of failure it prevents, not by aesthetic preference.

### 2.4 Explicit lifecycle ownership
Three lifecycles, three owners, no overlap. The Player is session-scoped: the cloud spawns it when a session is requested, the cloud kills it when the session ends, nothing else manages its lifetime. The orchestrator is process-scoped: it lives with the cloud process from app startup through app shutdown, accumulating sessions over time but never being recreated. The Robot is self-scoped and self-healing: it manages its own connection to the cloud, reconnects with exponential backoff on failure, and the only thing that can stop it is SIGINT or SIGTERM. This means a cloud restart, a network blip, or a transient host issue doesn't require operator intervention to recover from.

---

## 3. Middleware Choice

### 3.1 Why ZeroMQ
- Peer-to-peer topology fits the spec's "three pub/sub servers" wording literally.
- No broker process to operate.
- Transport-agnostic (IPC vs TCP) behind one API.
- Topic filtering at the wire level (prefix match on first frame).

ZeroMQ fits the spec's "three pub/sub servers" wording literally. Each entity binds a PUB socket on a known endpoint and connects SUB sockets to the other entities' PUB endpoints. Topic filtering is built in via byte-prefix matching on the first frame. There is no separate broker process to operate, monitor, or scale — each pub/sub server is just my process binding a socket. Transport is configurable behind one API: IPC (Unix domain sockets) for single-machine deployments, TCP for cross-host, with the rest of the system treating endpoint strings as opaque.

For the scale of this take-home, ZMQ is the right call. If I were to scale to 25K+ concurrent robots, I would transition to a federated broker model (NATS clusters or MQTT with regional brokers) and probably a separate control plane (gRPC). The framing matters: for this take-home's specific architecture, ZMQ matches the requirements exactly with no impedance mismatch. For the production system this take-home is a tiny version of, broker-based messaging becomes the right answer for different reasons — persistence, fan-out at scale, operational maturity.


### 3.2 Alternatives considered

**NATS.** NATS supports leaf-node mode (an isolated local system connected to a larger central network, where only explicitly allowed subjects are routed) and cluster mode (multiple servers in a tightly coupled mesh where all nodes share state and routing data). Neither matches the spec's "three peer brokers" framing. Picking cluster mode would mean the three servers form one logical broker, which is the opposite of what the spec asks for. Beyond the topology mismatch, it's not easy to see or control the routing path through a clustered NATS deployment, which means benchmarking robot-to-user latency would require instrumenting the cluster internals. The routing isn't deterministic from my code. NATS is the right call when you need high-throughput cluster-scoped pub/sub with optional persistence and you're willing to standardize on it as your primary messaging layer.

**MQTT.** To form a true mesh of three brokers, each bridged to the other two like a triangle, I would need to write directional topic-forwarding rules on each broker for each peer — three brokers, two outbound bridges each, multiplied by N topics. Bridge configuration is static and file-based, so adding or removing a bridge means rewriting config and restarting. This is not in line with how the spec asks for a per-session dynamic topology. MQTT is fundamentally a centralized broker protocol, like IoT scenarios where many devices connect to one broker, and bridges are designed for edge-broker-forwards-to-central-broker patterns. MQTT is the right call for production fleet management with thousands of robots connecting to regional brokers — persistence, QoS, retained messages, last-will-and-testament, MQTT-over-TLS — all of which matter at scale and are exactly why platforms like AWS IoT Core and HiveMQ exist. For the take-home's three-entity demo, none of it matters.

**ROS2 / DDS.** DDS shares the high-level shape of what the spec asks for: no central broker, topic-based pub/sub, peer-to-peer data flow. But DDS uses automatic discovery, and the spec wants cloud-orchestrated handshakes. Building on DDS would mean fighting the framework — disabling automatic discovery, injecting topology information manually, working against the QoS settings that DDS gives you for free. The decentralized shape is similar to ours, but the assumptions about how peers find each other are not.

### 3.3 Headline trade-off
For this scope — single host, demo-scale, take-home with explicit cloud orchestration — ZMQ is the right tool because it matches the architecture exactly with no impedance mismatch. At a scale with thousands of robots, federated regions, persistent fan-out, and operational maturity requirements that demand monitoring and replay, one of the broker-based alternatives becomes the right answer. The trade-off is not "ZMQ vs. NATS" in the abstract; it's "what is the architecture asking for, and which tool fits it without bending."

---

## 4. Signaling Protocol Design

### 4.1 The three-phase handshake
The signaling protocol is `session_start` → `peer_ready` → `session_live`, with each phase having one job. `session_start` tells each peer "bind your PUB socket and report where you're listening." `peer_ready` is the peer's response: "I'm bound at this endpoint." Once the cloud has all three `peer_ready` messages, it has the full topology and broadcasts `session_live` containing every peer's endpoint, with the implicit instruction "now connect your SUBs to the other two endpoints." This handshake is cloud-orchestrated rather than peer-discovered because the spec explicitly requires the cloud to coordinate session establishment, and orchestration is simpler than discovery — peers don't need to find each other through broadcast or registry queries, the cloud just tells them.

### 4.2 The slow-joiner problem and how this solves it
ZMQ PUB sockets have at-most-once delivery semantics, which means messages published before a SUB has finished its subscription handshake are silently dropped. In practice this means the first second or two of sensor data after a session starts could be missing if any peer publishes too early. The three-phase handshake eliminates this by ordering the operations: every peer has bound its PUB (via `session_start`) and the cloud knows every endpoint (via `peer_ready`) before any peer is told to connect (via `session_live`). By the time the data plane is asked to publish, every subscriber in the triangle has already connected.

Alternatives I considered: a magic-number sleep on each peer (200ms after `connect()`) is wasteful on fast networks and flaky on slow ones; XPUB/XSUB sockets which deliver subscription events as readable messages, which would require introducing new socket types and counting subscribers across multiple SUBs, and would still only prove that a SUB has subscribed to my PUB, not the reverse; and a REQ/REP handshake separate from the PUB/SUB path, which adds another socket per peer pair, more state, and only proves REQ/REP works, not that PUB/SUB subscription has propagated. Cloud-coordinated phased handshake uses infrastructure that already exists for session setup, requires no additional socket types or in-band coordination messages on the data plane, and keeps the architectural principle clean: control-plane concerns belong in the control plane.

### 4.3 "Bind then connect" invariant
Order matters in the handshake, and the invariant is enforced by the protocol itself. `peer_ready` arrives only after `bind()` has succeeded, because peers can't report an endpoint they haven't bound. `session_live` arrives only after all three `peer_ready` messages have arrived, because the cloud broadcasts it from the third-`peer_ready` callback. Therefore, when a peer receives `session_live` and starts calling `connect_to_peer()` for the other two, the endpoints it's connecting to are real and listening. Connecting SUBs uses real endpoints, not promises.

### 4.4 Cloud as introducer, not broker
The cloud sees signaling messages and never sees data messages. Once `session_live` has been broadcast, the cloud could crash and the triangle would keep working — peers don't need the cloud to deliver sensor readings, only to establish the topology. New sessions couldn't be created until the cloud comes back, but ongoing ones survive. This is the architectural property that makes the design scalable and fault-tolerant: the cloud's load is bounded by control-plane traffic, which is low-frequency and low-volume, regardless of how much data the triangle is exchanging.

---

## 5. Session State Machine

### 5.1 The five states
Sessions move through five states: `REQUESTED` when the cloud has accepted the user's request and allocated a `session_id`; `SPAWNING` when the Player subprocess has been launched and the cloud is waiting for it to attach its WebSocket; `AWAITING_PEERS` when the cloud has broadcast `session_start` and is collecting `peer_ready` messages; `LIVE` when all three peers are connected and the triangle is operational; and `ENDED` when the session has been torn down for any reason. Each transition is triggered by a specific event: Player subprocess launch moves `REQUESTED` to `SPAWNING`, Player WebSocket attach moves `SPAWNING` to `AWAITING_PEERS`, the third `peer_ready` moves `AWAITING_PEERS` to `LIVE`, and any disconnect, timeout, or explicit end moves the session to `ENDED`.

### 5.2 Guarded transitions
All transitions are methods on the `Session` class (`mark_spawning`, `record_peer_ready`, `mark_live`, `end`), not attribute mutation. Each method checks that the current state allows the transition and raises `InvalidTransition` if not. This beats "validate at the end" because invalid state is caught at the point of attempt rather than discovered later when something downstream fails confusingly. It beats "check before mutating elsewhere" because the check and the mutation are atomic — no other code path can intervene between them. The state machine is the source of truth about what's allowed, and bypassing it isn't possible without bypassing the public API.

### 5.3 end() is the universal escape hatch
`end()` is the one transition that's reachable from any state and safe to call repeatedly. This is intentional: cleanup code doesn't have to reason about whether a session is in `SPAWNING` versus `LIVE` versus already-ended. A WebSocket disconnect handler just calls `orchestrator.end_session(session_id, "user disconnected")` and the orchestrator does the right thing regardless of where the session was in its lifecycle. Idempotency at the terminal transition means partial-failure paths don't accumulate state — if `end()` is called during teardown and again during garbage collection, the second call is a no-op.

### 5.4 SessionManager vs. Session split
A `Session` is one row of state — it knows about its own ID, its own state, its own endpoints, its own transitions. The `SessionManager` is the collection plus operations that span multiple sessions: allocating IDs, looking up by ID, listing all active sessions, ending sessions by ID, and waiting for a specific session to reach `LIVE`. The manager handles cardinality; the session handles its own invariants. This split mirrors the broader pattern in the cloud (Registry vs. RobotInfo, ConnectionManager vs. WebSocket reference) — collections live in managers, instances enforce their own rules.

---

## 6. Process Boundaries

### 6.1 Why Player is a subprocess (not a thread, not an asyncio task)
The Player is a separate OS process spawned via `asyncio.create_subprocess_exec("python", "-m", "player", ...)`, not a thread or an asyncio task. Three reasons. Crash isolation: a Player segfault, an uncaught exception in inference code, or an OOM kill takes down only that Player, leaving the cloud and every other active session untouched. A thread or task would share the cloud's address space, and any of those failures would cascade. Resource accountability: standard OS tooling (`ps`, `top`, `htop`) sees each Player's CPU and memory independently, which makes debugging and capacity planning concrete. Cleanup ergonomics: SIGTERM is universal and well-understood; cancelling an asyncio task across module boundaries is fiddly and easy to get wrong. The mental model is clearer too — each Player is a "worker" with its own life, not a coroutine fighting for time on the cloud's event loop.

### 6.2 Costs of subprocess spawn
- ~200ms cold-start time (Python import + module load).
- Acceptable for session-scoped work; wrong for per-request work.
- How we mitigate: spawned once per session, lives until session_end.
Subprocess spawn has a ~200ms cold-start cost: forking a new process, loading the Python interpreter, importing the `player` module, and reaching the WebSocket connect call. This is fine for session-scoped work where Players live for minutes or hours, and it would be wrong for per-request work where you'd be paying 200ms on every operation. 

### 6.3 Stderr re-logging
The cloud captures each Player subprocess's stderr stream and re-logs every line with a `[player {session_id}]` prefix into the cloud's own log. Without this, Player crashes would be silent — the subprocess dies, the cloud notices the WebSocket disconnect, and there's no way to know what went wrong without manually attaching a debugger. With stderr re-logging, the cloud's log is a complete record of everything that happened across the entire system, including the inside of each Player. About ten lines of code that has saved hours of debugging.

---

## 7. The Cloud's Two Jobs

### 7.1 Directory service (Registry + REST)
The Registry is the cloud's single source of truth about which robots exist in the world right now. It answers questions every other piece of the system needs to ask: does this robot exist, what robots are available, is this robot still alive, did it drop. `POST /robots/register` adds a robot to the Registry (or refreshes its entry if already present). `GET /robots` returns the current Registry contents for the user CLI to display. The two endpoints are separate because they serve different audiences and different operational concerns — registration is a robot's self-assertion, listing is a user's discovery query. Heartbeats happen over WebSocket rather than HTTP because they're high-frequency (every 2 seconds per robot) and the WebSocket already exists for signaling — using a separate HTTP endpoint would mean opening and closing a TCP connection every 2 seconds per robot, which is wasteful and operationally noisy.

### 7.2 Session orchestration (SessionOrchestrator + signaling WebSockets)
The SessionOrchestrator owns the per-session lifecycle: spawn the Player, wait for it to attach, broadcast `session_start`, collect `peer_ready` messages, broadcast `session_live`, then on disconnect or end broadcast `session_end` and kill the Player. The orchestrator owns subprocess lifetimes because Players are scoped to sessions, and the orchestrator is the only thing that knows when a session starts and ends. The orchestrator is also the only place in the codebase where sessions are created or ended — the HTTP route handler calls `orchestrator.start_session()`, the WebSocket disconnect handlers call `orchestrator.end_session()`, and nothing else mutates session state. This makes the lifecycle auditable from one file.

### 7.3 Why both live in the same FastAPI app
Directory service and session orchestration share state — the orchestrator checks the Registry to confirm a robot is online before spawning a Player for it, and both use the same `ConnectionManager` for routing WebSocket messages. Splitting them into separate processes would require an inter-service protocol (RPC, message bus) for what is currently a method call. A larger deployment would split them: in production, the directory service might be a separate FastAPI app behind a load balancer, with the orchestrator running on dedicated nodes with subprocess-spawn permissions. The take-home doesn't need that.

---

## 8. Multi-Robot, Multi-Form-Factor

### 8.1 Multi-robot via process replication
Multi-robot support is built into the architecture from the start, not bolted on. Running N robots means running `python -m robot --id robot-X` N times — same SDK, same entry point, different IDs. The Registry keys on `robot_id` (a `dict[robot_id, RobotInfo]`, not a single `RobotInfo` variable), so adding robots is a write to the dict, not a code change. Topics are scoped `robot/{id}/sensor`, `robot/{id}/command`, etc., from the start, so each robot's data plane is isolated from every other robot's by ZMQ subscription filtering. This scales to N robots without any source-code changes, limited only by host resources (one process per robot).

### 8.2 RobotDriver protocol for multi-form-factor
The `RobotDriver` protocol mirrors the official `jetbot.Robot` public API (forward, backward, left, right, stop, set_motors, step, read_sensor). The robot's `CloudClient` holds a `RobotDriver` reference but never knows its concrete type. New drivers — real JetBot hardware, PyBullet, Isaac Sim, replay-from-recording, mock-for-testing — can be added in a single file and registered via configuration without touching the network, signaling, or command-dispatch code. Protocol injection (rather than subclassing) is the right shape here because it makes the dependency direction explicit: the protocol is owned by the network layer, drivers conform to it, and there's no shared base class that drivers inherit from. The same code runs against `FakeJetBot` in dev and would run against real hardware in production by changing a single line of wiring. This is the dependency-inversion seam that makes "any robot, any form factor" a tractable architectural problem rather than a refactoring problem.


### 8.3 What changes at scale
At thousands of robots, multiple regions, persistent connections measured in hours rather than minutes — several layers of this design would change. Federated brokers (NATS clusters, Kafka topics) would replace direct ZMQ peers for cross-region traffic, where latency and connectivity make point-to-point unreliable. Regional control planes with replication would replace the single cloud process, with state synchronized via Raft or a managed Postgres. A persistence layer would back the Registry (Redis for hot state, Postgres for historical), so cloud restarts wouldn't forget the world. TURN/STUN/relay infrastructure would handle NAT traversal for robots in customer networks. Kubernetes or container orchestration would replace `asyncio.create_subprocess_exec` for Player spawn, giving you scheduling, autoscaling, and isolation that subprocess can't. Each of these changes is independent, and each replaces a single component in the current design — the architecture doesn't require a rewrite to scale, just substitution of the appropriate components.

---

## 9. Testing Strategy

### 9.1 Test pyramid
The test suite has 114 tests. Many are fast unit tests with stubbed subprocess boundaries — schema validation, Registry behavior, ZMQ peer wrapper, FakeJetBot kinematics, the session state machine in isolation. A few are integration tests at module boundaries — HTTP API tests with a live FastAPI app, WebSocket handshake tests with a real cloud, robot client tests against a uvicorn-hosted cloud in the same process. One is end-to-end with a real Player subprocess (`test_session_e2e.py`), spawning all three peer types via `python -m` and asserting that a command round-trips and a sensor reading reaches the user. The pyramid is intentional: many fast tests at the leaves where unit-level bugs hide, fewer slow tests at the seams where integration bugs hide.

### 9.2 Test doubles at the subprocess boundary
HTTP and CLI unit tests replace `orchestrator._spawn_player` with a fake that satisfies the `asyncio.subprocess.Process` interface but doesn't actually fork a process. This keeps the unit suite fast and deterministic — no real subprocess startup costs, no risk of subprocess leaks between tests, no flakiness from subprocess scheduling. End-to-end correctness of the spawn-and-attach path is validated by one dedicated test that uses a real subprocess. The split mirrors the test pyramid principle: many fast isolated tests for behavior, few slow integration tests at the seams between systems.

### 9.3 "Don't sleep, signal" for async timing
Async and event-driven systems should not be tested with hardcoded sleeps. `time.sleep(1)` is simultaneously too slow on fast machines and too fast on slow ones, producing flaky tests in both directions. The pattern used throughout the suite is `_wait_for(predicate, timeout, interval)`, which polls a condition with a tight inner loop and a bounded outer timeout. Tests finish as soon as the condition is true and only wait the full timeout when something is actually broken. For data-plane crossings specifically, "retry until observably effective" replaces fixed settles: commands are re-sent until the underlying state changes, assertions on streamed messages clear the receive buffer first and wait for fresh arrivals. The general principle: event-driven systems need event-driven test waits, not wall-clock waits.

### 9.4 Test/production parity gap (lesson learned)
The end-to-end test wires `CloudClient + FakeJetBot + set_driver(bot)` directly in test code, mirroring `robot/__main__.py` by hand. During development, when the launcher was missing the `set_driver(bot)` call, the test continued to pass because the test had its own copy of the setup. A real bug — robot launcher missing `set_driver()`, which broke `make demo` — survived the test suite because the test never ran the launcher. The general lesson: tests should call the same setup function the production entry point calls, not duplicate it. A shared `build_robot(args)` factory used by both `robot/__main__.py` and the e2e test would have caught this regression at test time. Captured here as a lesson rather than a fix because the bug is now squashed; in a longer-lived codebase the refactor would be worth it.

---

## 10. Observability

### 10.1 Structured logging
Every module gets a named logger keyed to its component (`robot.client.{robot_id}`, `cloud_service`, `session_manager`, `zmq_peer.{name}`). Named loggers beat global ones for triage because `grep robot.client.robot-1 /tmp/jetbot-demo/robot-1.log` immediately filters to the relevant component without having to read line-by-line. When debugging multi-robot scenarios, the per-robot logger name lets you see exactly which robot did what, and when.

### 10.2 Embedded telemetry in envelopes
Every ZMQ message envelope includes `publish_ts_ns` set at publish time by the ZmqPeer wrapper, plus a `sender` field identifying the originating peer. The Player additionally preserves `source_publish_ts_ns` through its classification hop, so a processed message carries the timestamp of the sensor message that produced it. The latency benchmark uses these fields without adding benchmark-specific instrumentation — `time.time_ns() - publish_ts_ns` is a one-line latency calculation on the receiving side. Production observability would add structured telemetry on top of this, but the foundation is already there.

### 10.3 Subprocess output capture
Player stderr is re-logged with a `[player {session_id}]` prefix into the cloud's log. Demo scripts redirect cloud and robot stdout to `/tmp/jetbot-demo/cloud.log` and `/tmp/jetbot-demo/robot-{id}.log` so the demo narration stays readable but the underlying logs are tailable for debugging.

### 10.4 What I did not build (and why)
Metrics infrastructure (Prometheus, OpenTelemetry) is out of scope for the take-home but would be the natural production addition. Distributed tracing across the four processes (robot, cloud, player, user) would require trace context propagation through the signaling protocol and the ZMQ envelopes — straightforward but explicit work. Structured logs and timestamped envelopes are the closest in-spec analog: they let you reconstruct what happened after the fact, just not as efficiently as a tracing system would.

---

## 11. Latency Measurements

### 11.1 What was measured
Two paths: Robot → User direct (one ZMQ hop, sensor data flowing straight to the user), and Robot → Player → User pipelined (two ZMQ hops plus a classification step in the Player). Both measured at p50, p95, p99, and max, with effective message rate as a sanity check on throughput.

### 11.2 Methodology
The benchmark uses the existing `publish_ts_ns` envelope field set at publish time, plus `source_publish_ts_ns` that the Player preserves from the original sensor through its classification hop. This means both paths are measured against the same clock, on the same metric: when did the Robot publish the originating sensor, and when did that sensor (or its derived processed message) arrive at the User. No benchmark-specific instrumentation was added — the fields existed for production use and the benchmark just reads them.

### 11.3 Results
Sample run at 20Hz publish rate over 20 seconds: direct path p50 around 500-600µs, p95 around 800-900µs, p99 around 1ms, max around 1.5ms. Pipelined path is in the same regime — typically within 100-200µs of the direct path at p50, because the classification step is cheap and the second ZMQ hop is over IPC. Both paths are sub-millisecond at p50, which is the headline number. The pipelined path's p50 occasionally being lower than the direct path's reflects asyncio event-loop scheduling between the user's two SUB sockets — when both messages arrive together, ordering can be reversed by tens of microseconds.


### 11.4 Comparison to a broker-routed design
A hypothetical broker-routed design (Robot → Cloud → User) would add the cloud's serve-time to every message, putting throughput proportional to cloud capacity rather than peer capacity. At this project's scale that would still be sub-millisecond, but the property worth noting is the asymmetry: in the peer-to-peer design, the cloud's load is independent of message volume. Throughput scales with peer capacity, and each triangle is independent. In a broker-routed design, every additional robot multiplies cloud throughput requirements linearly.


---

## 12. Trade-offs Acknowledged

### 12.1 Replace-on-duplicate registration vs. cryptographic identity
I chose replace-on-duplicate so crashed robots can re-register without manual intervention. I rejected strict rejection because it would deadlock the restart-after-crash case. At production scale with cryptographic identities, replace-on-duplicate becomes a denial-of-service vector, and the right call inverts: strict rejection with signed identity tokens.

### 12.2 In-memory state vs. persistence
I chose in-memory state for both Registry and SessionManager because the this project's scope is one cloud process on one host, and persistence would be 80% setup overhead for 20% added correctness. I rejected Redis or Postgres backing because they add a dependency and operational concern that doesn't earn its keep here. At production scale, multi-tenant or long-running deployments would back the Registry with Redis (hot state) and Postgres (historical), so cloud restarts don't forget the world.

### 12.3 IPC transport default vs. TCP-only
I chose IPC (Unix domain sockets in `/tmp/`) as the default ZMQ transport, with TCP available via `ZMQ_TRANSPORT=tcp`. IPC is faster than TCP loopback (no IP stack), matches the spec's single-machine scope, and sidesteps platform-specific TCP binding issues I ran into on macOS during development. I rejected TCP-only because it would have made the demo unnecessarily slower with no compensating benefit. At multi-host scale, TCP becomes the right default and IPC becomes irrelevant.

### 12.4 Narrow subscriptions vs. wildcard subscriptions
I chose narrow subscriptions per the spec's topic table — Robot subscribes only to `command` and `status`, Player only to `sensor` and `status`, User to `sensor`, `processed`, and `status`. I rejected wildcard `""` subscriptions because they make intent invisible in code and create future risk: a new `robot/{id}/diagnostic` topic added later would accidentally land on every peer that uses wildcards. Narrow subscriptions are default-deny; wildcards are default-permit. At production scale, the principle stays the same.

### 12.5 Stub classifier vs. real model
I chose a threshold classifier in the Player because this project is about the system architecture, not the inference. The classifier maps `state` magnitude into normal/warning/alert bands using bounds calibrated to FakeJetBot's max_speed. I rejected building a real ML inference loop because it would be effort that doesn't change the architectural story. At production scale, the Player's structure already supports real inference — the only change is replacing `classify_state()` with a model invocation.

### 12.6 Subprocess Player vs. shared-process Player
I chose subprocess for crash isolation, resource accountability, and clean cleanup. I rejected shared-process (threads or asyncio tasks) because a Player crash in that model would corrupt cloud state. The subprocess cost (~200ms cold start) is amortized to nothing for session-scoped work but would be wrong for per-request work. At production scale, the right answer is Kubernetes-orchestrated containers — same subprocess principle, better scheduling and isolation.

### 12.7 Auto-scaling trail visualization vs. fixed-window
I chose dynamic bounds computed from the trail data plus the origin, with minimum extent and padding. I rejected fixed world bounds because they break as soon as the robot moves outside the predefined frame, producing a trail that looks empty even though data is flowing. Auto-scaling is one extra function but eliminates a whole class of "the visualization looks broken even though the data is fine" complaints. Same principle applies to chart axes, log viewers, and any UI showing time-series data with no a-priori bound on range.

### 12.8 Single-cloud-host vs. multi-region
I chose a single cloud process for the take-home, with the Registry, SessionManager, and ConnectionManager all in-process. I rejected multi-region because it would require state replication infrastructure (Raft, managed Postgres) that's overkill for the scope. At production scale with global robot fleets, regional control planes with replication become necessary, and the Registry's API was shaped to accept a backing-store swap without changing its callers.

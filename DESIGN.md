# JetBot Control Plane — Design Document

**For setup instructions, running examples, and the test/benchmark commands, see [README.md](README.md).**

## 1. Framing

### 1.1 What this system is
- One-sentence purpose: cloud-orchestrated peer-to-peer pub/sub for robots, users, and per-session inference workers.
- Modeled on GRID architecture: cloud control plane + direct peer data plane + per-robot inference workers.
- What the spec asked for and how this answers it (one paragraph, tied to spec language).

### 1.2 The three entities and the triangle
- Robot, User, Player as ZMQ pub/sub peers.
- Cloud sits outside the triangle as orchestrator.
- Why the triangle shape (vs. star, vs. bus): topic-specific producers and consumers map cleanly to direct peer links.

### 1.3 Control plane vs. data plane
- What lives in each.
- Why the cloud is in the control plane only.
- Three consequences: scalability, latency, reliability — each in one paragraph.

---

## 2. Core Design Principles

### 2.1 Single source of truth per concern
- Registry owns robot identity and status.
- SessionManager owns session lifecycle.
- ConnectionManager owns active WebSocket references.
- Why splitting these three is better than one god-object.

### 2.2 Idempotent identity assertions
- Replace-on-duplicate registration.
- Replace-on-duplicate WebSocket attachment.
- Why "last write wins" is the right call when identity is unauthenticated.
- When it would be wrong (authenticated identities) and how the design accommodates that future change.

### 2.3 Failure-oriented design
- Invariants enforced at the point of attempt (state-machine guards, not after-the-fact validation).
- Mark-offline rather than remove on disconnect (operator visibility).
- Idempotent end() — terminal transitions reachable from any state.
- Subprocess crash boundary (Player crash ≠ cloud corruption).

### 2.4 Explicit lifecycle ownership
- Session-scoped Player: spawned by cloud, dies with the session.
- Long-lived orchestrator: lives with the cloud process.
- Robot: self-managed, self-healing across cloud restarts.

---

## 3. Middleware Choice

### 3.1 Why ZeroMQ
- Peer-to-peer topology fits the spec's "three pub/sub servers" wording literally.
- No broker process to operate.
- Transport-agnostic (IPC vs TCP) behind one API.
- Topic filtering at the wire level (prefix match on first frame).

### 3.2 Alternatives considered

#### NATS
- Leaf vs. cluster mode and which would apply here.
- Why cluster mode conflicts with explicit triangle topology.
- Where NATS would win at scale (multi-region, fan-out, replay).

#### MQTT
- Broker-bridging topology mismatch (asymmetric, file-config-heavy).
- Centralized-broker assumptions vs. our peer model.
- Where MQTT wins for production fleet management.

#### ROS2 / DDS
- Decentralized shape similar to ours.
- Auto-discovery conflicts with explicit cloud-orchestrated signaling.
- DDS QoS settings overlap with what we built manually.

### 3.3 Headline trade-off
- For this scope (single host, demo-scale, take-home), ZMQ is the right tool.
- At GRID-scale (thousands of robots, federated regions, persistent fan-out), one of the others is.

---

## 4. Signaling Protocol Design

### 4.1 The three-phase handshake
- `session_start` → `peer_ready` → `session_live`.
- Each phase's responsibility (bind, advertise, connect).
- Why this is cloud-orchestrated rather than peer-discovered.

### 4.2 The slow-joiner problem and how this solves it
- ZMQ PUB drops messages sent before SUBs are subscribed.
- Three-phase handshake guarantees all SUBs are connected before any peer publishes.
- Alternative we did not use: XPUB subscription events. Why explicit signaling is cleaner here.

### 4.3 "Bind then connect" invariant
- Order matters: peer_ready arrives only after bind() succeeds.
- session_live arrives only after all three peer_ready arrived.
- Therefore connecting SUBs can use real endpoints, not promises.

### 4.4 Cloud as introducer, not broker
- Cloud sees signaling messages, never data messages.
- After session_live the cloud could crash and the triangle would keep working.

---

## 5. Session State Machine

### 5.1 The five states
- REQUESTED → SPAWNING → AWAITING_PEERS → LIVE → ENDED.
- What each state means operationally.
- What event triggers each transition.

### 5.2 Guarded transitions
- All transitions are methods on `Session`, not attribute mutation.
- `InvalidTransition` raises on illegal moves.
- Why this beats "validate at the end" or "check before mutating elsewhere."

### 5.3 end() is the universal escape hatch
- Reachable from any state, idempotent, terminal.
- Why end() is special and other transitions are not.

### 5.4 SessionManager vs. Session split
- Session is one row of state.
- SessionManager is the collection plus the create/end/wait_for_live operations.
- Reasoning: the manager handles cardinality (many sessions); the session handles its own invariants (one lifecycle).

---

## 6. Process Boundaries

### 6.1 Why Player is a subprocess (not a thread, not an asyncio task)
- Crash isolation: a Player segfault doesn't take down the cloud.
- Resource accountability: standard OS tooling sees its CPU and memory.
- Cleanup ergonomics: SIGTERM is universal; cancelling tasks is fiddly.
- Mental model: each Player is a "worker" with its own life.

### 6.2 Costs of subprocess spawn
- ~200ms cold-start time (Python import + module load).
- Acceptable for session-scoped work; wrong for per-request work.
- How we mitigate: spawned once per session, lives until session_end.

### 6.3 Stderr re-logging
- Cloud captures Player stderr with a session_id prefix.
- Why: silent Player crashes are the worst kind of debugging.

---

## 7. The Cloud's Two Jobs

### 7.1 Directory service (Registry + REST)
- What `/robots` does and why it exists.
- Why `register` and `list` are separate operations.
- Why heartbeats need a separate channel (WebSocket) from registration (REST).

### 7.2 Session orchestration (SessionOrchestrator + signaling WebSockets)
- Lifecycle: spawn Player, drive handshake, broadcast session_end, kill Player.
- Why the orchestrator owns subprocess lifetimes.
- Why the orchestrator is the only place sessions are created or ended.

### 7.3 Why both live in the same FastAPI app
- They share `ConnectionManager` and the registry-status check on session create.
- Splitting would require an inter-service protocol for what is currently a method call.
- A larger deployment would split them; the take-home doesn't need to.

---

## 8. Multi-Robot, Multi-Form-Factor

### 8.1 Multi-robot via process replication
- Same SDK, N processes, different `--id` arguments.
- Why the registry keys on `robot_id` from the start.
- Why topics are scoped `robot/{id}/...` from the start.

### 8.2 RobotDriver protocol for multi-form-factor
- The protocol mirrors the official jetbot.Robot API.
- A `PyBulletDriver` or `JetBotHardwareDriver` would slot in without touching network code.
- Why protocol injection (rather than subclassing) is the right shape here.

### 8.3 What changes at scale
- Federated brokers (NATS, Kafka) for cross-region traffic.
- Regional control planes with replication.
- Persistence layer for the registry (Redis, Postgres).
- TURN/STUN/relay for NAT-traversal in real-world deployments.
- Kubernetes/container orchestration for Player spawn.

---

## 9. Testing Strategy

### 9.1 Test pyramid
- Many fast unit tests with stubbed subprocess boundaries.
- A few integration tests at module boundaries (HTTP, WebSocket).
- One end-to-end test with a real Player subprocess.

### 9.2 Test doubles at the subprocess boundary
- `orchestrator._spawn_player` is replaced with a fake in HTTP and CLI unit tests.
- Keeps the unit suite fast (~13s for 114 tests).
- Real subprocess validation lives in one dedicated e2e test.

### 9.3 "Don't sleep, signal" for async timing
- Wait for an observable state change (motor state, message arrival).
- Never `await asyncio.sleep(N)` to wait for "things to propagate."
- Why this eliminates flakiness from event-loop pressure.

### 9.4 Test/production parity gap (lesson learned)
- The e2e test wired `set_driver(bot)` directly, mirroring the launcher by hand.
- When the launcher was missing `set_driver(bot)`, the test still passed.
- Fix in principle: shared `build_robot(args)` factory called by both launcher and test.
- General principle: tests should call the production setup path, not duplicate it.

---

## 10. Observability

### 10.1 Structured logging
- Per-component named loggers (`robot.client.{robot_id}`, `cloud_service`, `session_manager`).
- Why named loggers beat global ones for triage.

### 10.2 Embedded telemetry in envelopes
- `publish_ts_ns` set at publish, available at receive.
- `sender` field identifies the originating peer.
- Used by the latency benchmark with no additional instrumentation.

### 10.3 Subprocess output capture
- Player stderr → cloud log with `[player {session_id}]` prefix.
- Demo scripts redirect cloud + robot stdout to `/tmp/jetbot-demo/`.

### 10.4 What we did not build (and why)
- Metrics: out of scope; production would add Prometheus or OpenTelemetry.
- Distributed tracing: same reason; structured logs and timestamped envelopes are the closest in-spec analog.

---

## 11. Latency Measurements

### 11.1 What was measured
- Robot → User direct path (one ZMQ hop).
- Robot → Player → User pipelined path (two ZMQ hops + classification).

### 11.2 Methodology
- Existing `publish_ts_ns` envelope field, no benchmark-specific instrumentation.
- Player preserves `source_publish_ts_ns` through the classification hop.
- Apples-to-apples comparison on the same clock.

### 11.3 Results
- Numbers from a representative run.
- What the p50/p95/p99 values say about the design.

### 11.4 Comparison to a broker-routed design
- Why those numbers would be qualitatively different.
- The cloud's load is independent of message volume in this design.

---

## 12. Trade-offs Acknowledged

For each: what we chose, what we rejected, why this trade-off is right for the take-home scope but would invert at production scale.

### 12.1 Replace-on-duplicate registration vs. cryptographic identity
### 12.2 In-memory state vs. persistence
### 12.3 IPC transport default vs. TCP-only
### 12.4 Narrow subscriptions vs. wildcard subscriptions
### 12.5 Stub classifier vs. real model
### 12.6 Subprocess Player vs. shared-process Player
### 12.7 Auto-scaling trail visualization vs. fixed-window
### 12.8 Single-cloud-host vs. multi-region
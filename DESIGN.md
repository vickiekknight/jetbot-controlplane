# <Project Name>

## Design Framing

### Architectural Pattern
- Cloud-orchestrated peer-to-peer data plane
- Three-entity ZMQ triangle:
  - Robot
  - User
  - Player
- Cloud exists outside the triangle as the control plane orchestrator
- Why this architecture fits the take-home requirements
- What responsibilities belong to:
  - Control plane
  - Data plane

### Middleware Choice
#### Why ZeroMQ
- PUB/SUB topology fit
- Minimal moving parts
- No separate broker
- Topic filtering
- Deterministic peer relationships
- Simplicity for single-machine / LAN demo

#### Alternatives Considered
##### NATS
- Leaf-node mode
- Cluster mode
- Why cluster mode conflicts with “three peer brokers”
- Operational advantages at scale
- Why NATS would make sense at GRID-scale

##### MQTT
- Broker bridging topology mismatch
- Static bridge configuration problem
- Centralized-broker assumptions
- Why MQTT is stronger for production fleet management

##### ROS2 / DDS
- Similar decentralized shape
- Automatic discovery vs orchestrated signaling
- Why DDS conflicts with explicit session coordination

### What Changes at Scale
- Moving from peer mesh → federated brokers
- NATS / Kafka / MQTT at scale
- Regional brokers
- Dedicated control plane
- Persistence
- Fan-out
- Replay
- Multi-region routing
- TURN/STUN/relay infrastructure
- Kubernetes / container orchestration

### Extension Points
- RobotDriver abstraction
- Multiple robot types
- Real hardware vs simulation
- Multi-skill Players
- LLM command parser
- Benchmarking hooks
- Future persistence layers

---

# System Overview

## High-Level Architecture

### Components
- Cloud Service
- Robot
- User CLI
- Player
- Shared common/ infrastructure modules

### Data Flow
- Sensor data
- Commands
- Processed messages

### Control Flow
- Registration
- Heartbeats
- Session establishment
- Session teardown

### Why the Cloud Lives Outside the Triangle
- Cloud as orchestrator only
- Separation of identity from communication
- Separation of session coordination from data transport

---

# Core Design Principles

## Control Plane vs Data Plane Separation
- Cloud only orchestrates
- Data flows directly between peers
- Scalability benefits
- Reliability benefits
- Latency benefits

## Failure-Oriented Design
- Making wrong behavior difficult
- Defensive architecture choices
- Non-functional guarantees

## Idempotency
- Replace-on-duplicate registration
- Replace-on-duplicate WebSocket attachment
- Crash recovery behavior

## Explicit Lifecycle Ownership
- Session-scoped Player lifecycle
- Long-lived orchestrator lifecycle
- Session teardown semantics

---

# Repository Structure

## Directory Layout

```text
common/
cloud_service/
robot/
player/
user/
tests/
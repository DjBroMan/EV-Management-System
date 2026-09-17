# Distributed EV Charging Network Management System

A fully-distributed, fault-tolerant **Electric Vehicle Charging Network** built with **Java RMI**, deployed via **Docker Compose**, and incorporating every major distributed-systems concept: Lamport logical clocks, Cristian's physical clock synchronisation, the Bully leader-election algorithm, primary-backup replication, automated failover, load balancing, and MySQL persistence — all running across **32 Docker containers**.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Technology Stack](#2-technology-stack)
3. [Architecture](#3-architecture)
4. [Module / Service Breakdown](#4-module--service-breakdown)
5. [Distributed Systems Concepts Implemented](#5-distributed-systems-concepts-implemented)
   - [Java RMI Communication](#51-java-rmi-communication)
   - [Lamport Logical Clocks](#52-lamport-logical-clocks)
   - [Cristian's Physical Clock Synchronisation](#53-cristians-physical-clock-synchronisation)
   - [Bully Leader-Election Algorithm](#54-bully-leader-election-algorithm)
   - [Primary-Backup Replication](#55-primary-backup-replication)
   - [Automated Failover](#56-automated-failover)
   - [Load Balancing](#57-load-balancing)
6. [End-to-End Business Workflow](#6-end-to-end-business-workflow)
7. [Database Schema](#7-database-schema)
8. [Port and Container Map](#8-port-and-container-map)
9. [Project Structure](#9-project-structure)
10. [Quick Start — Docker (Recommended)](#10-quick-start--docker-recommended)
11. [Quick Start — Manual Multi-JVM](#11-quick-start--manual-multi-jvm)
12. [Running the Clients](#12-running-the-clients)
13. [Testing](#13-testing)
14. [Failover Demo](#14-failover-demo)
15. [Energy and Pricing Formulas](#15-energy-and-pricing-formulas)
16. [Documentation Index](#16-documentation-index)

---

## 1. Project Overview

This project simulates a real-world EV charging network for a Distributed Computing (DC) university course. It is designed to demonstrate that a distributed system can:

- **Remain available** when individual servers fail (fault tolerance via Bully election + failover).
- **Stay consistent** across replicas (synchronous primary-backup replication).
- **Scale reads** by distributing them across healthy cluster instances (round-robin load balancing).
- **Order events causally** across independent servers that have no shared clock (Lamport logical clocks).
- **Agree on physical time** despite container-level clock skew injected by `libfaketime` (Cristian's algorithm).

A single interactive console client (`EVClient`) drives all 5 microservices through the complete EV lifecycle — reserve a slot, start charging, stop charging, calculate bill, and pay — while the infrastructure handles all distributed concerns transparently.

---

## 2. Technology Stack

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Communication | Java RMI (Remote Method Invocation) |
| Containerisation | Docker + Docker Compose |
| Database | MySQL 8.0 (one instance per cluster instance, 15 total) |
| JDBC Driver | MySQL Connector/J 8.0.33 |
| Clock skew simulation | `libfaketime` (Linux `LD_PRELOAD`) |
| Build | `javac` (inside Dockerfile, no Maven/Gradle) |
| Base image | `eclipse-temurin:17-jdk` |

---

## 3. Architecture

### High-Level View

```
                    +---------------------------------------+
                    |         EV CLIENT (console)          |
                    |           EVClient.java              |
                    +------------------+--------------------+
                                       |  RMI (all 5 services)
                                       v
                    +---------------------------------------+
                    |          MANAGER  (:1240)             |
                    |   ReservationServerManager.java       |
                    |  * Single entry point for EVClient    |
                    |  * Round-robin load balancing (reads) |
                    |  * Leader-only routing (writes)       |
                    |  * Background health monitor          |
                    |  * Replication fan-out                |
                    +---+----+----+----+-------------------+
                        |    |    |    |
     +------------------+    |    |    +-------------------+
     v                       v    v                        v
ChargingStation        Reservation  ChargingSession       Payment
Cluster (x3)           Cluster (x3) Cluster (x3)          Cluster (x3)
CS1/CS2/CS3            R1/R2/R3     S1/S2/S3            Pay1/Pay2/Pay3
:1234/:1244/:1254    :1235/:1245/:1255  :1236/:1246/:1256  :1237/:1247/:1257
     |                       |              |                  |
     +------- Pricing Cluster (x3) ---------+------------------+
                     P1/P2/P3
                :1238/:1248/:1258

               TimeServer (:1239/2239)
     All app servers sync against it via Cristian's Algorithm
```

### Key Design Decisions

| Concern | Decision | Why |
|---|---|---|
| Client entry point | Single Manager proxy | Clients never need to know which instance is the current leader |
| Leader election | Bully algorithm, per-cluster | Simple, correct, independently scoped per service |
| Write routing | Leader-only (`pickForWrite`) | Prevents split-brain on state-mutating operations |
| Read routing | Round-robin (`pickForRead`) | Distributes load; all replicas hold the same state |
| Replication | Synchronous fan-out before ACK | Strong consistency — secondary is current before client is told "success" |
| Time | Dual-clock (Lamport + Cristian) | Physical clock for duration math; Lamport for causal event ordering |
| Persistence | MySQL per instance | State survives process restarts without full snapshot transfer |
| Failure window | Heartbeat 3s, health poll 4s | Leader failure detected and rerouted within ~12s end-to-end |

---

## 4. Module / Service Breakdown

### `Clock/` — Distributed Clock Utilities

| File | Purpose |
|---|---|
| `LogicalClock.java` | Thread-safe Lamport logical clock using `AtomicLong` CAS — zero lock contention |
| `PhysicalClock.java` | Wraps `System.currentTimeMillis()` with a Cristian-corrected offset for wall-clock timestamps |
| `CristianClient.java` | Implements Cristian's algorithm: records T0, calls `TimeServer.getPhysicalTimeMillis()`, records T1, computes offset = `serverTime + (RTT/2) - T1` |
| `TimeServer.java` | RMI server that returns its own `System.currentTimeMillis()` — the authoritative reference clock |
| `TimeServerInterface.java` | RMI remote interface for the time server |
| `LamportResult.java` | Generic wrapper `LamportResult<T>` — every RMI return value carries `(data, lamportTimestamp)` |
| `DistributedLogger.java` | Formats log lines with both physical and Lamport timestamps |

### `Common/` — Shared Distributed Infrastructure

| File | Purpose |
|---|---|
| `BullyElection.java` | Self-contained Bully algorithm engine. One instance per server process. Manages heartbeat monitoring, election initiation, OK/COORDINATOR message handling, and coordinator broadcasting |
| `ClusterNodeInterface.java` | Shared RMI interface implemented by every cluster instance: Bully messages (`receiveElection`, `receiveOk`, `receiveCoordinator`), health (`ping`, `getRole`, `getServerId`), and replication (`applyUpdate`, `getClusterSnapshot`, `applyClusterSnapshot`) |
| `ClusterManagerInterface.java` | RMI interface the Manager exposes to cluster PRIMARYs for replication fan-out |
| `ClusterManagerClient.java` | Helper that PRIMARYs call after a local write to push a `StateDelta` to the Manager |
| `ServerIdentity.java` | Parses `SERVER_ID`, `PEERS`, `RMI_REGISTRY_PORT`, `RMI_EXPORT_PORT` from env vars so one compiled binary serves any cluster instance |
| `PeerHandle.java` | Value object: `{id, host, registryPort}` — the address of one cluster peer |
| `StateDelta.java` | Generic `(opType, args...)` replication payload covering all 5 clusters without bespoke DTOs |
| `GenericSnapshot.java` | `Map<String, Serializable>` used for full-state transfer (disaster recovery only) |
| `NetworkSetup.java` | Installs a custom `RMISocketFactory` with a 2-second TCP connect timeout to prevent hung connections to stopped containers |
| `ManagerRouting.java` | Cluster routing utilities used by the Manager |

### `ChargingStation/` — Physical Port Management

| File | Purpose |
|---|---|
| `ChargingStationServer.java` | Manages ports P1-P4 and their states (AVAILABLE/RESERVED/CHARGING). Participates in Bully election. Replicates every state change via `ClusterManagerClient` |
| `ChargingStationInterface.java` | RMI interface: `getStationStatus`, `getAvailablePorts`, `checkPortAvailability`, `reservePort`, `reserveAnyAvailablePort`, `startPortCharging`, `releasePort` |
| `ChargingStationDAO.java` | JDBC persistence for the `charging_ports` table |

### `Reservation/` — Slot Booking

| File | Purpose |
|---|---|
| `ReservationServer.java` | Implements both `ReservationInterface` and `ClusterNodeInterface`. On write: reserves a port from `ChargingStation`, persists to DB, replicates via Manager. Supports PRIMARY/SECONDARY roles |
| `ReservationServerManager.java` | **The Manager** — single RMI entry point for all 5 services. Proxies all client calls, implements load balancing, replication fan-out, health monitoring, and leader discovery |
| `ReservationInterface.java` | `reserveSlot`, `getReservation`, `cancelReservation`, `getReservationPort` |
| `ReservationReplicationInterface.java` | Original 2-node replication interface (preserved for backward compatibility with `ReplicationTest`) |
| `ReservationManagerInterface.java` | Interface the PRIMARY uses to ask the Manager to fan out a replication |
| `ReservationDAO.java` | JDBC persistence for the `reservations` table |
| `ReservationStateSnapshot.java` | Serializable snapshot for full state transfer |

### `ChargingSession/` — Session Tracking and Energy Calculation

| File | Purpose |
|---|---|
| `ChargingSessionServer.java` | Records `T_start = Instant.now()` on `startCharging`, `T_end = Instant.now()` on `stopCharging`, computes `Energy = 7.2 kW x (duration_seconds / 3600)`. Participates in Bully election |
| `ChargingSessionInterface.java` | `startCharging`, `stopCharging`, `getSessionStatus`, `getEnergyConsumed`, `getSessionPort` |
| `ChargingSessionDAO.java` | JDBC persistence for the `charging_sessions` table (millisecond-precision `TIMESTAMP(3)`) |

### `Pricing/` — Bill Calculation

| File | Purpose |
|---|---|
| `PricingServer.java` | Read-only service. Seeds tariff data from DB at startup. Computes `Bill = base_price x energy_kWh x demand_multiplier`. Every instance is an independent read replica (no write path, no election needed) |
| `PricingInterface.java` | `calculatePrice(stationId, energyKwh, lamport)`, `getDemandMultiplier` |
| `PricingDAO.java` | JDBC persistence for the `station_pricing_tariffs` table |

### `Payment/` — Payment Settlement

| File | Purpose |
|---|---|
| `PaymentServer.java` | On `makePayment`: fetches energy from `ChargingSessionServer`, price from `PricingServer`, records payment, then releases the port via `ChargingStationServer`. Participates in Bully election |
| `PaymentInterface.java` | `makePayment`, `getPaymentStatus`, `getPaymentDetails` |
| `PaymentDAO.java` | JDBC persistence for the `payments` table |

### Root-level Files

| File | Purpose |
|---|---|
| `EVClient.java` | Interactive console client. Maintains its own Lamport clock. Talks exclusively to the Manager. All 14 menu operations (reserve → charge → pay) are implemented here |
| `MultithreadTest.java` | Simulates 10 concurrent EVs making reservations, charging, and paying simultaneously — validates thread safety and Lamport clock propagation |
| `ReplicationTest.java` | Automated 8-scenario regression suite for the primary-backup Reservation replication path |
| `DBConnectionHelper.java` | Shared JDBC factory: reads `DB_HOST/PORT/NAME/USER/PASSWORD` from env vars. Retries up to 15x on startup. Degrades gracefully to pure in-memory if DB is absent |
| `Dockerfile` | Builds a single image for all app servers: installs `libfaketime` + MySQL Connector, compiles all Java, sets `CLASSPATH` |
| `docker-compose.yml` | Defines all 32 services (1 TimeServer + 15 MySQL + 15 app instances + 1 Manager) |

### `tests/` — Automated Integration Tests

| File | Tests |
|---|---|
| `BullyElectionTest.java` | Confirms correct ELECTION → OK → COORDINATOR message flow |
| `LoadBalancingTest.java` | Verifies reads round-robin and writes go leader-only |
| `HealthCheckTest.java` | Validates the Manager's background health-monitor tick |
| `CombinedFailoverLoadTest.java` | Fires concurrent requests while killing the leader; asserts brief bounded failure window |

---

## 5. Distributed Systems Concepts Implemented

### 5.1 Java RMI Communication

All inter-service communication uses **Java RMI** (Remote Method Invocation). Every service defines a `Remote` interface and a server class that extends `UnicastRemoteObject`. The Manager binds proxy stubs into its own RMI registry at port 1240, so the client only ever looks up one host/port regardless of how many backend instances exist.

Every RMI return value is wrapped in `LamportResult<T>` — a generic pair of `(data, lamportTimestamp)` — ensuring the Lamport clock advances on every hop without needing out-of-band channels.

### 5.2 Lamport Logical Clocks

Implemented in `Clock/LogicalClock.java` using a lock-free `AtomicLong` compare-and-swap loop.

**Three rules, applied on every RMI call:**

| Event | Rule | Implementation |
|---|---|---|
| Local event | `L = L + 1` | `logicalClock.tick()` |
| Send (outgoing RMI) | `L = L + 1` | `logicalClock.sendEvent()` — called before every outbound RMI call |
| Receive (incoming RMI) | `L = max(L_local, L_received) + 1` | `logicalClock.receiveEvent(clientLamport)` — first line of every RMI method body |

The client's Lamport value is visible in the menu header: `[Physical=2026-08-26 15:32:01 | Lamport=7]`.

Lamport time is used **exclusively for event ordering / causality** — never for duration calculations or database timestamps.

### 5.3 Cristian's Physical Clock Synchronisation

Each app server calls `Clock/CristianClient.java` on startup to synchronise its local clock with the reference `TimeServer`:

```
1.  T0 = System.currentTimeMillis()                 <- record local time before RPC
2.  serverTime = TimeServer.getPhysicalTimeMillis()  <- RMI call to TimeServer
3.  T1 = System.currentTimeMillis()                 <- record local time after RPC
4.  RTT = T1 - T0
5.  estimatedServerTime = serverTime + (RTT / 2)
6.  offset = estimatedServerTime - T1
7.  PhysicalClock.setClockOffsetMs(offset)           <- stored globally
```

After sync, every call to `PhysicalClock.getSynchronizedPhysicalTimeMillis()` returns `System.currentTimeMillis() + offset`. This corrected time is what gets written into every MySQL `TIMESTAMP` column — MySQL's `CURRENT_TIMESTAMP` is never used.

`libfaketime` is loaded via `LD_PRELOAD` in each Docker container with a distinct `FAKETIME=@YYYY-MM-DD HH:MM:SS` to simulate realistic clock skew between containers.

### 5.4 Bully Leader-Election Algorithm

Implemented generically in `Common/BullyElection.java` and instantiated **once per server process**. Each of the 5 clusters (15 instances total) runs a completely independent election — Reservation's R1/R2/R3 election never communicates with ChargingStation's CS1/CS2/CS3 election.

**Algorithm Steps:**

```
1. Heartbeat monitor fires every 3 seconds — pings the known coordinator.
2. After 2 consecutive missed pings → call startElection().
3. Send ELECTION to every peer with a HIGHER SERVER_ID.
4. Wait up to 2 seconds for an OK reply:
   a. No OK received → declareVictory() → become PRIMARY → broadcast COORDINATOR.
   b. OK received → wait up to 3s for a COORDINATOR announcement from that peer.
      If announcement never comes → restart election.
5. A node receiving ELECTION from a lower ID → reply OK, start its own election.
6. A node receiving COORDINATOR → demote self to SECONDARY, adopt the winner as leader.
```

**Reliability fixes discovered during Docker testing:**
- `lookupPeerWithRetry` for `COORDINATOR` and `ELECTION` messages (not heartbeats) so slow-starting peers don't silently miss one-shot messages.
- `NetworkSetup.installBoundedConnectTimeout()` caps TCP connect at 2 seconds to avoid multi-minute hangs against stopped (but not removed) containers.

**End-to-end verified live:** killing `reservation-3` caused R2 to be elected and the Manager to re-route within ~12 seconds.

### 5.5 Primary-Backup Replication

Two mechanisms coexist (only one active per cluster at a time):

**Original 2-node Reservation path (preserved for backward compatibility):**

```
Client → Manager → PRIMARY (R3)
                       |  Calls ChargingStation, updates local state + DB
                       |  Calls Manager.replicateReservation(...)
                       v
              Manager → SECONDARY (R1)
                           |  applyReservationUpdate → updates state + own DB
```

**Generalized N-instance path (used by all 5 clusters in Docker):**

```
PRIMARY applies write locally + persists to own DB
    |
    v  ClusterManagerClient.replicate(serviceName, originId, StateDelta, clock)
Manager.replicateUpdate(...)
    |
    +-> peer-1.applyUpdate(delta) → peer-1's own DB
    +-> peer-2.applyUpdate(delta) → peer-2's own DB
    (origin is excluded from fan-out to avoid self-deadlock)
```

`StateDelta` is a generic `(opType, args...)` payload: `PORT_STATUS`, `SESSION_START`, `SESSION_STOP`, `PAYMENT_INSERT`, `RESERVATION_UPSERT`, `RESERVATION_DELETE`. One wire format covers every cluster.

Pricing has **no write replication** — it's a read-only service where all instances independently load state from their own DB at startup.

### 5.6 Automated Failover

Failover flows naturally from the Bully election:

```
1. DETECT   — BullyElection.heartbeatTick() misses 2 pings → startElection()
2. ELECT    — Bully exchange: ELECTION → OK → COORDINATOR
3. PROMOTE  — Winner's onBecomeCoordinator callback flips role to PRIMARY in-process
4. DISCOVER — Manager's background healthAndLeaderDiscoveryTick() (every 4s) polls
              getRole() on all instances → sees new PRIMARY → updates currentLeader
5. REROUTE  — Very next write to that cluster uses pickForWrite(cluster) → new leader
6. CONTINUE — No state reconstruction needed: new leader's state was already current
              because writes were synchronously replicated before ACK
```

**Failback semantics (standard Bully):** A restarted highest-ID node reclaims leadership unconditionally — it broadcasts `COORDINATOR` to all peers without checking whether a leader is already active.

### 5.7 Load Balancing

Implemented entirely in `ReservationServerManager.java`. EVClient never addresses individual cluster instances directly.

| Operation type | Strategy | Method |
|---|---|---|
| Read-only operations (`getStationStatus`, `getSessionStatus`, `calculatePrice`, `getReservation`, ...) | Round-robin across healthy instances | `pickForRead(cluster)` — cycles `AtomicInteger` over `ClusterConfig.healthy` |
| Write / state-mutating operations (`reserveSlot`, `startCharging`, `makePayment`, `releasePort`, ...) | Leader-only | `pickForWrite(cluster)` — always returns `ClusterConfig.currentLeader` |
| Pricing operations | Always round-robin | No write path exists; every Pricing instance is a valid read replica |

The `healthy` map is updated by the same background thread that discovers the current leader, so a failed instance is removed from the read pool within one health-poll cycle (≤4 seconds).

---

## 6. End-to-End Business Workflow

A single EV charging lifecycle involves all 5 microservices in strict sequence:

```
Step 1: RESERVE SLOT
  EVClient → Manager → ReservationServer (PRIMARY)
    ├── ChargingStation: reserveAnyAvailablePort() → P1 "RESERVED"
    ├── Local state: reservations["RES1001"] = {userId, vehicleId, portId}
    ├── DB: INSERT INTO reservations ...
    ├── Manager: replicateReservation(RES1001, ...) → SECONDARY
    └── Client: "Reservation successful! Reservation ID: RES1001"

Step 2: START CHARGING
  EVClient → Manager → ChargingSessionServer (leader)
    ├── Validates reservation RES1001 exists
    ├── ChargingStation: startPortCharging("P1") → P1 "CHARGING"
    ├── T_start = Instant.now()  (Cristian-corrected physical clock)
    ├── Creates SESSION-1001 with chargingPower=7.2 kW
    └── Client: "Charging started! Session ID: SESSION-1001"

Step 3: STOP CHARGING & CALCULATE ENERGY
  EVClient → Manager → ChargingSessionServer (leader)
    ├── T_end = Instant.now()
    ├── duration_s = (T_end - T_start).toMillis() / 1000.0
    ├── energy_kWh = 7.2 kW × (duration_s / 3600.0)
    ├── Session status → "COMPLETED"; energy persisted to DB
    └── Lamport event logged: [Event=LOCAL] energy calculation

Step 4: CALCULATE BILL (client-side menu option)
  EVClient → ChargingSession: getEnergyConsumed(SESSION-1001)
  EVClient → Pricing: calculatePrice("S01", energyKwh)
    ├── bill = Rs.10.0/kWh × energy_kWh × demand_multiplier
    └── Client: "Estimated Bill: Rs. 14.40"

Step 5: MAKE PAYMENT & RELEASE PORT
  EVClient → Manager → PaymentServer (leader)
    ├── ChargingSession: getEnergyConsumed(SESSION-1001)
    ├── Pricing: calculatePrice("S01", energy)
    ├── DB: INSERT INTO payments (PAY-1001, ...)
    ├── ChargingStation: releasePort("P1") → P1 "AVAILABLE"
    └── Client: "Payment successful! Payment ID: PAY-1001, Amount: Rs. 14.40"
```

**Lamport clock propagates across every hop:** the client increments before every outbound call (`sendEvent`), each server applies `receiveEvent(clientLamport)` as its very first action, then increments before its own outbound calls to peer services. Every log line prints both `[Physical=...]` and `[Lamport=...]`.

---

## 7. Database Schema

Each of the 15 cluster instances connects to its own dedicated MySQL 8.0 container. There are **no cross-database foreign keys** — references between services are resolved via RMI calls, not SQL joins.

| Table | Cluster | Key Columns |
|---|---|---|
| `charging_ports` | ChargingStation (x3) | `port_id`, `station_id`, `status`, `last_updated` |
| `reservations` | Reservation (x3) | `reservation_id`, `user_id`, `vehicle_id`, `port_id`, `status`, `created_at` |
| `charging_sessions` | ChargingSession (x3) | `session_id`, `reservation_id`, `port_id`, `status`, `start_time`, `end_time` (ms precision), `charging_power_kw`, `energy_consumed_kwh` |
| `station_pricing_tariffs` | Pricing (x3) | `station_id`, `base_price_per_kwh`, `demand_level`, `demand_multiplier` |
| `payments` | Payment (x3) | `payment_id`, `session_id`, `energy_consumed_kwh`, `total_amount`, `payment_status`, `payment_time` |

Servers self-seed initial data at startup (`ChargingStation` seeds P1-P4; `Pricing` seeds the S01 tariff). SQL init scripts in `db/init/` only create the schema — they never INSERT business data.

All `TIMESTAMP` columns are written using `PhysicalClock.getSynchronizedPhysicalTimeMillis()` (Cristian-corrected), never MySQL `CURRENT_TIMESTAMP`. See `docs/DATABASE_SCHEMA.md` for the full DDL.

---

## 8. Port and Container Map

### App Servers

| Service | Instance | Registry Port | Export Port | Docker Container |
|---|---|---|---|---|
| TimeServer | — | 1239 | 2239 | `time-server` |
| Manager | — | 1240 | 2240 | `manager` |
| ChargingStation | CS1 | 1234 | 2234 | `charging-station-1` |
| ChargingStation | CS2 | 1244 | 2244 | `charging-station-2` |
| ChargingStation | CS3 | 1254 | 2254 | `charging-station-3` |
| Reservation | R3 (initial primary) | 1235 | 2235 | `reservation-3` |
| Reservation | R1 (secondary) | 1245 | 2245 | `reservation-1` |
| Reservation | R2 (secondary) | 1255 | 2255 | `reservation-2` |
| ChargingSession | S1 | 1236 | 2236 | `charging-session-1` |
| ChargingSession | S2 | 1246 | 2246 | `charging-session-2` |
| ChargingSession | S3 | 1256 | 2256 | `charging-session-3` |
| Pricing | P1 | 1238 | 2238 | `pricing-1` |
| Pricing | P2 | 1248 | 2248 | `pricing-2` |
| Pricing | P3 | 1258 | 2258 | `pricing-3` |
| Payment | Pay1 | 1237 | 2237 | `payment-1` |
| Payment | Pay2 | 1247 | 2247 | `payment-2` |
| Payment | Pay3 | 1257 | 2257 | `payment-3` |

### MySQL Containers (host-exposed ports for inspection)

| Container | Host Port | Database |
|---|---|---|
| `mysql-reservation-3` | 3307 | `ev_reservation_primary_db` |
| `mysql-reservation-1` | 3308 | `ev_reservation_secondary_db` |
| `mysql-reservation-2` | 3309 | `ev_reservation_tertiary_db` |
| All others | not exposed | see `docker-compose.yml` |

---

## 9. Project Structure

```
Java/
├── Clock/                          # Clock utilities (Lamport, Cristian, TimeServer)
│   ├── LogicalClock.java
│   ├── PhysicalClock.java
│   ├── CristianClient.java
│   ├── TimeServer.java
│   ├── TimeServerInterface.java
│   ├── LamportResult.java
│   └── DistributedLogger.java
├── Common/                         # Shared distributed infrastructure
│   ├── BullyElection.java
│   ├── ClusterNodeInterface.java
│   ├── ClusterManagerInterface.java
│   ├── ClusterManagerClient.java
│   ├── ServerIdentity.java
│   ├── PeerHandle.java
│   ├── StateDelta.java
│   ├── GenericSnapshot.java
│   ├── NetworkSetup.java
│   └── ManagerRouting.java
├── ChargingStation/                # Port management cluster
│   ├── ChargingStationServer.java
│   ├── ChargingStationInterface.java
│   └── ChargingStationDAO.java
├── Reservation/                    # Slot booking cluster + Manager
│   ├── ReservationServer.java
│   ├── ReservationServerManager.java  <- THE MANAGER
│   ├── ReservationInterface.java
│   ├── ReservationReplicationInterface.java
│   ├── ReservationManagerInterface.java
│   ├── ReservationDAO.java
│   └── ReservationStateSnapshot.java
├── ChargingSession/                # Session & energy calculation cluster
│   ├── ChargingSessionServer.java
│   ├── ChargingSessionInterface.java
│   └── ChargingSessionDAO.java
├── Pricing/                        # Pricing calculation cluster (read-only)
│   ├── PricingServer.java
│   ├── PricingInterface.java
│   └── PricingDAO.java
├── Payment/                        # Payment settlement cluster
│   ├── PaymentServer.java
│   ├── PaymentInterface.java
│   └── PaymentDAO.java
├── tests/                          # Automated integration tests
│   ├── BullyElectionTest.java
│   ├── LoadBalancingTest.java
│   ├── HealthCheckTest.java
│   └── CombinedFailoverLoadTest.java
├── docs/                           # Extended documentation (17 documents)
├── db/init/                        # MySQL schema init scripts (01-15_*.sql)
├── EVClient.java                   # Interactive console client
├── MultithreadTest.java            # Concurrent workflow test (10 EVs)
├── ReplicationTest.java            # 8-scenario replication regression test
├── DBConnectionHelper.java         # Shared JDBC connection factory
├── Dockerfile                      # Single multi-role Docker image
└── docker-compose.yml              # Full 32-service deployment
```

---

## 10. Quick Start — Docker (Recommended)

**Prerequisites:** Docker Desktop (Compose v2), 4+ GB RAM allocated.

```powershell
# Navigate to the project directory
cd "C:\Users\asus\Desktop\College\Sem 5\DC\Java"

# Build and start all 32 containers in the background
docker compose up -d

# Watch startup progress (MySQL containers must reach "healthy" first, ~30-90s)
docker compose ps

# Tail the Manager's log to see health-monitor and leader-discovery output
docker compose logs -f manager

# Tail a specific cluster's logs
docker compose logs -f reservation-3 reservation-1 reservation-2
```

Wait until all `mysql-*` containers show `(healthy)` before expecting the app servers to finish connecting.

---

## 11. Quick Start — Manual Multi-JVM

Use this if Docker is unavailable. Requires Java 17+. All servers degrade gracefully to in-memory mode if MySQL is absent.

```powershell
# 1. Download MySQL connector
if (-not (Test-Path lib)) { New-Item -ItemType Directory lib | Out-Null }
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" `
  -OutFile "lib\mysql-connector-j-8.0.33.jar"

# 2. Compile everything
if (Test-Path bin) { Remove-Item -Recurse -Force bin }
New-Item -ItemType Directory bin | Out-Null
javac -cp "lib\mysql-connector-j-8.0.33.jar" -d bin `
  Clock\*.java Common\*.java `
  ChargingStation\*.java Reservation\*.java ChargingSession\*.java Pricing\*.java Payment\*.java `
  DBConnectionHelper.java EVClient.java MultithreadTest.java ReplicationTest.java tests\*.java

$CP = "bin;lib\mysql-connector-j-8.0.33.jar"

# 3. Time server
Start-Process java -ArgumentList "-cp",$CP,"Clock.TimeServer"
Start-Sleep -Seconds 2

# 4. ChargingStation cluster (3 instances)
$env:SERVER_ID="1"; $env:RMI_REGISTRY_PORT="1234"; $env:RMI_EXPORT_PORT="2234"
$env:PEERS="2:localhost:1244,3:localhost:1254"
Start-Process java -ArgumentList "-cp",$CP,"ChargingStationServer"

$env:SERVER_ID="2"; $env:RMI_REGISTRY_PORT="1244"; $env:RMI_EXPORT_PORT="2244"
$env:PEERS="1:localhost:1234,3:localhost:1254"
Start-Process java -ArgumentList "-cp",$CP,"ChargingStationServer"

$env:SERVER_ID="3"; $env:RMI_REGISTRY_PORT="1254"; $env:RMI_EXPORT_PORT="2254"
$env:PEERS="1:localhost:1234,2:localhost:1244"
Start-Process java -ArgumentList "-cp",$CP,"ChargingStationServer"
Start-Sleep -Seconds 4

# 5. Manager
$env:STATION_INSTANCES="1:localhost:1234,2:localhost:1244,3:localhost:1254"
$env:SESSION_INSTANCES="1:localhost:1236,2:localhost:1246,3:localhost:1256"
$env:PRICING_INSTANCES="1:localhost:1238,2:localhost:1248,3:localhost:1258"
$env:PAYMENT_INSTANCES="1:localhost:1237,2:localhost:1247,3:localhost:1257"
$env:RESERVATION_INSTANCES="3:localhost:1235,1:localhost:1245,2:localhost:1255"
$env:PRIMARY_HOST="localhost"; $env:SECONDARY_HOST="localhost"; $env:MANAGER_PORT="1240"
Start-Process java -ArgumentList "-cp",$CP,"ReservationServerManager"
Start-Sleep -Seconds 2

# 6. Reservation, ChargingSession, Pricing, and Payment clusters use the same pattern.
# See docs/MANUAL_DEMONSTRATION.md for the complete 17-process startup script.
```

---

## 12. Running the Clients

### Interactive Client (EVClient)

```powershell
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" EVClient
```

The client prompts for `User ID` and `Vehicle ID`, then shows a 14-option menu:

```
1.  View Station Status         8.  Check Charging Session
2.  View Available Ports        9.  Stop Charging
3.  Check Port Availability    10.  Calculate Bill
4.  Reserve Charging Slot      11.  Make Payment
5.  Check Reservation          12.  Check Payment Status
6.  Cancel Reservation         13.  View Payment Details
7.  Start Charging             14.  Exit
```

Previously-returned IDs (Reservation ID, Session ID, Payment ID) are remembered and offered as defaults for subsequent operations.

### Multithreaded Test (10 concurrent EVs)

```powershell
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" MultithreadTest
```

### Replication Test (8 scenarios)

```powershell
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" ReplicationTest
```

---

## 13. Testing

| Test Class | What it verifies |
|---|---|
| `ReplicationTest` | 8 scenarios: startup binding, single reservation replication, multi-reservation consistency, cancellation replication, concurrent multithreaded load, full state sync, primary failure + failover promotion, post-failover write continuity |
| `MultithreadTest` | 10 concurrent EVs complete the full reserve→charge→stop→pay cycle; validates thread safety, energy calculation, Lamport propagation, port release |
| `tests/BullyElectionTest` | ELECTION → OK → COORDINATOR message exchange correctness |
| `tests/LoadBalancingTest` | Confirms reads are distributed round-robin and writes always go to the current leader |
| `tests/HealthCheckTest` | Validates the Manager's background health monitor discovers leader changes |
| `tests/CombinedFailoverLoadTest` | Fires continuous concurrent requests while killing the leader; asserts bounded failure window followed by 100% success |

**All tests target the Manager's port (1240)** — they never address individual cluster instances directly.

---

## 14. Failover Demo

```powershell
# 1. Kill the current Reservation leader (R3 is the initial primary)
docker compose stop reservation-3

# 2. Watch the election in the remaining nodes' logs (~12 seconds total)
docker compose logs -f reservation-1 reservation-2 manager

# Expected log sequence:
#   reservation-1: "Heartbeat to coordinator (3) failed 1x"
#   reservation-1: "Heartbeat to coordinator (3) failed 2x -- starting election"
#   reservation-2: "No higher-ID peer responded. Reservation-2 ELECTED as new coordinator."
#   reservation-2: "This instance is now PRIMARY of the Reservation cluster."
#   manager:       "New leader for Reservation cluster = Reservation-2"

# 3. Confirm the system still works -- EVClient reserveSlot should succeed via R2
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" EVClient
# Option 4 (Reserve Charging Slot) succeeds, counter continues from where R3 left off

# 4. Restart R3 and watch it reclaim leadership (standard Bully semantics)
docker compose start reservation-3
#   reservation-3: "Reservation-3 ELECTED as new coordinator."
#   manager:       "New leader for Reservation cluster = Reservation-3"
```

---

## 15. Energy and Pricing Formulas

**Charging Duration:**
```
duration_seconds = (T_end - T_start).toMillis() / 1000.0
duration_hours   = duration_seconds / 3600.0
```

**Energy Consumption (E = P × T):**
```
energy_kWh = charging_power_kW × duration_hours
           = 7.2 kW × duration_hours
```

**Total Bill:**
```
bill_Rs = base_price_per_kWh × energy_kWh × demand_multiplier
        = Rs.10.0 × energy_kWh × multiplier
```

**Example:** A 12-minute session at 7.2 kW with LOW demand (multiplier = 1.0):
```
duration  = 720 s  = 0.2 hours
energy    = 7.2 × 0.2  = 1.44 kWh
bill      = 10.0 × 1.44 × 1.0 = Rs. 14.40
```

---

## 16. Documentation Index

All extended documentation is in `docs/`:

| Document | Contents |
|---|---|
| `ARCHITECTURE.md` | Original single-instance architecture; dual-clock and replication topology |
| `BULLY_ALGORITHM.md` | Full algorithm walkthrough, worked example, reliability fixes from Docker testing |
| `REPLICATION.md` | Both replication mechanisms (2-node Reservation + generalized N-instance), sequence diagrams, self-deadlock fix |
| `FAILOVER.md` | Step-by-step failover flow, live verification logs, failback semantics |
| `LOAD_BALANCING.md` | `pickForRead` / `pickForWrite` design, which operations are round-robined vs leader-only |
| `DATABASE_SCHEMA.md` | All 5 table schemas, per-instance DB mapping, connection handling details |
| `DOCKER.md` | Comprehensive Docker guide: `libfaketime`, Cristian's algorithm, Lamport clocks, architecture diagrams |
| `WORKFLOW.md` | Step-by-step business workflow with Lamport event lifecycle and energy formulas |
| `MANUAL_DEMONSTRATION.md` | PowerShell scripts for Docker and manual 17-JVM demonstration |
| `TESTING.md` | Test scenarios, expected outcomes, empirical verification checklist |
| `MULTITHREADING.md` | Thread safety: `synchronized` blocks, `AtomicLong` CAS, concurrent port reservation |
| `STATE_MANAGEMENT.md` | In-memory state maps vs DB persistence, crash recovery |
| `RMI_COMMUNICATION.md` | RMI binding names, Lamport propagation rules per hop |
| `DISTRIBUTED_SYSTEM_ROADMAP.md` | Complete as-built distributed system design document (the definitive reference) |
| `MODULES.md` | Module dependency graph |
| `PROJECT_STRUCTURE.md` | Directory tree explanation |
| `AUDIT.md` | Changelog and decision audit trail |

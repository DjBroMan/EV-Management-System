# Distributed System Roadmap — Full Cluster HA/Load-Balancing Extension

Status: **IMPLEMENTED.** Phases 1-12 below have been built, compiled, and
live-tested (17-process manual multi-JVM run: Bully election, replication,
Manager routing, load balancing, and a real failover were all exercised and
verified working end-to-end — see the Final Report delivered alongside this
document, and `log/` for the timestamped implementation record). The open
questions at the end of this document were resolved as follows: **3
instances per cluster** (as originally proposed); **`ReservationManager`/
`ReservationService` names preserved**, generalized to also serve the other
4 services rather than introducing a separate `EVManager` class; **Pricing
kept read-replica-only** (no client call ever mutates pricing state, so
inventing a write-replication path for it would not reflect real behavior).
See `docs/BULLY_ALGORITHM.md`, `docs/REPLICATION.md`, `docs/FAILOVER.md`,
and `docs/LOAD_BALANCING.md` for the as-built mechanics of each phase.

Historical note — this section originally read:
"DRAFT — for review before any implementation begins."
Scope: extend the existing EV Management System (RMI + Lamport + Cristian + MySQL + one-off
Reservation Primary/Secondary replication) so that **all 5 server modules**
(ChargingStation, Reservation, ChargingSession, Pricing, Payment) support N-instance
clusters with Bully leader election, primary/secondary replication, failover, and
Manager-mediated load balancing.

This document does not change any code. It is the plan referenced by Part 13 of the
implementation rules ("do not proceed to the next phase if broken", "one phase at a
time"). Each phase below is meant to be implemented, compiled, tested, and logged
before the next one starts.

---

## 0. Ground truth (from repository inspection — see also `docs/REPLICATION.md`,
   `docs/RMI_COMMUNICATION.md`, `docs/STATE_MANAGEMENT.md`)

### 0.1 Current per-service reality

| Service | State today | HA today | Registry:Export port(s) | Bound name(s) |
|---|---|---|---|---|
| ChargingStationServer | in-memory `String[] ports/portStatus`, `synchronized` methods | none | 1234:2234 | `ChargingStationServer` |
| ReservationServer | `Map reservations`, `Map reservationPorts`, counter | **yes** — PRIMARY/SECONDARY + `ReservationServerManager` router, but hardcoded to exactly 2 nodes, no election, no failback | PRIMARY 1235:2235, SECONDARY 1245:2245, Manager 1240:2240 | `ReservationService`, `ReservationReplicationService` (per node); Manager also binds `ReservationManager` + `ReservationService` |
| ChargingSessionServer | 7 `HashMap`s + counter | none | 1236:2236 | `ChargingSessionServer` |
| PricingServer | `Map stationDemand`, loaded once at boot, not mutated by client calls (closest to stateless) | none | 1238:2238 | `PricingService` |
| PaymentServer | 2 `HashMap`s + counter | none | 1237:2237 | `PaymentServer` |
| TimeServer (Cristian reference clock) | stateless | n/a | 1239:2239 | `TimeServer` |

### 0.2 Current databases (`db/init/01..06_*.sql`, no `DATABASE_SCHEMA.md` exists yet)

`ev_station_db.charging_ports`, `ev_reservation_primary_db.reservations`,
`ev_reservation_secondary_db.reservations` (identical schema), `ev_session_db.charging_sessions`,
`ev_pricing_db.station_pricing_tariffs`, `ev_payment_db.payments`. One MySQL 8.0 container per
database. `DBConnectionHelper.getConnectionWithRetry` already retries container startup.
Every server degrades to pure in-memory if `DB_HOST` is unset/unreachable.

### 0.3 Current Manager / client reality

Only Reservation has a Manager (`ReservationServerManager`, port 1240) acting as a proxy +
replication coordinator + ad-hoc failover trigger. `EVClient` talks to the Manager for
Reservation, but talks **directly** to ChargingStation (1234), ChargingSession (1236),
Payment (1237), Pricing (1238) — there is no single client entry point across all 5 services.

### 0.4 Current clocks — keep as-is

Lamport: `Clock/LogicalClock.java` (AtomicLong CAS `max(local,received)+1`), `LamportResult<T>`
wrapper, dual-overload RMI methods everywhere. Cristian: `Clock/TimeServer` + `Clock/CristianClient`
+ `Clock/PhysicalClock` (static volatile offset), `synchronizeClock()` on every server, driven by
per-container `libfaketime` skew in `docker-compose.yml`. **Nothing in this roadmap replaces
these mechanisms** — they are extended to the new Manager and to every new server instance
exactly as already wired.

### 0.5 What must NOT be reinvented

- Do not create a new election algorithm — Bully only (Part 4).
- Do not add cross-service SQL foreign keys (Part 5) — `reservation_id`/`session_id`/`port_id`/
  `payment_id` stay logical/application-level references, resolved only via RMI.
- Do not replace Lamport/Cristian.
- Do not pre-seed business data in new init scripts (only `ChargingStationDAO` P1-P4 and
  `PricingDAO` S01-S03 self-seed, and only because that is existing behavior — no new seeding).
- Preserve `ReservationInterface`, `PaymentInterface`, `PricingInterface`,
  `ChargingSessionInterface`, `ChargingStationInterface` client-facing method signatures so
  `EVClient`'s and existing tests' call sites keep compiling.

---

## PHASE 1 — Current architecture verification

**Objective:** Confirm (not assume) the inventory in Section 0 against the live repository and
establish `docs/DATABASE_SCHEMA.md` as the approved source of truth before any schema changes
happen later (Part 8 requires it to exist and be authoritative).

**Architecture:** No behavioral change. This phase produces documentation and a verification
checklist only.

**Files affected (created only):**
- `docs/DATABASE_SCHEMA.md` — transcribed verbatim from `db/init/01..06_*.sql` plus a short
  preface noting it is the schema baseline that Phase 3 will extend (new columns: `server_id`,
  `role`; no table redesign).
- `docs/DISTRIBUTED_SYSTEM_ROADMAP.md` — this file.

**Classes/interfaces required:** none.

**Docker changes:** none.

**Database changes:** none — read-only transcription.

**Tests:** `mvn`/`javac` not touched; run existing `ReplicationTest` and `MultithreadTest` once
against current `docker-compose up` to record a "before" baseline (expected: 8/8 replication
scenarios pass, multithread test's existing pass criteria hold). This baseline is the regression
bar for every later phase.

**Expected terminal output:** identical to current `ReplicationTest`/`MultithreadTest` runs
today (no change yet).

**Failure scenarios:** if the baseline does not currently pass 100%, STOP — fix or document the
pre-existing failure before starting Phase 2, since later phases must not be blamed for
pre-existing breakage.

---

## PHASE 2 — Make all 5 services capable of running multiple instances

**Objective:** Remove the hardcoded assumption ("exactly one instance per service, one instance
bound to one fixed registry port") from ChargingStation, ChargingSession, Pricing, and Payment
so that, like Reservation already does with `RESERVATION_ROLE`/registry-port args, every service
can be launched N times with distinct identity.

**Architecture:** Introduce a common `ServerIdentity` concept applied uniformly to all 5 servers
(Reservation's existing pattern generalized):
- CLI args / env vars: `SERVER_ID` (unique small integer, used later for Bully priority),
  `SERVICE_ROLE` (`PRIMARY`/`SECONDARY`), `RMI_PORT` (registry+export), `RMI_BIND_NAME` suffixed
  with instance id (e.g. `ChargingStationServer-2`), `PEER_LIST` (comma-separated host:port of
  sibling instances in the same cluster).
- Each server still binds its historical bare name (e.g. `ChargingStationServer`) **only when it
  is elected leader**, so unaware legacy lookups keep working during transition; it always also
  binds its unique instance name (e.g. `ChargingStationServer-2`) for peer/manager use.

**Files affected:**
- `ChargingStation/ChargingStationServer.java`, `ChargingSession/ChargingSessionServer.java`,
  `Pricing/PricingServer.java`, `Payment/PaymentServer.java` — add identity/role fields and
  CLI/env parsing mirrored from `ReservationServer.java`'s existing pattern.
- New shared class `Common/ServerIdentity.java` (parses `SERVER_ID`, `SERVICE_ROLE`, `PEER_LIST`)
  to avoid duplicating parsing logic 5×.

**Classes/interfaces required:** `Common.ServerIdentity` (POJO/parser only, no behavior change
to RMI contracts yet).

**Docker changes:** none yet (compose changes land in Phase 4/7 once replication+manager exist);
this phase only proves multi-instance startup works when launched manually with distinct
`RMI_PORT`/`SERVER_ID` on the same host.

**Database changes:** none.

**Tests:** manual/scripted — start 2 instances of PricingServer (the least stateful service) on
two ports with distinct `SERVER_ID`s, confirm both bind successfully and both answer
`calculatePrice` identically (read-only from DB, so no split-brain risk yet since no replication
exists until Phase 4).

**Expected terminal output:**
```
[PricingServer-1] Bound as PricingService-1 on port 1238
[PricingServer-2] Bound as PricingService-2 on port 1248
```

**Failure scenarios:** two instances started with the same `SERVER_ID` or same `RMI_PORT` must
fail fast with a clear error (port/registry bind exception) rather than silently overwriting
each other.

---

## PHASE 3 — Database-per-server-instance architecture

**Objective:** Give every instance of every stateful service its own database (generalizing the
existing `ev_reservation_primary_db`/`ev_reservation_secondary_db` split to all clusters), per
`docs/DATABASE_SCHEMA.md` from Phase 1.

**Architecture:** For each cluster of N instances, N databases with identical schema (mirrors
the two Reservation DBs already in place). Add two non-breaking columns to every stateful table:
`server_id INT NOT NULL` (which instance last wrote the row — for diagnostics/replication
provenance) and `role VARCHAR(16)` is **not** stored in the DB (role is runtime cluster state,
not persisted business data — avoids the DB itself becoming a second source of truth for
leadership).

**Files affected:**
- `db/init/07_ev_station_db_2.sql`, `08_ev_station_db_3.sql` (ChargingStation cluster) — same
  pattern for ChargingSession (`ev_session_db_2/3`), Pricing (`ev_pricing_db_2/3`), Payment
  (`ev_payment_db_2/3`). Reservation already has this pattern (primary/secondary); extend
  naming only if Phase 5 needs a 3rd Reservation node for Bully quorum (see Phase 5 note).
- `ChargingStation/ChargingStationDAO.java`, `ChargingSession/ChargingSessionDAO.java`,
  `Pricing/PricingDAO.java`, `Payment/PaymentDAO.java` — add `server_id` column to INSERT/UPDATE
  statements only (no query logic redesign).
- `docs/DATABASE_SCHEMA.md` — updated with the new per-instance DB names and the `server_id`
  column addition.

**Classes/interfaces required:** none new; DAOs extended, not replaced.

**Docker changes:** add one MySQL container per new instance (`mysql-station-2`,
`mysql-station-3`, etc.), each with its own named volume, mirroring the existing
`mysql-reservation-primary`/`mysql-reservation-secondary` pattern exactly.

**Database changes:** as above. Databases start empty (only self-seeding that already exists —
ChargingStation P1-P4, Pricing S01-S03 — runs on **each** instance's own DB, not shared; no new
sample data).

**Tests:** verify each new instance connects only to its own `DB_HOST`/`DB_NAME` (no
cross-instance queries); verify `DBConnectionHelper.getConnectionWithRetry` behavior is
unchanged.

**Expected terminal output:**
```
[ChargingStationServer-2] Connected to mysql-station-2 (ev_station_db) after 1 retries
[ChargingStationServer-2] Self-seeded P1-P4 (table was empty)
```

**Failure scenarios:** if `mysql-station-2` is unavailable, instance 2 must degrade to
in-memory-only exactly like the existing single-instance behavior (no crash) — verifies Phase 3
does not remove the existing graceful-degradation guarantee.

---

## PHASE 4 — Primary/Secondary replication for all stateful services

**Objective:** Generalize the existing Reservation replication mechanism
(`ReservationReplicationInterface`, `ReservationStateSnapshot`, `applyReservationUpdate`,
`synchronizeFullState`) into a reusable pattern applied to ChargingStation, ChargingSession, and
Payment (Pricing is read-mostly — see note below).

**Architecture:**
```
PRIMARY (instance holding current writes)
   | replicateUpdate(update, lamport)   [synchronous RMI, mirrors ReservationServer.reserveSlot]
   v
SECONDARY.applyUpdate(update, lamport)  [overwrite local state, advance counter, persist to own DB]
```
- New generic interfaces mirroring the Reservation pattern 1:1:
  - `<Service>ReplicationInterface` per service (e.g. `ChargingStationReplicationInterface`)
    with `applyStateUpdate`, `getStateSnapshot`, `synchronizeFullState`, `ping`, `promoteToPrimary`,
    `getRole` — same method shapes as `ReservationReplicationInterface` for consistency.
  - `<Service>StateSnapshot` per service (serializable DTO of that service's in-memory maps),
    mirroring `ReservationStateSnapshot`.
- Each stateful server gains the same `Role{PRIMARY,SECONDARY}` enum and `synchronized` apply
  path Reservation already has; write methods (`reservePort`, `startCharging`, `makePayment`,
  etc.) get the same "SECONDARY rejects client writes" guard already in
  `ReservationServer.reserveSlot`.
- **Pricing exception:** since Pricing has no client-driven mutation in the current workflow, it
  gets read-replica behavior only (all instances load from their own DB independently, no
  write-replication RPC needed) — this avoids inventing replication for state that is never
  written by clients, consistent with Part 5's "do not introduce" spirit applied conservatively.

**Files affected:**
- New: `ChargingStation/ChargingStationReplicationInterface.java`,
  `ChargingStation/ChargingStationStateSnapshot.java`, and equivalents under
  `ChargingSession/` and `Payment/`.
- Modified: `ChargingStationServer.java`, `ChargingSessionServer.java`, `PaymentServer.java` —
  add role field, replication-apply methods, synchronous replicate-on-write call (mirroring
  `ReservationServer.java:317-334`).
- `Reservation/*` unchanged in contract (already correct); only refactor if a genuinely shared
  helper emerges (e.g., an abstract base class) — do not force one that breaks existing behavior.

**Classes/interfaces required:** 3 new `*ReplicationInterface` + 3 new `*StateSnapshot` classes
(one triplet per newly-replicated service).

**Docker changes:** none beyond what Phase 3 already added (DBs); Manager wiring happens in
Phase 7.

**Database changes:** none beyond Phase 3.

**Tests:** new `ChargingStationReplicationTest.java`, `ChargingSessionReplicationTest.java`,
`PaymentReplicationTest.java` mirroring `ReplicationTest.java`'s 8-scenario structure
(discovery, single/multi replication, concurrent load, full sync) — reuse its structure, not a
new design.

**Expected terminal output (per service, same shape as existing ReplicationTest):**
```
[REPLICATION] ChargingStation PRIMARY -> SECONDARY: applyStateUpdate(P2, RESERVED) Lamport=14
[REPLICATION] SECONDARY ack: state applied, local counter unaffected (no counter for ports)
```

**Failure scenarios:** secondary unreachable during replicate-on-write → log warning, do not
roll back the primary write (matches existing Reservation behavior exactly, for consistency
across services).

---

## PHASE 5 — Bully algorithm for leader election (per cluster, not global)

**Objective:** Replace the current *hardcoded* 2-node primary/secondary swap (Reservation) and
*absent* failover (other 4 services) with a real Bully election, run independently inside each
of the 5 clusters.

**Architecture:**
- Each cluster (e.g. Reservation: R1/R2/R3) needs **at least 3 nodes** for Bully to be
  meaningful (2-node Bully degenerates to "the other one wins", which is what already exists).
  This roadmap assumes 3 instances per cluster going forward (Phase 3/4 already generalized DB
  and replication to N instances).
- New `Common/BullyElection.java` (shared by all 5 server types, parameterized by
  `ServerIdentity.serverId` and `PEER_LIST`):
  - `startElection()`: sends `ELECTION` to every peer with a higher `serverId`.
  - If no `OK` received within timeout → this node declares itself COORDINATOR, sends
    `COORDINATOR` announcement to all peers.
  - If `OK` received → waits for a `COORDINATOR` announcement; if none arrives within a second
    timeout, restarts its own election.
  - On receiving `ELECTION` from a lower id → replies `OK`, then starts its own election.
  - Heartbeat thread (`ScheduledExecutorService`, fixed interval, e.g. 2s) pings the current
    coordinator via `ping()` (already present on the replication interfaces from Phase 4); N
    consecutive failures trigger `startElection()`.
- New `Common/ElectionInterface.java` RMI interface (`receiveElection(int candidateId)`,
  `receiveOk(int fromId)`, `receiveCoordinator(int newLeaderId, String host, int port)`,
  `ping()`) implemented by every server alongside its existing replication interface.
- `serverId` is the existing/extended `SERVER_ID` env var from Phase 2 — higher id wins ties,
  exactly per Part 4.

**Files affected:**
- New: `Common/BullyElection.java`, `Common/ElectionInterface.java`.
- Modified: all 5 server classes — implement `ElectionInterface`, start a `BullyElection`
  instance at boot, wire `promoteToPrimary()`-equivalent (already exists for Reservation, added
  in Phase 4 for the other 3) to be invoked when Bully declares this node coordinator.

**Classes/interfaces required:** `Common.BullyElection`, `Common.ElectionInterface`,
`Common.ServerIdentity` (from Phase 2, extended with peer RMI lookups).

**Docker changes:** each cluster's compose services grow from 2 to 3 instances (mirrors adding
`reservation-secondary`-style entries); each new instance gets `SERVER_ID`, `PEER_LIST` env vars
listing sibling container hostnames.

**Database changes:** none beyond Phase 3 (3rd instance per cluster needs its own DB, already
covered by Phase 3's per-instance pattern).

**Tests:** new `BullyElectionTest.java` — start a 3-node cluster (any one service, generalized to
all 5), kill the current coordinator process, assert the highest remaining `serverId` wins and
announces within a bounded time.

**Expected terminal output (matches Part 4/10 example):**
```
[BULLY][ReservationServer-2] Heartbeat to coordinator (R3) failed 3x
[BULLY][ReservationServer-2] Starting election, sending ELECTION to peers with higher id: [R3 down, none higher]
[BULLY][ReservationServer-2] No OK received, declaring self COORDINATOR
[BULLY][ReservationServer-2] Sent COORDINATOR announcement to R1
[MANAGER] New leader for Reservation cluster = ReservationServer-2
```

**Failure scenarios:** simultaneous election storms (two nodes detect failure at once) must
still converge to one coordinator (standard Bully guarantee — highest id always wins,
lower-id nodes yield on receiving a higher `COORDINATOR` announcement).

---

## PHASE 6 — Leader promotion and failover integration

**Objective:** Wire Bully's `COORDINATOR` announcement into the existing `promoteToPrimary`/role
mechanism (Phase 4) so a newly elected node actually starts accepting writes and existing
replicated state (Phase 4's replicated DB) is reused with no reconstruction — per Part 6 exactly.

**Architecture:**
```
Manager
   | X current leader unreachable (RemoteException on proxied call)
   v
Cluster's BullyElection.startElection() [Phase 5]
   v
New coordinator: role = PRIMARY, reconnect to dependent services (mirrors
ReservationServer.ensureChargingStationConnected(), generalized per service)
   v
Manager updates its leader pointer for that cluster (Phase 7 introduces the Manager
that has such a pointer for all 5 clusters, not just Reservation)
```
No `synchronizeFullState`/full snapshot pull happens on ordinary failover, because Phase 4's
synchronous replicate-on-write already kept the secondary's DB current — full sync (already
existing as `triggerFullSynchronization`) remains available only for manual/disaster recovery,
not the default failover path, matching Part 6's "no unnecessary full-state reconstruction".

**Files affected:**
- All 5 server classes — replace the Reservation-only failover trigger
  (`ReservationServerManager.performFailoverInternal`) logic with cluster-local Bully-driven
  promotion; Manager (Phase 7) no longer performs failover itself, it only detects an
  unreachable leader and asks that cluster's surviving nodes to elect (or simply waits for the
  cluster's own heartbeat-driven election from Phase 5 and re-queries who the leader is).

**Classes/interfaces required:** none new beyond Phase 4/5; this phase is integration wiring.

**Docker changes:** none beyond Phase 5.

**Database changes:** none.

**Tests:** `FailoverTest.java` per cluster (generalizing `ReplicationTest`'s Test 6/7/8) — kill
the leader mid-workflow, assert in-flight-adjacent requests succeed against the new leader using
already-replicated state (no data loss, no reconstruction delay beyond election time).

**Expected terminal output:** matches Part 6/10 examples exactly (Manager reports "Primary
unavailable" → "Bully Election" → "New Primary" → continued service).

**Failure scenarios:** the previously-failed node rejoining later must come back as SECONDARY
(not re-trigger a duplicate coordinator) — on rejoin it queries current peers' role via `ping`/
`getRole` before assuming any role, closing the existing "orphaned old primary" gap called out
in Section 0.1/`docs/REPLICATION.md`'s Known Limitations.

---

## PHASE 7 — Manager as the single client entry point (all 5 services)

**Objective:** Generalize `ReservationServerManager` into one `EVManager` (or 5 lightweight
per-cluster manager facades behind one bound name) so `EVClient` looks up exactly one
address — the design goal in Part 3's diagram — instead of 4 direct service lookups + 1 Manager
lookup as today.

**Architecture:**
```
EVClient --(single Naming.lookup, port 1240 or new EVManager port)--> Manager
Manager maintains, per cluster (Reservation/ChargingStation/ChargingSession/Pricing/Payment):
   - list of known instance addresses (from PEER_LIST / static discovery config)
   - each instance's last-known health (from Phase 9 heartbeats)
   - each cluster's current leader (learned from Bully COORDINATOR announcements, Phase 5/6)
Manager exposes one proxy method per existing client-facing RMI method (reserveSlot,
startCharging, makePayment, calculatePrice, getStationStatus, ...), forwarding to the
appropriate instance (leader for writes, any healthy instance for reads — see Phase 8).
```
This is an additive generalization of the existing `ReservationServerManager` proxy pattern
(`ReservationServerManager.java` already does exactly this for one service) — not a rewrite of
its mechanism, applied to 4 more services.

**Files affected:**
- `Reservation/ReservationServerManager.java` — generalize into `Manager/EVManager.java` (new
  package) that composes 5 per-cluster routing tables instead of one hardcoded
  primary/secondary pair; keep `ReservationManagerInterface`-style proxy methods for backward
  compatibility.
- `EVClient.java` — replace the 4 direct-service `Naming.lookup` calls with lookups against the
  single Manager bound name, for parity with how Reservation calls already go through the
  Manager.

**Classes/interfaces required:** `Manager.EVManager`, `Manager.EVManagerInterface` (superset of
today's `ReservationManagerInterface`, extended with ChargingStation/Session/Pricing/Payment
proxy methods).

**Docker changes:** the single `reservation-manager` service becomes `ev-manager` (or is kept
as-is and simply granted more responsibility — naming decision to confirm with user before
Phase 7 implementation); `depends_on` expands to all cluster instances across all 5 services.

**Database changes:** none — Manager remains stateless w.r.t. business data (only holds routing/
health tables in memory, same as today's `currentPrimaryHost/Port` pattern).

**Tests:** update `EVClient` smoke test / manual run to confirm every menu option still works
through the single Manager address.

**Expected terminal output:**
```
[MANAGER] Request from EVClient: reserveSlot(user5, vehicleX)
[MANAGER] Routed to ReservationServer-2 (current leader)
[MANAGER] Request from EVClient: getStationStatus()
[MANAGER] Routed to ChargingStationServer-1 (load-balanced read)
```

**Failure scenarios:** Manager itself remains a SPOF at this phase (documented as a known
limitation until/unless the user asks for Manager HA — out of the original scope's 5 clusters).

---

## PHASE 8 — Load balancing across healthy instances

**Objective:** Implement the Part 7 requirement — Manager chooses among healthy instances,
distinguishing load-balanced reads from leader-only writes.

**Architecture:**
- Round-robin index per cluster for **read/stateless-safe operations** (e.g.
  `getStationStatus`, `getAvailablePorts`, `calculatePrice`, `getPaymentStatus`) — any healthy
  instance in that cluster answers, cycling through the healthy set.
- **Write / leader-owned operations** (`reserveSlot`, `cancelReservation`, `startCharging`,
  `stopCharging`, `makePayment`) always route to the cluster's current Bully-elected leader,
  never round-robined — this is the explicit Part 7 distinction: "Do NOT blindly load-balance
  operations that require Primary ownership."
- `Manager.EVManager` (Phase 7) holds, per cluster, `List<InstanceHandle> healthyInstances` and
  `int roundRobinCursor`, updated by Phase 9's heartbeat loop.

**Files affected:** `Manager/EVManager.java` only (routing-table consumer logic); no server-side
changes needed beyond what Phases 4-6 already provide.

**Classes/interfaces required:** `Manager.LoadBalancer` (small helper: `selectForRead(cluster)`,
`selectForWrite(cluster)` — the latter simply returns the known leader).

**Docker changes:** none beyond Phase 5/7 (more instances already exist).

**Database changes:** none.

**Tests:** `LoadBalancingTest.java` — issue N read-only calls to a 3-instance cluster, assert
they're distributed round-robin across all 3; issue N write calls, assert 100% land on the
current leader regardless of round-robin state.

**Expected terminal output (matches Part 10 example):**
```
[LOAD BALANCER] Selected ReservationServer-2 (read)
[LOAD BALANCER] Write operation -> routing to leader ReservationServer-3, bypassing round-robin
```

**Failure scenarios:** an instance marked unhealthy by Phase 9 must be skipped by round-robin
until it reports healthy again.

---

## PHASE 9 — Health checks / heartbeats

**Objective:** Give the Manager (Phase 7/8) and each cluster's Bully election (Phase 5) a
consistent, shared heartbeat mechanism, instead of only reactive on-call failure detection
(today's Reservation-only behavior).

**Architecture:**
- Every server instance already implements `ping()` (added in Phase 5's `ElectionInterface`).
  A `ScheduledExecutorService` in `Manager.EVManager` polls `ping()` on every known instance
  every N seconds (configurable, default 3s), updating the health table Phase 8 reads.
- Each cluster's own Bully heartbeat loop (Phase 5, polling only the coordinator) stays
  separate from the Manager's heartbeat loop (polling all instances) — these are two views of
  the same `ping()` RPC for two different consumers (election vs. load-balancing), not a
  duplicated algorithm.

**Files affected:** `Manager/EVManager.java` (new heartbeat scheduler); no new server-side RMI
methods (reuses Phase 5's `ping()`).

**Classes/interfaces required:** `Manager.HealthMonitor` (wraps the scheduled polling +
health table, feeding `Manager.LoadBalancer`).

**Docker changes:** none.

**Database changes:** none.

**Tests:** `HealthCheckTest.java` — stop one instance's process, assert Manager's health table
marks it unhealthy within `N * missedThreshold` seconds and load balancer stops selecting it;
restart it, assert it's marked healthy again.

**Expected terminal output:**
```
[HEALTH] ChargingStationServer-3 missed heartbeat (2/3)
[HEALTH] ChargingStationServer-3 marked UNHEALTHY, removed from load-balancing pool
[HEALTH] ChargingStationServer-3 responded to ping, marked HEALTHY, re-added
```

**Failure scenarios:** Manager's own health-check thread crashing must not take down request
routing (isolate in try/catch per poll, log and continue).

---

## PHASE 10 — Integration of load balancing + replication + Bully + failover

**Objective:** Prove the four mechanisms work together end-to-end, not just in isolation —
the actual novel risk area (e.g., a failover happening *during* a load-balanced read, or an
election happening *during* a replicated write).

**Architecture:** no new components; this phase is integration testing + hardening of edge
cases discovered:
- Read hitting an instance that is mid-demotion (was leader, Bully just elected someone else) —
  Manager must retry against the new leader if a stale-leader write is attempted (mirrors
  existing `ReservationServerManager`'s retry-after-failover pattern, generalized).
- Concurrent elections in two different clusters must not interfere (they are fully independent
  per Part 4 — verify no shared static state accidentally couples them, e.g. confirm
  `BullyElection` instances are per-cluster-per-node, not a shared singleton).

**Files affected:** likely small fixes across `Manager/EVManager.java` and the 5 server classes
based on what integration testing surfaces — no big new files expected.

**Classes/interfaces required:** none new (integration phase).

**Docker changes:** none beyond what exists.

**Database changes:** none.

**Tests:** `FullIntegrationTest.java` — run `MultithreadTest`-style concurrent client load while
killing/restarting one instance per cluster mid-run, assert no lost/duplicated writes and all
counters remain consistent across surviving + rejoined instances.

**Expected terminal output:** combination of all previous phases' log lines interleaved
correctly (Manager routing, Bully election, replication acks, Lamport/Cristian stamps all
present per Part 9's logging requirement).

**Failure scenarios:** if any interleaving produces split-brain (two nodes both believing they
are leader) or lost writes, this phase is not complete — fix before Phase 11.

---

## PHASE 11 — Multithreaded testing

**Objective:** Extend the existing `MultithreadTest.java` pattern (10 concurrent EV client
threads via `CountDownLatch`-gated `ExecutorService`) to run against the new multi-instance,
load-balanced, Bully-enabled clusters, not just the original single-instance-per-service setup.

**Files affected:** `MultithreadTest.java` extended (or a new `MultithreadClusterTest.java`
alongside it, keeping the original test intact per "do not rewrite working functionality
unnecessarily").

**Tests:** same thread-safety assertions as today (no lost updates, counters correct), plus new
assertions that requests were actually distributed across multiple instances (log-based
verification of round-robin behavior under concurrent load).

**Expected terminal output:** existing `MultithreadTest` output shape, annotated with which
instance served each simulated EV's requests.

**Failure scenarios:** race conditions surfacing only under N-instance concurrency that didn't
exist in the single-instance version (e.g., two leaders both accepting a write during a
brief post-election window) — must be fixed, not tolerated.

---

## PHASE 12 — Manual terminal demonstration

**Objective:** Deliver the Part 10 demonstration scripts/tests so the user can show the
professor each capability individually and in combination.

**Files affected:** new `demo/` scripts (shell or a small Java `DemoRunner`) — one per
capability listed in Part 10 (multithreading, DB persistence, replication, Bully election,
leader failure, leader promotion, load balancing, full EV workflow, combined failure+failover).

**Tests:** these *are* the tests — thin wrappers around the Phase 4-11 test classes, run
individually with clear, narrated terminal output matching the Part 10 example format exactly
(`[MANAGER]`, `[LOAD BALANCER]`, `[RESERVATION] Role: PRIMARY`, `[PHYSICAL CLOCK]`, `[LAMPORT]`,
`[REPLICATION]`, `[BULLY]` tags — reusing `Clock/DistributedLogger.java`'s existing tag-based
format rather than inventing a new logging style).

**Expected terminal output:** as specified verbatim in Part 10 of the request.

**Failure scenarios:** n/a — this phase packages demonstrations of behavior already proven
correct in Phases 4-11; if a demo fails, the underlying phase (not the demo script) is what's
broken and must be revisited.

---

## Open questions to confirm with the user before implementation starts

1. **Cluster size**: Part 3's example shows 3 instances per service (R1/R2/R3, etc.). Bully
   needs ≥3 to be meaningfully different from the current 2-node swap. Confirm 3 is the target
   size for all 5 clusters (affects Phase 3/5 Docker Compose scale).
2. **Manager naming**: keep the bound name `ReservationManager`/`ReservationService` for
   backward compatibility, or introduce a new `EVManager` name and update `EVClient` fully
   (Phase 7 assumes the latter, per Part 3's single `MANAGER :1240` diagram).
2b. **Pricing replication**: this roadmap treats Pricing as read-replica-only (no
   write-replication RPC) since no client call mutates its state today. Confirm this
   interpretation of Part 5 ("each stateful service should maintain replicated state") is
   acceptable, or whether demand-level updates should become a client-facing mutation that then
   needs full Primary/Secondary replication like the other 4.
3. **Port allocation for new instances**: this roadmap assumes sequential port offsets per
   instance (e.g., ChargingStation-1 1234/2234, -2 1244/2244, -3 1254/2254) mirroring the
   existing Reservation +10 convention (1235→1245). Confirm before Phase 3/5 Docker changes.

Once these are confirmed, Phase 1 can begin (verification + `docs/DATABASE_SCHEMA.md`
creation), followed strictly one phase at a time per Part 13.

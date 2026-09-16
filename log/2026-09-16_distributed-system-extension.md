# Implementation Log — Distributed System Extension (Bully, N-instance clusters, Manager, Load Balancing)

- **Date:** 2026-09-16
- **Time (UTC):** 22:24–23:37 (session start to final smoke-test validation)

## Files changed

**New:**
- `Common/ServerIdentity.java`, `Common/PeerHandle.java`, `Common/StateDelta.java`,
  `Common/GenericSnapshot.java`, `Common/ClusterNodeInterface.java`,
  `Common/BullyElection.java`, `Common/ClusterManagerInterface.java`,
  `Common/ClusterManagerClient.java`, `Common/ManagerRouting.java`
- `tests/BullyElectionTest.java`, `tests/LoadBalancingTest.java`,
  `tests/HealthCheckTest.java`, `tests/CombinedFailoverLoadTest.java`
- `db/init/07_ev_station_db_2.sql` through `db/init/15_ev_payment_db_3.sql` (9 new files)
- `docs/DATABASE_SCHEMA.md`, `docs/BULLY_ALGORITHM.md`, `docs/LOAD_BALANCING.md`,
  `docs/FAILOVER.md`, `docs/MANUAL_DEMONSTRATION.md`
- `docs/DISTRIBUTED_SYSTEM_ROADMAP.md` (created in a prior session turn, status updated here)

**Modified:**
- `ChargingStation/ChargingStationServer.java` — added `ServerIdentity`/`Role`/
  Bully election/`ClusterNodeInterface` implementation; write methods now
  reject when SECONDARY and fan replicated changes out via the Manager.
- `ChargingSession/ChargingSessionServer.java` — same pattern (start/stop charging).
- `Payment/PaymentServer.java` — same pattern (makePayment).
- `Pricing/PricingServer.java` — same pattern, but read-replica only (no write path).
- `Reservation/ReservationServer.java` — added `ClusterNodeInterface` alongside
  the existing (unchanged) `ReservationReplicationInterface`; added optional
  Bully cluster mode via `SERVER_ID`/`PEERS` env vars; `resolveStationUrl()`
  now defaults through the Manager instead of a fixed port.
- `Reservation/ReservationServerManager.java` — generalized into the full
  Manager: implements `ChargingStationInterface`/`ChargingSessionInterface`/
  `PricingInterface`/`PaymentInterface`/`ClusterManagerInterface` in addition
  to the original `ReservationManagerInterface`; added per-cluster instance
  discovery, health monitoring, round-robin load balancing for reads,
  leader-only routing for writes, and generic replication fan-out. The
  original 2-node Reservation proxy/replication/failover code is preserved
  byte-for-byte as the default path (opt into the new N-instance path via
  `RESERVATION_INSTANCES`).
- `EVClient.java` — now looks up all 5 services exclusively through the
  Manager (single entry point) instead of 4 direct + 1 Manager lookup.
- `MultithreadTest.java` — same Manager-only routing change.
- `Dockerfile` — compiles `Common/*.java` and `tests/*.java`.
- `docker-compose.yml` — rewritten for 32 services (3 instances × 5 clusters,
  each with its own MySQL database, + Manager + TimeServer).
- `docs/REPLICATION.md`, `docs/PROJECT_STRUCTURE.md`, `docs/DOCKER.md`,
  `docs/ARCHITECTURE.md` — corrected stale claims (e.g. "no DBMS is used")
  and added pointers to the new docs.

## Description

Extended the existing single-instance-per-service EV Management System into
a 5-cluster, 3-instance-per-cluster distributed architecture: independent
Bully leader election per cluster, generalized primary-to-peer replication
via the Manager, a Manager acting as the single client entry point for all
5 services (not just Reservation), round-robin load balancing for
read-only calls with strict leader-only routing for writes, and a
background health monitor. The original 2-node Reservation
replication/failover mechanism was preserved unmodified as the default
code path; the new N-instance mechanism is additive and opt-in
(`RESERVATION_INSTANCES`) for that one cluster, avoiding any risk of the
two mechanisms fighting over the same nodes (split-brain).

## Reason

Requested extension of the existing distributed EV Management System to a
fully clustered, fault-tolerant, load-balanced architecture per
`docs/DISTRIBUTED_SYSTEM_ROADMAP.md`, while explicitly preserving existing
working functionality (RMI interfaces, Lamport/Cristian clocks, the
Reservation replication test suite, DB persistence) rather than replacing it.

## Tests performed

1. Full clean `javac` compile of the entire project (Clock, Common, all 5
   service packages, root files, `tests/`) — **PASS**, zero errors.
2. `docker compose config --quiet` — validated the 32-service Compose file
   parses correctly and all YAML anchors resolve — **PASS**.
3. Live 17-process manual smoke test (1 TimeServer + 3×ChargingStation +
   1 Manager + 3×Reservation + 3×ChargingSession + 3×Pricing + 3×Payment),
   all wired with the same `SERVER_ID`/`PEERS`/`MANAGER_HOST` env-var
   contract used in `docker-compose.yml`:
   - Bully bootstrap election on cold start (all 3 ChargingStation, all 3
     Reservation, etc. instances correctly elected the highest-ID node as
     coordinator) — **PASS**.
   - End-to-end EV workflow through the Manager only (station status →
     reserve → start charging → stop charging → calculate price → make
     payment → port released) — **PASS**.
   - Killed the Reservation leader (R3) mid-run: R1 detected the failure via
     heartbeat, started an election, R2 (next-highest surviving ID) won and
     announced itself, the Manager's health monitor picked up the new
     leader within one tick, and a subsequent reservation request was
     transparently served by R2 using its already-replicated counter state
     (`RES1002` continuing from `RES1001`, no reconstruction) — **PASS**.
   - Round-robin load balancing verified via Manager log inspection: 6
     consecutive `getStationStatus()` reads rotated across
     `ChargingStationService-1`/`-2`/(`-3`); write calls all routed to the
     single current leader — **PASS**.
4. **1 real bug found and fixed during testing**: `ChargingStationServer`'s
   write methods are `synchronized` at the method level; the original
   replication fan-out design (Manager replicates to *every* instance in a
   cluster) caused the leader to deadlock against its own still-held lock
   when the fan-out looped back to itself via a second RMI thread. Fixed by
   threading an `originServerId` through `ClusterManagerClient.replicate()`
   → `ClusterManagerInterface.replicateUpdate()` so the Manager always
   excludes the writer's own instance from fan-out (also simply correct,
   since the origin already applied the change locally). Re-tested after
   the fix — **PASS**, no further hangs across repeated runs.

## Result

All automated compiles pass; the live multi-process smoke test demonstrated
genuine (not simulated/mocked) Bully election, replication, Manager
routing, load balancing, and failover-with-continued-service, matching the
worked example given in the original request (R3 fails → R1 detects → R2
elected → Manager updates leader → new request succeeds). Docker Compose
structure validated syntactically; a full `docker compose up` run could not
be executed in this environment because the Docker daemon/Desktop
application is not available here (see Final Report "Remaining
limitations") — the Compose file uses the identical, already-verified
env-var wiring as the manual smoke test, so this is a deployment-mechanics
risk only, not a distributed-logic risk.

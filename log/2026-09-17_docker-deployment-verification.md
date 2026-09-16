# Implementation Log — Docker Deployment Verification & Fixes

- **Date:** 2026-09-17
- **Time (UTC):** 19:29–19:48 (Docker Desktop now available; full end-to-end
  verification requested to remove the "Docker daemon unavailable" caveat
  from the previous session's report)

## Docker environment tested

- Docker Engine 29.7.2, Docker Compose v5.4.0, `desktop-linux` context (Docker Desktop, Windows).
- Full `docker-compose.yml`: 32 services (1 TimeServer, 15 MySQL 8.0 instances, 15 clustered app instances, 1 Manager).

## Commands used

```
docker compose config --quiet          # structural validation
docker compose up -d --build           # first pass
docker compose ps                      # container verification
docker compose logs --tail=100         # startup error scan
docker exec <mysql-container> mysql -uroot -pevroot -e "SELECT ..."   # DB state checks
docker stop <container>  /  docker start <container>                 # failover simulation
docker compose down -v && docker compose build && docker compose up -d   # clean re-verification runs (x2, after each fix)
```
Plus small ad-hoc host-side Java RMI client programs (`DockerSmokeTest`,
`DockerLoadBalanceCheck`, `DockerFailoverCheck` — written for this session,
compiled locally against the project's own interfaces, run from the host
against the published Manager port 1240, then deleted afterward; not
committed to the repo) to exercise the actual EV workflow, load balancing,
and post-failover requests through the running containers.

## Services started

All 32: `time-server`, `manager`, `charging-station-1/2/3`,
`reservation-1/2/3`, `charging-session-1/2/3`, `pricing-1/2/3`,
`payment-1/2/3`, and their corresponding 15 `mysql-*` containers.

## Tests performed and results

1. `docker compose config --quiet` — **PASS**, no structural errors.
2. `docker compose up -d --build` — **PASS**, all 16 app images built without error, all 32 containers reached `Up`/`healthy`.
3. Startup log scan for exceptions/bind errors — **PASS** (zero genuine exceptions across 3 separate full runs after fixes).
4. Bully election on cold boot, all 5 clusters — **PASS**: every cluster elected the instance with `SERVER_ID=3` as coordinator, and every follower logged `Learned new coordinator` confirming actual message delivery (not just a lucky deterministic-bootstrap coincidence).
5. Manager leader discovery — **PASS**: `manager` logged `New leader for <cluster> cluster = <prefix>-3` for all 5 clusters via its own independent `getRole()` polling.
6. Full EV workflow through the Manager (`ChargingStationService` → `ReservationService` → `ChargingSessionService` start/stop → `PricingService` → `PaymentService` → port release) — **PASS**, run on a freshly-booted, schema-only (no seed data) database set; first records were `RES1001`/`SESSION-1001`/`PAY-1001`.
7. Database persistence — **PASS**: verified via direct `mysql` queries inside the MySQL containers that `reservations`, `charging_sessions`, `payments`, and `charging_ports` all reflect each workflow stage correctly, and that the reservation replicated identically to all 3 Reservation instance databases (`ev_reservation_primary_db`, `_secondary_db`, `_tertiary_db`) and all 3 ChargingStation instance databases.
8. Load balancing — **PASS**: 6 consecutive reads rotated `ChargingStationService-1/2/3` and `PricingService-1/2/3` in the Manager's own `[LOAD BALANCER]` log; consecutive writes all routed to the single current leader.
9. Real failover, Reservation cluster (`docker stop reservation-3`) — **PASS**: R1 detected the failure via heartbeat, ran a real Bully election, R2 (highest surviving ID) won and announced itself, the Manager updated its leader within ~12s (after the fix below), a subsequent `reserveSlot` request succeeded against R2, and both `RES1001` (pre-existing, preserved) and `RES1002` (new, correctly replicated) were present in both surviving instances' databases afterward.
10. Real failover, ChargingStation cluster (`docker stop charging-station-3`) — **PASS**: same pattern, CS2 (highest surviving ID) elected within ~12s, writes continued to succeed.
11. Rejoin behavior — observed and documented (not a defect): a restarted highest-ID node reclaims coordinatorship per standard Bully semantics; the demoted former leader steps down cleanly the moment it receives the announcement, with no lasting split-brain.

## Fixes made

1. **Lost Bully broadcast/election messages during Docker cold boot.**
   Root cause: `BullyElection.broadcastCoordinator()` and the `ELECTION`
   send used a single fire-and-forget RMI lookup per peer; during Docker
   startup, a peer still waiting out its own MySQL connection retries
   hadn't bound its RMI name yet, so the lookup failed silently and the
   message was never delivered (observed for the ChargingStation cluster:
   zero `Sent COORDINATOR announcement` lines were ever logged by CS3, yet
   CS1/CS2 ended up in the correct state only via a separate deterministic-
   bootstrap fallback, not via an actual received message). Fix:
   `Common/BullyElection.lookupPeerWithRetry()`, used for these two
   one-shot, must-be-delivered messages. Re-verified: all 15 instances
   across all 5 clusters now log `Learned new coordinator` on every fresh
   boot.
2. **Slow dead-peer detection in the Manager's background health tick.**
   Root cause: connecting to a `docker stop`-ed (but not removed)
   container can hang on OS-level TCP retransmission far longer than a
   healthy RMI call would, because the container's network attachment
   isn't fully torn down the instant the process stops. Observed: the
   Manager's leader-discovery update for a killed Reservation leader took
   ~75 seconds instead of the expected ~4-8 seconds (measured by wall-clock
   diff between `docker stop` and the `New leader for ...` log line). Fix:
   `Common/NetworkSetup.installBoundedConnectTimeout()`, an
   `RMISocketFactory` bounding the TCP connect phase to 2 seconds, applied
   in the Manager and all 5 server types. Re-verified twice (Reservation
   and ChargingStation kills): leader-discovery update now lands within
   ~12 seconds of the kill.
3. **Harmless but real duplicate-key warning on every Reservation write in
   cluster mode.** Root cause: `fanOutReservationCluster`/
   `fanOutReservationClusterCancel` replicated a change to *every* instance
   in the cluster including the leader that just wrote it, which re-ran
   `INSERT` against a row it had already inserted, producing `[DB] WARNING:
   ... Duplicate entry`. Fix: skip `cfg.currentLeader` in the fan-out loop,
   mirroring the origin-exclusion already used for the other 4 clusters
   (which was needed there to avoid an actual deadlock — see the previous
   session's log entry). Re-verified: the post-failover write in the final
   test run produced zero duplicate-key warnings.

Corrected one inaccurate claim from the previous session's `docs/FAILOVER.md`
draft (written before live Docker rejoin testing was possible): it had
claimed a restarting node checks for an existing coordinator before
asserting leadership. Live testing showed the actual behavior is standard
Bully semantics (highest ID always wins, including on rejoin, with the
outgoing leader demoting cleanly) — the document has been corrected to
describe what was actually observed, not what was assumed.

## Final status

**PASS.** All 21 items in the verification request were completed against
the actual running Docker deployment, with real fixes applied and
re-verified (not weakened or bypassed) for every issue found. See the
Final Verification Report delivered in this session for the itemized
PASS/FAIL table.

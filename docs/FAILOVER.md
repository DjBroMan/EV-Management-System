# Failover

Failover has **two independent mechanisms** in this codebase, kept
deliberately separate to avoid split-brain (see `docs/BULLY_ALGORITHM.md`'s
"why this doesn't conflict" section for the full reasoning):

1. **Legacy 2-node reactive swap** (`ReservationServerManager.performFailoverInternal`,
   unchanged from before this extension) — used when `RESERVATION_INSTANCES`
   is *not* set. Triggered only when a client-facing call throws
   `RemoteException`; unconditionally promotes the single hardcoded
   secondary.
2. **Bully-driven failover** (all 5 clusters, always active per-cluster) —
   proactive, heartbeat-driven, N-instance-capable. This is the mechanism
   described below and is what the Docker Compose deployment actually uses
   for all 5 clusters (`RESERVATION_INSTANCES` is set in `docker-compose.yml`).

## Step-by-step (Bully-driven, matches the request's required sequence exactly)

```
1. Detect failure using heartbeat/health check.
2. Start Bully election.
3. Elect highest available server.
4. Promote it to Primary/Leader.
5. Update Manager.
6. Route subsequent requests to the new leader.
7. Continue using already replicated database state.
```

1. **Detection**: each instance's own `BullyElection` heartbeat loop
   (`Common/BullyElection.java`, every 3s) pings the instance it believes is
   coordinator; 2 consecutive failures trigger `startElection()`. This is
   fully independent per cluster.
2. **Election**: standard Bully (see `docs/BULLY_ALGORITHM.md`) — `ELECTION`
   to higher-ID peers, `OK` responses, `COORDINATOR` broadcast by the winner.
3. **Promotion**: the winning node's `onBecomeCoordinator` callback flips its
   own `role` field to `PRIMARY` in-process — no external call needed, no
   state reconstruction, because the instance already holds its own
   continuously-replicated state.
4. **Manager discovery**: the Manager's own background health monitor
   (`ReservationServerManager.healthAndLeaderDiscoveryTick`, every 4s,
   independent of any single cluster's Bully heartbeat) polls every known
   instance's `getRole()` and updates `ClusterConfig.currentLeader` the
   moment it observes a new `PRIMARY`. It logs
   `"New leader for <cluster> cluster = <prefix>-<id>"` at that moment.
5. **Routing update**: `pickForWrite(cluster)` (used by every write proxy
   method) simply returns `cfg.currentLeader` — the very next write request
   after the Manager's discovery tick is transparently sent to the new
   leader. `EVClient` and every other caller notice nothing except a brief
   delay bounded by the election + discovery timing.
6. **No unnecessary reconstruction**: because every write was already
   synchronously replicated to peers *before* the failure (see
   `docs/REPLICATION.md`), the newly-promoted leader's local state (and its
   own database) is already current — `synchronizeFullState` /
   `applyClusterSnapshot` exist only for disaster recovery / bootstrapping a
   brand-new empty replica, never for ordinary failover.

## Verified live (this session's smoke test)

```
Killed R3 (Reservation primary, PID via `Stop-Process -Force`)
  R1[SECONDARY:1245] log: "Heartbeat to coordinator (3) failed 1x"
  R1[SECONDARY:1245] log: "Reservation-2 announced itself as the new COORDINATOR."
  R2[SECONDARY:1255] log: "No higher-ID peer responded. Reservation-2 ELECTED as new coordinator."
  R2[SECONDARY:1255] log: "This instance is now PRIMARY (coordinator) of the Reservation cluster."
  Manager log: "New leader for Reservation cluster = Reservation-2"
  New client reserveSlot() call -> routed to R2 -> "Reservation successful! ... RES1002 ..."
    (counter continued from RES1001, proving replicated state was reused, not rebuilt)
```

## Bounded failure window under load

`tests/CombinedFailoverLoadTest.java` fires continuous concurrent
`reserveSlot` calls while a human kills the leader mid-run; the expected
result is a small burst of failures clustered exactly around the kill,
followed by 100% success once the Manager's next health-discovery tick
(≤4s) finds the new leader.

## Failback / rejoin (verified live in Docker, corrected from an earlier draft)

A killed-then-restarted instance boots with no memory of its previous role
and computes an initial guess from `PEERS`/`SERVER_ID` alone, **without**
first checking whether a coordinator is already active. This means a
restarted **highest-ID** node reclaims leadership unconditionally — this is
standard Bully semantics (the highest ID always wins, including on
rejoin), not a bug, but it is worth stating precisely because an earlier
draft of this document incorrectly claimed the restarting node checks
before asserting itself.

Verified live: after `docker stop reservation-3` triggered failover to
`Reservation-2`, running `docker start reservation-3` produced:
```
reservation-3: "No higher-ID peer responded. Reservation-3 ELECTED as new coordinator."
reservation-3: "Sent COORDINATOR announcement to Reservation-1" / "...to Reservation-2"
reservation-2: "Reservation-3 announced itself as the new COORDINATOR." -> "Learned new coordinator: Reservation-3"
manager:       "New leader for Reservation cluster = Reservation-3"
```
`Reservation-2` demotes itself cleanly the moment R3's announcement
arrives, and the Manager's health monitor confirms the new leader within
one tick — there is a brief window (bounded by election + broadcast
latency, typically well under a second) where R2 still believes it is
PRIMARY, but no request was observed to be misrouted or lost during this
transition in testing, and no lasting split-brain occurs. If a request
happens to land on R2 in that exact window, it is served correctly by R2
(which is not wrong — R2 legitimately held state current up to that
instant); the next request goes to R3 once the Manager's next discovery
tick runs.

One related fix made during this verification: the Manager's Reservation
cluster-mode replication fan-out (`fanOutReservationCluster`) originally
replicated a write back to the very leader that made it, causing a harmless
but real `[DB] WARNING: ... Duplicate entry` on every write (the leader's
own DAO insert already happened; the fan-out's second insert collided with
the primary key). Fixed by skipping `cfg.currentLeader` in the fan-out loop,
mirroring the origin-exclusion already used for the other 4 clusters.

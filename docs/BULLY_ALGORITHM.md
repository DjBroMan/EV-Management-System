# Bully Algorithm

Implemented in [`Common/BullyElection.java`](../Common/BullyElection.java),
instantiated **independently once per server process** — every one of the
15 cluster instances (ChargingStation ×3, Reservation ×3, ChargingSession
×3, Pricing ×3, Payment ×3) runs its own `BullyElection` object with its own
peer list. There is no global election and no cross-cluster coordination:
Reservation's R1/R2/R3 election never communicates with Pricing's P1/P2/P3
election, even though both may be running an election at the same instant.

This was verified twice, live: once with a manual 17-process multi-JVM run,
and again as full end-to-end testing against the actual `docker compose up`
deployment (32 containers, both a cold-boot election and two separate
`docker stop` failovers — Reservation and ChargingStation — see the Final
Report and `log/2026-09-17_docker-deployment-verification.md`). Both times,
killing a cluster's leader triggered exactly the algorithm below and the
Manager updated its routing target, with zero effect on the other clusters.

## Message types (`Common/ClusterNodeInterface.java`)

- `receiveElection(candidateId, lamport)` — "I think the coordinator is
  down; if you outrank me, take over."
- `receiveOk(fromId, lamport)` — "Acknowledged, I'm alive and higher-ranked
  than you; back off."
- `receiveCoordinator(leaderId, host, port, lamport)` — "I won; I am now the
  coordinator."
- `ping(lamport)` / `getRole(lamport)` — heartbeat + role introspection used
  by both the cluster's own election and the Manager's health monitor.

## Algorithm (standard Bully, unmodified)

1. A node's own heartbeat loop (`BullyElection.heartbeatTick`, every 3s)
   pings the node it currently believes is coordinator.
2. After 2 consecutive missed heartbeats, the node calls `startElection()`.
3. `startElection()` sends `ELECTION` to every peer with a **higher**
   `SERVER_ID` (higher ID = higher priority, exactly as specified).
4. If no peer with a higher ID replies `OK` within 2 seconds, the node
   declares itself coordinator (`declareVictory()`), flips its own role to
   `PRIMARY`, and broadcasts `COORDINATOR` to every peer.
5. If a higher-ID peer does reply `OK`, the node waits up to 3 seconds for a
   `COORDINATOR` announcement; if none arrives (e.g. that peer also failed
   mid-election), it restarts its own election.
6. A node that receives `ELECTION` from a lower ID immediately replies `OK`
   and starts its own election (standard Bully requirement — the higher-ID
   node must also verify it should be the winner, not just concede).
7. A node that receives `COORDINATOR` adopts the sender as the new leader
   and demotes itself to `SECONDARY` if it isn't the winner.

## Worked example (matches the request's own example exactly)

```
Reservation cluster: R1 (id=1, :1245), R2 (id=2, :1255), R3 (id=3, :1235)

Startup: no cluster member believes anyone is coordinator yet, so each
node's own heartbeat tick fires an election within ~3s of boot. R3 (highest
id) finds no higher peer, declares itself PRIMARY, and broadcasts
COORDINATOR to R1 and R2. R1/R2 adopt R3 as leader and set role=SECONDARY.

R3 fails (killed):
  R1's heartbeat to R3 fails twice -> R1 starts an election
  R1 sends ELECTION to R2 (the only higher id, since R3 is dead)
  R2 replies OK, then starts its OWN election (Bully requires this)
  R2 finds no higher peer (R3 unreachable) -> R2 declares itself coordinator
  R2 broadcasts COORDINATOR to R1
  R1 learns the new coordinator = R2
  Manager's health monitor discovers R2.getRole()=="PRIMARY" on its next
    tick and logs: "New leader for Reservation cluster = Reservation-2"
  Manager routes all subsequent writes to R2; the cluster continues serving
    requests using R2's already-replicated state (no reconstruction).
```

This is exactly what the live Docker test produced (`docker stop
reservation-3`; R1 detected the failure, R2 won, the Manager updated to
`Reservation-2` within ~12 seconds of the kill, and a subsequent
`reserveSlot` request succeeded transparently against R2 — see the Final
Report and `log/2026-09-17_docker-deployment-verification.md`).

## Reliability fixes made during Docker verification

Two real issues surfaced only under actual container networking (not
visible in the earlier same-host manual multi-JVM test) and were fixed:

1. **Lost COORDINATOR/ELECTION messages during cold boot.** `broadcastCoordinator()`
   and the `ELECTION` send originally did a single fire-and-forget RMI
   lookup per peer. During Docker cold-start, a peer whose own MySQL
   connection retries were still in progress hadn't bound its RMI name yet,
   so the lookup failed silently and the message was lost — observed live
   for the ChargingStation cluster's initial election (the receivers still
   ended up correct only because of the separate deterministic-bootstrap
   fallback, not because they actually received the message). Fixed with
   `lookupPeerWithRetry` (a few retries with a short pause) for these two
   one-shot, must-be-delivered messages specifically; the periodic heartbeat
   ping intentionally keeps the fast, non-retrying lookup since it already
   retries every cycle by design.
2. **Slow dead-peer detection in the Manager's background health tick.**
   Connecting to a `docker stop`-ed (but not removed) container can hang on
   OS-level TCP retransmission far longer than a healthy RMI call ever
   would, because the container's network attachment isn't fully torn down
   immediately. This turned an expected ~4-8s leader-discovery delay into an
   observed ~75s delay on the first test run. Fixed with
   `Common/NetworkSetup.installBoundedConnectTimeout()`, an `RMISocketFactory`
   that bounds the TCP connect phase to 2 seconds, installed by all 5
   server types and the Manager. Re-tested after the fix: both a
   Reservation and a ChargingStation kill were detected and routed around
   within ~12 seconds.

## Higher ID = higher priority, applied consistently

`SERVER_ID` is an explicit environment variable (1/2/3 per cluster,
independent of the other 4 clusters — cluster boundaries are what matters,
not global uniqueness of the number). No other election algorithm (Ring,
Chang-Roberts, Raft, etc.) is used anywhere in the codebase.

## Why this doesn't conflict with the legacy 2-node Reservation failover

The original `ReservationServerManager` 2-node hardcoded primary/secondary
swap (`docs/REPLICATION.md`) still exists, unmodified, and is the *default*
behavior (`RESERVATION_INSTANCES` unset). The new 3-node Bully path is
strictly opt-in via `RESERVATION_INSTANCES` (set in `docker-compose.yml`),
so exactly one mechanism drives real routing decisions at a time — running
both simultaneously against the same 3 nodes would risk the Manager's
reactive 2-node swap and the cluster's own Bully election disagreeing about
who is Primary (split-brain). See
`docs/DISTRIBUTED_SYSTEM_ROADMAP.md` and the `reservationClusterMode` flag
in `ReservationServerManager.java` for the full reasoning.

# State Replication in the EV Charging Network

This document describes **two replication mechanisms** that now coexist in
the codebase:

1. The **original 2-node Reservation primary-backup mechanism** (sections
   1–11 below), preserved essentially unchanged from before this
   distributed-system extension — it is still the default when
   `RESERVATION_INSTANCES` is unset.
2. The **generalized N-instance mechanism** added for ChargingStation,
   ChargingSession, Payment, and (optionally, via `RESERVATION_INSTANCES`)
   Reservation itself (section 12), which fans a change out to every peer
   in a cluster via the Manager instead of a single hardcoded secondary.

Both mechanisms replicate **individual changes**, never a full
delete-all/insert-all on every write — full-state snapshot transfer
(`getStateSnapshot`/`synchronizeFullState` and their generalized
counterparts `getClusterSnapshot`/`applyClusterSnapshot`) is reserved for
disaster recovery / bootstrapping a brand-new empty replica, exactly as
required.

---

## 1. What Replication Means

State replication is the process of sharing and copying distributed system
state across multiple compute nodes to ensure data redundancy, high
availability, and fault tolerance. State is mirrored synchronously from a
**PRIMARY** node to its **SECONDARY** replica(s) on every write.

## 2. Why Replication is Used

- If a Primary fails, a Secondary replica can immediately be promoted to
  take over client requests without losing prior state.
- Clients experience continuous availability without duplicate resource
  allocation.
- Both in-memory state *and* each instance's own MySQL database
  (`docs/DATABASE_SCHEMA.md`) are kept consistent with each other — the
  database is the persistent source of truth; in-memory maps are a
  runtime cache loaded from it at startup and never allowed to silently
  diverge from it (every state-changing RMI method persists to its own DAO
  in the same call that mutates the in-memory maps).

## 3. Primary-Secondary Architecture (original 2-node Reservation path)

```
                         EVClient / MultithreadTest
                                     |
                                     | ReservationInterface (RMI, via Manager)
                                     v
                          +----------------------+
                          | ReservationServer    |
                          | PRIMARY (R3, :1235)  |
                          +----------+-----------+
                                     |
                                     | State update (RMI)
                                     v
                          +----------------------+
                          | ReservationServerManager (Manager, :1240) |
                          +----------+-----------+
                                     |
                                     | ReservationReplicationInterface (RMI)
                                     v
                          +----------------------+
                          | ReservationServer    |
                          | SECONDARY (R1, :1245)|
                          +----------------------+
```

1. **PRIMARY** accepts client reservation requests, calls
   `ChargingStationServer` (now routed via the Manager) to reserve a
   physical port, updates local state, persists to its own DB, and
   synchronously notifies the Manager.
2. **SECONDARY** never processes client writes directly; it only applies
   replicated updates and persists them to its *own* DB instance.
3. **ReservationServerManager (Manager)** dispatches replication, runs
   health checks, and performs failover promotion.

Both roles are the *same* `ReservationServer.java` class — role is an
internal enum (`Role.PRIMARY`/`Role.SECONDARY`) selected at startup via CLI
arg (`primary`/`secondary`) or `RESERVATION_ROLE` env var.

## 4. `ReservationReplicationInterface` (unchanged)

- `applyReservationUpdate(...)` — stores a replicated reservation + advances
  the counter + persists to the secondary's own DB.
- `applyCancellationUpdate(...)` — removes a cancelled reservation + deletes
  from DB.
- `synchronizeFullState(...)` / `getStateSnapshot(...)` — full
  reservations-map transfer, used only for disaster recovery.
- `ping(...)` / `getRole(...)` / `promoteToPrimary(...)` — health + failover.

## 5. Sequence: reserving a slot

```
EVClient      Manager        Primary          Manager        Secondary       ChargingStation (via Manager)
   |             |               |                |               |                    |
   |--reserveSlot(via Manager)-->|                |               |                    |
   |             |               |--reserveAnyAvailablePort()------------------------->|
   |             |               |<--allocated P1---------------------------------------|
   |             |               |[reservations.put(RES1001,...)]                       |
   |             |               |[DB insert]     |               |                    |
   |             |               |--replicateReservation-------->|                     |
   |             |               |                |--applyReservationUpdate---------->|
   |             |               |                |               |[DB insert]        |
   |             |               |                |<--ack----------|                   |
   |             |               |<--ack-----------|               |                    |
   |<--CONFIRMED (RES1001)-------|                |               |                    |
```

Counters (`RES1001`, `RES1002`, ...) are monotonic and recovered from
`SELECT MAX(...)` on the database at startup, so a promoted secondary never
collides with IDs the old primary already issued.

## 6. Full State Synchronization (disaster recovery only)

`ReservationServerManager.triggerFullSynchronization()` pulls a complete
snapshot from the Primary and pushes it to the Secondary — used when a
replica falls behind or starts from empty, **not** on every normal write.

## 7. Failover & Promotion

See `docs/FAILOVER.md` for the full step-by-step (both the legacy 2-node
reactive path and the new Bully-driven N-instance path).

## 8. Lamport Logical Clock Integration

Every replication RPC is a distinct distributed event obeying Lamport clock
rules end-to-end (send/receive/local ticks on both sides of every hop), and
every log line prints the resulting Lamport value alongside the physical
(Cristian-corrected) timestamp. See `docs/RMI_COMMUNICATION.md` for the
propagation rules and `Clock/LogicalClock.java` for the implementation
(unchanged by this extension).

## 9. Multithreading & Concurrency Protection

In-memory maps and counters remain protected by `synchronized` blocks;
`LogicalClock` remains lock-free (`AtomicLong` CAS). Unchanged by this
extension — see `docs/MULTITHREADING.md`.

## 10. Database Persistence (supersedes the old "no DBMS" note)

Every one of the 15 cluster instances persists to its own MySQL 8.0
database (`docs/DATABASE_SCHEMA.md`) via a DAO class, using
`PhysicalClock.getSynchronizedPhysicalTimeMillis()` for every timestamp
column — MySQL `CURRENT_TIMESTAMP` is never used, so timestamps stay
consistent with the Cristian-corrected application clock across container
clock skew. Every server degrades gracefully to pure in-memory operation if
its database is unreachable at startup.

## 11. Known limitation retained from the original design

If *every* replica of a cluster is terminated simultaneously with no
surviving database, all state for that cluster is lost — MySQL persistence
mitigates this significantly (state survives individual process restarts as
long as at least the database container is intact) but does not eliminate a
total-cluster-plus-database wipeout as a limitation.

---

## 12. Generalized N-instance replication (ChargingStation, ChargingSession,
Payment, and optionally Reservation)

Extending the concept above to a cluster of N instances (not just 2) is
handled generically instead of writing 4 bespoke Manager methods:

```
PRIMARY (write applied locally + persisted to own DB)
   |
   | ClusterManagerClient.replicate(serviceName, originServerId, delta, clock)
   v
Manager.replicateUpdate(serviceName, originServerId, delta, lamport)
   |
   | fans out to every OTHER instance in that cluster (never back to
   | originServerId -- see "why exclude the origin" below)
   v
Peer.applyUpdate(delta, lamport)   [Common/ClusterNodeInterface]
   |
   v
Peer's own DB (persisted via its own DAO)
```

- `Common/StateDelta.java` is a generic `(opType, args...)` payload —
  `"PORT_STATUS"`, `"SESSION_START"`, `"SESSION_STOP"`, `"PAYMENT_INSERT"`,
  `"RESERVATION_UPSERT"`, `"RESERVATION_DELETE"` — so one wire format
  covers every cluster's individual-change replication without a bespoke
  DTO class per service.
- `Common/GenericSnapshot.java` is the generic full-state-transfer
  counterpart (disaster recovery only), a `Map<String,Serializable>`
  keyed by field name.
- **Why the origin instance is excluded from fan-out**: `ChargingStationServer`'s
  write methods are `synchronized` at the *method* level. Fanning a change
  back to the very instance that just wrote it means that instance's own
  incoming `applyUpdate` (also `synchronized` on the same object) would
  block waiting for a monitor its own `reservePort`/`releasePort`/etc. call
  is still holding on a *different* RMI thread — a genuine self-deadlock,
  which was caught and fixed during live testing of this feature (see the
  `log/` changelog entry). `replicateUpdate(serviceName, originServerId,
  delta, lamport)` therefore always skips `originServerId` — the origin
  already has the change applied locally, so this is also simply correct,
  not just a deadlock workaround.
- Pricing has no write-replication path at all: no client call ever
  mutates its state, so every instance is an independent read replica
  loaded from its own database (see `docs/DATABASE_SCHEMA.md`).

### Reservation's opt-in cluster mode

`ReservationServerManager.reservationClusterMode` (set when
`RESERVATION_INSTANCES` is configured, as it is in `docker-compose.yml`)
switches Reservation's `reserveSlot`/`cancelReservation`/`getReservation`/
`getReservationPort` proxy methods from the legacy hardcoded 2-node swap
onto the same generalized leader-discovery + round-robin path as the other
4 clusters, and switches `replicateReservation`/`replicateCancellation`
from the fixed-secondary assumption to fanning out across the full
`RESERVATION_INSTANCES` list. See `docs/BULLY_ALGORITHM.md` for why the two
mechanisms are never active at once against the same nodes.

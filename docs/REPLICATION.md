# Primary-Backup State Replication in EV Charging Network

## 1. What Replication Means
State replication is the process of sharing and copying distributed system state across multiple compute nodes to ensure data redundancy, high availability, and fault tolerance. In this project, the in-memory reservation state of the EV Charging Network Management System is mirrored synchronously from a **PRIMARY** node to a **SECONDARY** backup node.

---

## 2. Why Replication is Used
In distributed systems, individual node crashes or network partitions can disrupt critical services. By maintaining an up-to-date backup of the reservation state:
- If the Primary server fails, the Secondary replica can immediately be promoted to take over client requests without losing prior reservations.
- Clients experience continuous availability without duplicate resource allocation.
- In-memory data structures are safeguarded against single-node failures.

> [!NOTE]
> The project uses in-memory primary-secondary state replication because persistent database storage is not required by the assignment.

---

## 3. Primary-Secondary Architecture
The replication architecture consists of three decoupled components:

```
                         EVClient / MultithreadTest
                                     |
                                     | ReservationInterface (RMI)
                                     v
                          +----------------------+
                          | ReservationServer    |
                          | PRIMARY              |
                          | RMI :1235 (Exp: 2235)|
                          +----------+-----------+
                                     |
                                     | State update (RMI)
                                     v
                          +----------------------+
                          | ReservationServer    |
                          | Manager              |
                          | RMI :1240 (Exp: 2240)|
                          +----------+-----------+
                                     |
                                     | ReservationReplicationInterface (RMI)
                                     v
                          +----------------------+
                          | ReservationServer    |
                          | SECONDARY            |
                          | RMI :1245 (Exp: 2245)|
                          +----------------------+
```

1. **ReservationServer Instance #1 (PRIMARY, Port 1235 / Export 2235)**:
   - Accepts client reservation requests (`reserveSlot`, `cancelReservation`, `getReservation`, `getReservationPort`).
   - Communicates with `ChargingStationServer` (Port 1234) to reserve physical ports.
   - Updates local in-memory state.
   - Synchronously notifies `ReservationServerManager` of state mutations.

2. **ReservationServer Instance #2 (SECONDARY, Port 1245 / Export 2245)**:
   - Does not independently process active client reservations while in `SECONDARY` role.
   - Receives replicated state updates from `ReservationServerManager`.
   - Stores resulting state mapping (`reservationId -> details`, `reservationId -> portId`) and advances `reservationCounter`.
   - Never calls `ChargingStationServer.reserveAnyAvailablePort()` directly during replication.
   - Can be promoted to `PRIMARY` role dynamically during failover.

3. **ReservationServerManager (Coordinator, Port 1240 / Export 2240)**:
   - Intermediary coordinator managing replication dispatches, heartbeat monitoring, full state synchronization, and failover promotion.

---

## 4. Single Server Class with Dual Roles (`ReservationServer.java`)
Both the Primary and Secondary instances are instantiated directly from the **same** [`ReservationServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Reservation/ReservationServer.java) class. No duplicate `ReservationBackupServer.java` is created.

The server's role is governed by an internal enum (`Role.PRIMARY` vs `Role.SECONDARY`), configurable at startup via CLI parameters:
- `java ReservationServer primary 1235`
- `java ReservationServer secondary 1245`

Or via environment variables (`RESERVATION_ROLE=PRIMARY|SECONDARY`, `RMI_REGISTRY_PORT=1235|1245`).

---

## 5. Role of `ReservationServerManager`
The manager coordinates replication without performing reservation business logic:
- Tracks Primary and Secondary network addresses.
- Synchronously dispatches state updates (`replicateReservation`, `replicateCancellation`) to Secondary.
- Pulls point-in-time snapshots for full synchronization.
- Pings replicas and triggers automated failover promotion when Primary failure is detected.

---

## 6. Role of `ReservationReplicationInterface`
A dedicated RMI remote interface ([`ReservationReplicationInterface.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Reservation/ReservationReplicationInterface.java)) is used for manager-to-replica communication:
- `applyReservationUpdate(...)`: Stores replicated reservation and synchronizes counter.
- `applyCancellationUpdate(...)`: Removes cancelled reservation from replica maps.
- `synchronizeFullState(...)`: Ingests a complete [`ReservationStateSnapshot`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Reservation/ReservationStateSnapshot.java).
- `getStateSnapshot(...)`: Returns current in-memory maps and counter.
- `ping(...)`: Health check probe.
- `promoteToPrimary(...)`: Transitions replica role from `SECONDARY` to `PRIMARY`.
- `getRole(...)`: Returns current active role.

---

## 7. How a Reservation is Replicated
Replication copies the **resulting state**, not the operation execution:

```
EVClient                  Primary                     Manager                     Secondary              ChargingStation
   |                         |                           |                            |                        |
   |--- reserveSlot(req) --->|                           |                            |                        |
   |                         |--- reserveAnyPort() ----------------------------------------------------------->|
   |                         |<-- allocated P1 ----------------------------------------------------------------|
   |                         | [Update local maps]       |                            |                        |
   |                         | [RES1001 -> EV1 -> P1]    |                            |                        |
   |                         |--- replicate(RES1001) --->|                            |                        |
   |                         |                           |--- applyUpdate(RES1001) -->|                        |
   |                         |                           |                            | [Store RES1001 -> P1]  |
   |                         |                           |<-- ack --------------------|                        |
   |                         |<-- replication success ---|                            |                        |
   |<-- CONFIRMED (RES1001) -|                           |                            |                        |
```

1. EVClient sends `reserveSlot("USER-1", "EV-1")` to Primary (Port 1235).
2. Primary invokes `ChargingStationServer.reserveAnyAvailablePort()`, receiving allocated port `P1`.
3. Primary updates local in-memory state: `reservations.put("RES1001", details)` and `reservationPorts.put("RES1001", "P1")`.
4. Primary calls `ReservationServerManager.replicateReservation("RES1001", details, "P1", counter, sendL)`.
5. Manager invokes `Secondary.applyReservationUpdate("RES1001", details, "P1", counter, sendL2)`.
6. Secondary stores `RES1001 -> P1` in its internal HashMap and updates its `reservationCounter = counter + 1`.
7. Secondary acknowledges to Manager; Manager acknowledges to Primary.
8. Primary returns success response to EVClient.

---

## 8. How Cancellation is Replicated
1. EVClient sends `cancelReservation("RES1001")` to Primary.
2. Primary removes `RES1001` from local maps and invokes `ChargingStationServer.releasePort("P1")`.
3. Primary calls `ReservationServerManager.replicateCancellation("RES1001", sendL)`.
4. Manager invokes `Secondary.applyCancellationUpdate("RES1001", sendL2)`.
5. Secondary removes `RES1001` from its in-memory maps.
6. Manager returns confirmation to Primary; Primary returns success to EVClient.

---

## 9. Full State Synchronization
When a Secondary replica starts after Primary has already processed reservations or recovers from a disconnection:
1. `ReservationServerManager.triggerFullSynchronization()` is invoked.
2. Manager requests a snapshot from Primary via `getStateSnapshot()`.
3. Primary packages its `reservations`, `reservationPorts`, and `reservationCounter` into a serializable [`ReservationStateSnapshot`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Reservation/ReservationStateSnapshot.java).
4. Manager delivers the snapshot to Secondary via `synchronizeFullState(snapshot)`.
5. Secondary atomically replaces its state and synchronizes its counter.

---

## 10. Failover & Promotion
If Primary crashes or becomes unreachable:
1. Manager detects failure via heartbeat / ping or failed replication call (`checkAndFailover()`).
2. Manager logs `PRIMARY SERVER FAILURE DETECTED!` and sends `promoteToPrimary()` to Secondary.
3. Secondary executes promotion:
   - Updates role: `role = Role.PRIMARY`.
   - Connects to `ChargingStationServer` (Port 1234).
   - Enables client request processing.
4. The promoted node now processes new client reservations (`reserveSlot`, `cancelReservation`) and queries (`getReservation`, `getReservationPort`).
5. Monotonically increasing `reservationCounter` guarantees new reservations (e.g., `RES1002`, `RES1003`) continue seamlessly without ID collisions.

---

## 11. Lamport Logical Clock Integration
Every replication interaction represents a distinct distributed event obeying Lamport clock rules:
- Primary state update: $L = L_{\text{local}} + 1$, sends `repSendL` ($L+1$).
- Manager receive: $L_{\text{mgr}} = \max(L_{\text{mgr}}, \text{repSendL}) + 1$.
- Manager forward: $L = L_{\text{mgr}} + 1$, sends `fwdSendL` ($L+1$).
- Secondary receive: $L_{\text{sec}} = \max(L_{\text{sec}}, \text{fwdSendL}) + 1$.
- Secondary ack: $L = L_{\text{sec}} + 1$.
- Manager receive ack & send confirmation to Primary.

All events are logged in ASCII format with physical timestamps, Lamport timestamps, server tags, thread names, and event types (`RECEIVE`, `LOCAL`, `SEND`).

---

## 12. Multithreading & Concurrency Protection
- In-memory Maps (`reservations`, `reservationPorts`) and `reservationCounter` mutations are synchronized using mutex blocks.
- Non-blocking read operations and concurrent client threads can safely query or reserve without data corruption.
- `LogicalClock` uses atomic lock-free CAS (`AtomicLong`) for thread-safe timestamp management.

---

## 13. Why No DBMS is Used
- Persistent database storage (SQL, MySQL, PostgreSQL, MongoDB, JDBC) is not required for this college project assignment.
- Pure Java in-memory replication demonstrates core distributed computing principles (RMI, clock synchronization, primary-backup protocols, and failover coordination) with zero external database dependencies.

---

## 14. Known Limitations
1. **ChargingStationServer State**: Only `ReservationServer` is replicated. `ChargingStationServer` maintains its own port allocation state in-memory on port 1234. If `ChargingStationServer` crashes, physical port state must be recovered separately.
2. **Volatile In-Memory Lifecycle**: If both Primary and Secondary nodes are terminated simultaneously, all state is lost as no disk persistence is maintained.

# EV Charging Network Management System — State Management & Transitions

## Overview
State management in this system covers seven distinct entities:
1. **Charging Port State** (managed by `ChargingStationServer`)
2. **Reservation State** (managed by `ReservationServer` PRIMARY and replicated to SECONDARY)
3. **Replication State Machine** (managed by `ReservationServerManager`)
4. **Charging Session State** (managed by `ChargingSessionServer`)
5. **Payment State** (managed by `PaymentServer`)
6. **Lamport Logical Clock State** (maintained independently per server via `LogicalClock`)
7. **Cristian Physical Clock Offset State** (maintained via `PhysicalClock`)

---

## 1. Charging Port State Machine

```
   AVAILABLE  ────────►  RESERVED  ────────►  CHARGING  ────────►  AVAILABLE
 (Initial State)       (reserveSlot)        (startCharging)      (makePayment SUCCESS)
```

| Current State | Target State | Trigger Method | Controlling Server | Description |
|---------------|--------------|----------------|--------------------|-------------|
| `AVAILABLE` | `RESERVED` | `reserveAnyAvailablePort()` | `ChargingStationServer` | Port is checked and reserved for a specific reservation. |
| `RESERVED` | `CHARGING` | `startPortCharging(portId)` | `ChargingStationServer` | Vehicle plugs in and begins charging. |
| `CHARGING` | `AVAILABLE` | `releasePort(portId)` | `ChargingStationServer` | **Triggered only by `PaymentServer` after successful payment.** Port is freed for future use. |
| `RESERVED` | `AVAILABLE` | `releasePort(portId)` | `ChargingStationServer` | Triggered by `ReservationServer` if reservation is cancelled before charging starts. |

---

## 2. Reservation State Machine (Replicated)

```
   REQUESTED  ────────►  CONFIRMED (Primary + Secondary)  ────────►  USED / COMPLETED
                              (reserveSlot)                             (startCharging)
                                   │
                                   ▼
                       CANCELLED (Primary + Secondary)
                             (cancelReservation)
```

| Current State | Target State | Trigger Method | Controlling Server | Replication Behavior |
|---------------|--------------|----------------|--------------------|----------------------|
| `REQUESTED` | `CONFIRMED` | `reserveSlot(userId, vehicleId)` | `ReservationServer` (PRIMARY) | Primary allocates port, saves local state, and invokes Manager $\rightarrow$ Secondary to replicate `RES1001 -> portId`. |
| `CONFIRMED` | `USED` / `COMPLETED` | `startCharging(reservationId)` | `ChargingSessionServer` | Reservation validated and consumed to launch charging session. |
| `CONFIRMED` | `CANCELLED` | `cancelReservation(reservationId)` | `ReservationServer` (PRIMARY) | Primary releases port, removes local state, and invokes Manager $\rightarrow$ Secondary to replicate removal. |

---

## 3. Replication Node State Machine

```
   SECONDARY (Passive Replica) ────────► PRIMARY (Active Node)
                               (promoteToPrimary)
```

- **`SECONDARY`**: Rejects direct client write requests; accepts replication and synchronization updates.
- **`PRIMARY`**: Directly handles client requests, performs physical port allocation via `ChargingStationServer`, and synchronizes state to replicas via `ReservationServerManager`.

---

## 4. Charging Session State Machine

```
   CHARGING  ───────────────────────────────►  COMPLETED
(startCharging)                              (stopCharging)
                                         (Port remains locked)
```

| Current State | Target State | Trigger Method | Controlling Server | Description |
|---------------|--------------|----------------|--------------------|-------------|
| `CHARGING` | `COMPLETED` | `stopCharging(sessionId)` | `ChargingSessionServer` | Session marked COMPLETED, energy consumed calculated from real physical duration. Port is **NOT** released. |

---

## 5. Payment State Machine

```
   PENDING  ────────────────────────────────►  SUCCESS
(Session Completed)                          (makePayment)
                                         (Port released to AVAILABLE)
```

| Current State | Target State | Trigger Method | Controlling Server | Description |
|---------------|--------------|----------------|--------------------|-------------|
| `PENDING` | `SUCCESS` | `makePayment(sessionId)` | `PaymentServer` | Session verified `COMPLETED`, energy retrieved, price calculated, receipt stored as `SUCCESS`, port released to `AVAILABLE`. |

---

## 6. Lamport Logical Clock & Physical Clock Offset State

| Clock Entity | State Variable | Update Mechanism | Synchronization Guarantee |
|--------------|----------------|------------------|---------------------------|
| `LogicalClock` | `AtomicLong clock` | `tick()` ($L+1$), `sendEvent()` ($L+1$), `receiveEvent()` ($\max(L, \text{recv})+1$) | Atomic lock-free CAS thread-safety |
| `PhysicalClock` | `volatile long clockOffsetMs` | Cristian algorithm ($T_1 + RTT/2 - \text{LocalTime}$) | Volatile visibility across server threads |

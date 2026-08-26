# EV Charging Network Management System — State Management & Transitions

## Overview
State management in this system covers six distinct entities:
1. **Charging Port State** (managed by `ChargingStationServer`)
2. **Reservation State** (managed by `ReservationServer`)
3. **Charging Session State** (managed by `ChargingSessionServer`)
4. **Payment State** (managed by `PaymentServer`)
5. **Lamport Logical Clock State** (maintained independently per server via `LogicalClock`)
6. **Cristian Physical Clock Offset State** (maintained via `PhysicalClock`)

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

## 2. Reservation State Machine

```
   REQUESTED  ────────►  CONFIRMED  ────────►  USED / COMPLETED
                        (reserveSlot)           (startCharging)
                             │
                             ▼
                         CANCELLED
                    (cancelReservation)
```

| Current State | Target State | Trigger Method | Controlling Server | Description |
|---------------|--------------|----------------|--------------------|-------------|
| `REQUESTED` | `CONFIRMED` | `reserveSlot(userId, vehicleId)` | `ReservationServer` | Valid user and available port confirmed; reservation record created. |
| `CONFIRMED` | `USED` / `COMPLETED` | `startCharging(reservationId)` | `ChargingSessionServer` | Reservation validated and consumed to launch charging session. |
| `CONFIRMED` | `CANCELLED` | `cancelReservation(reservationId)` | `ReservationServer` | User cancels reservation; allocated port released back to `AVAILABLE`. |

---

## 3. Charging Session State Machine

```
   CHARGING  ───────────────────────────────►  COMPLETED
(startCharging)                              (stopCharging)
                                         (Port remains locked)
```

| Current State | Target State | Trigger Method | Controlling Server | Description |
|---------------|--------------|----------------|--------------------|-------------|
| `CHARGING` | `COMPLETED` | `stopCharging(sessionId)` | `ChargingSessionServer` | Session marked COMPLETED, energy consumed recorded (`25.0 kWh`). Port is **NOT** released. |

---

## 4. Payment State Machine

```
   PENDING  ────────────────────────────────►  SUCCESS
(Session Completed)                          (makePayment)
                                         (Port released to AVAILABLE)
```

| Current State | Target State | Trigger Method | Controlling Server | Description |
|---------------|--------------|----------------|--------------------|-------------|
| `PENDING` | `SUCCESS` | `makePayment(sessionId)` | `PaymentServer` | Session verified `COMPLETED`, energy retrieved, price calculated, receipt stored as `SUCCESS`, port released to `AVAILABLE`. |

---

## 5. Lamport Logical Clock & Physical Clock Offset State

| Clock Entity | State Variable | Update Mechanism | Synchronization Guarantee |
|--------------|----------------|------------------|---------------------------|
| `LogicalClock` | `AtomicLong clock` | `tick()` ($L+1$), `sendEvent()` ($L+1$), `receiveEvent()` ($\max(L, \text{recv})+1$) | Atomic lock-free CAS thread-safety |
| `PhysicalClock` | `volatile long clockOffsetMs` | Cristian algorithm ($T_1 + RTT/2 - \text{LocalTime}$) | Volatile visibility across server threads |

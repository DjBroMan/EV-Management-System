# EV Charging Network Management System — Architecture Overview

## Executive Summary
This project implements a **Distributed EV Charging Network Management System** built with **Java RMI**, **Docker Compose**, **Lamport Logical Clocks**, **Cristian's Physical Clock Synchronization Algorithm**, **Real-Time Physical Session Duration Energy Calculation**, and **Primary-Backup In-Memory State Replication**.

The system manages physical charging ports, replicated slot reservations, charging sessions, real-time energy calculation ($E = P \times T$), dynamic pricing calculations, and payment settlements across decoupled microservice servers and a reference time server.

---

## Architectural Principles & Replication Topology

```
+-----------------------------------------------------------------------------------------+
|                                    DISTRIBUTED SYSTEM                                   |
|                                                                                         |
|  +-------------------+        +-------------------+                                     |
|  |     TimeServer    |        |  ChargingStation  |                                     |
|  |   (Port 1239/2239)|        |  (Port 1234/2234) |                                     |
|  +---------^---------+        +---------^---------+                                     |
|            |                            |                                               |
|            | Cristian Sync              | RMI + Lamport                                 |
|            |                            |                                               |
|  +---------+---------+        +---------+---------+                                     |
|  | ReservationServer |        |ChargingSessionSrv | (Instant.now())                     |
|  | PRIMARY (1235/2235)|        |  (Port 1236/2236) +---+ Energy =                       |
|  +---------+---------+        +---------+---------+   | Power * Time                    |
|            | State Update               |             +---------------+                 |
|            v                            | RMI + Lamport (Calculated kWh)                |
|  +-------------------+                  v                                               |
|  |ReservationManager |        +-------------------+                                     |
|  |   (Port 1240/2240)|        |   PricingServer   |                                     |
|  +---------+---------+        |  (Port 1238/2238) |                                     |
|            | Replication RMI  +---------^---------+                                     |
|            v                            |                                               |
|  +-------------------+                  | RMI + Lamport                                 |
|  | ReservationServer |                  |                                               |
|  |SECONDARY(1245/2245)|        +---------+---------+                                     |
|  +-------------------+        |   PaymentServer   |                                     |
|                               |   (Port 1237/2237)|                                     |
|                               +-------------------+                                     |
+-----------------------------------------------------------------------------------------+
```

### 1. Dual Clock Abstraction
- **Physical Clock (`java.time.Instant.now()`)**: Measures actual elapsed physical charging duration ($T_{\text{end}} - T_{\text{start}}$) hooked by `libfaketime` inside Docker containers. Physical time is used EXCLUSIVELY for energy calculation ($E = P \times T$).
- **Lamport Logical Clock (`LogicalClock.java`)**: Manages logical event ordering across independent distributed servers and clients using lock-free `AtomicLong` CAS state updates (`tick`, `sendEvent`, `receiveEvent`). Lamport time is used EXCLUSIVELY for event ordering, never for duration calculations.

### 2. Primary-Backup State Replication
- **Single Implementation**: Both Primary (:1235) and Secondary (:1245) instances use the same `ReservationServer.java` class with distinct role configurations.
- **Result-State Mirroring**: Replication copies confirmed states (`reservationId -> userId -> portId`, `reservationCounter`) without repeating physical port reservations on `ChargingStationServer`.
- **Failover Promotion**: In the event of Primary failure, `ReservationServerManager` detects unreachability and promotes the Secondary replica to active Primary.

---

## Energy & Pricing Formulas

1. **Charging Duration**:
   $$\text{Duration (seconds)} = \text{Duration.between}(T_{\text{start}}, T_{\text{end}}).\text{toMillis}() / 1000.0$$

2. **Energy Consumed**:
   $$\text{Energy (kWh)} = \text{Charging Power (7.2 kW)} \times \frac{\text{Duration (seconds)}}{3600.0}$$

3. **Total Bill**:
   $$\text{Bill (Rs.)} = \text{Base Price (Rs. 10.0/kWh)} \times \text{Energy (kWh)} \times \text{Demand Multiplier}$$

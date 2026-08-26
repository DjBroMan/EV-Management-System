# EV Charging Network Management System — Architecture Overview

## Executive Summary
This project implements a **Distributed EV Charging Network Management System** built with **Java RMI**, **Docker Compose**, **Lamport Logical Clocks**, **Cristian's Physical Clock Synchronization Algorithm**, and **Real-Time Physical Session Duration Energy Calculation**.

The system manages physical charging ports, slot reservations, charging sessions, real-time energy calculation ($E = P \times T$), dynamic pricing calculations, and payment settlements across five decoupled microservice servers and one reference time server.

---

## Architectural Principles & Clock Dualism

```
+-----------------------------------------------------------------------+
|                            DISTRIBUTED SYSTEM                         |
|                                                                       |
|  +-------------------+        +-------------------+                   |
|  |     TimeServer    |        |  ChargingStation  |                   |
|  |   (Port 1239/2239)|        |  (Port 1234/2234) |                   |
|  +---------^---------+        +---------^---------+                   |
|            |                            |                             |
|            | Cristian Sync              | RMI + Lamport               |
|            |                            |                             |
|  +---------+---------+        +---------+---------+                   |
|  | ReservationServer |        |ChargingSessionSrv | (Instant.now())   |
|  |   (Port 1235/2235)|        |  (Port 1236/2236) +---+ Energy =     |
|  +---------+---------+        +---------+---------+   | Power * Time  |
|            |                            |             +---------------+
|            | RMI + Lamport              | RMI + Lamport (Calculated kWh)
|            v                            v                             |
|  +-------------------+        +-------------------+                   |
|  |   PaymentServer   +------->|   PricingServer   |                   |
|  |   (Port 1237/2237)|        |  (Port 1238/2238) |                   |
|  +-------------------+        +-------------------+                   |
+-----------------------------------------------------------------------+
```

### 1. Dual Clock Abstraction
- **Physical Clock (`java.time.Instant.now()`)**: Measures actual elapsed physical charging duration ($T_{\text{end}} - T_{\text{start}}$) hooked by `libfaketime` inside Docker containers. Physical time is used EXCLUSIVELY for energy calculation ($E = P \times T$).
- **Lamport Logical Clock (`LogicalClock.java`)**: Manages logical event ordering across independent distributed servers and clients using lock-free `AtomicLong` CAS state updates (`tick`, `sendEvent`, `receiveEvent`). Lamport time is used EXCLUSIVELY for event ordering, never for duration calculations.

---

## Energy & Pricing Formulas

1. **Charging Duration**:
   $$\text{Duration (seconds)} = \text{Duration.between}(T_{\text{start}}, T_{\text{end}}).\text{toMillis}() / 1000.0$$

2. **Energy Consumed**:
   $$\text{Energy (kWh)} = \text{Charging Power (7.2 kW)} \times \frac{\text{Duration (seconds)}}{3600.0}$$

3. **Total Bill**:
   $$\text{Bill (Rs.)} = \text{Base Price (Rs. 10.0/kWh)} \times \text{Energy (kWh)} \times \text{Demand Multiplier}$$

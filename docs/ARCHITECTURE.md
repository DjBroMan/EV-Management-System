# EV Charging Network Management System — Architecture Overview

## Overview
The EV Charging Network Management System is a distributed, service-oriented architecture implemented using **Java Remote Method Invocation (RMI)**. The system enables Electric Vehicle (EV) drivers to discover charging stations, reserve ports, initiate and stop charging sessions, calculate dynamic pricing, process payments, and automatically release charging ports upon successful payment completion.

---

## High-Level Component Architecture

```
                         EVClient (Console App)
                            |
              +-------------+-------------+
              |             |             |
              v             v             v
       Reservation   ChargingSession   Payment
          Server         Server         Server
              |             |             |
              v             v             v
       ChargingStation   Reservation   Pricing & ChargingStation
          Server          Server        Servers
```

```
                    MultithreadTest (Concurrency Test)
                           |
                    10 Parallel Threads
                           |
                           v
                     RMI Servers
```

---

## Key System Components

### 1. Primary Application Client (`EVClient.java`)
- Single interactive console application for human users.
- Connects dynamically to all 5 RMI servers on demand.
- Exercises the full lifecycle (station status, port lookup, slot reservation, session management, billing calculation, payment, and status checking).
- Never exports remote objects itself.

### 2. Concurrency Test Client (`MultithreadTest.java`)
- Automated multi-threaded load test simulating 10 concurrent EV client requests.
- Uses Java `ExecutorService` and `CountDownLatch` (start signal & completion signal) to unleash 10 concurrent threads simultaneously.
- Demonstrates safe shared-state management, double-booking prevention, and graceful handling when all 4 station ports are occupied.

### 3. ChargingStationServer (RMI Port 1234)
- **Service Name**: `ChargingStationServer`
- Manages physical charging ports (`P1`, `P2`, `P3`, `P4`) and their operational states (`AVAILABLE`, `RESERVED`, `CHARGING`).
- Provides atomic port check-and-reserve (`reserveAnyAvailablePort`) and state transition operations (`startPortCharging`, `releasePort`).

### 4. ReservationServer (RMI Port 1235)
- **Service Name**: `ReservationService`
- Handles slot booking requests from clients.
- Coordinates with `ChargingStationServer` to allocate available ports.
- Manages reservation records (`RES1001`, `RES1002`, etc.) and port mappings.

### 5. ChargingSessionServer (RMI Port 1236)
- **Service Name**: `ChargingSessionServer`
- Validates reservation status with `ReservationServer`.
- Signals `ChargingStationServer` to transition port state from `RESERVED` to `CHARGING`.
- Tracks session lifecycle (`CHARGING` → `COMPLETED`) and energy consumption (`25.0 kWh`).
- **Does NOT release the port on stop charging**; keeps session data available for payment processing.

### 6. PricingServer (RMI Port 1238)
- **Service Name**: `PricingService`
- Computes dynamic billing rates based on station demand multipliers (`LOW` = 1.0x, `MEDIUM` = 1.25x, `HIGH` = 1.50x) and energy consumed (`BASE_PRICE` = Rs. 10.0/kWh).

### 7. PaymentServer (RMI Port 1237)
- **Service Name**: `PaymentServer`
- Validates completed sessions with `ChargingSessionServer`.
- Retrieves energy consumed from `ChargingSessionServer` and price calculation from `PricingServer`.
- Generates payment receipts (`PAY-1001`) with status `SUCCESS`.
- **Triggers Post-Payment Port Release**: Invokes `ChargingStationServer.releasePort(portId)` only after payment succeeds, transitioning the port from `CHARGING` to `AVAILABLE`.

---

## Server Dependencies & Registry Ports

| Server | Registry Port | Bound Service Name | External Server Dependencies |
|--------|---------------|-------------------|------------------------------|
| `ChargingStationServer` | 1234 | `ChargingStationServer` | None |
| `ReservationServer` | 1235 | `ReservationService` | `ChargingStationServer` (Port 1234) |
| `ChargingSessionServer` | 1236 | `ChargingSessionServer` | `ChargingStationServer` (1234), `ReservationServer` (1235) |
| `PricingServer` | 1238 | `PricingService` | None |
| `PaymentServer` | 1237 | `PaymentServer` | `ChargingSessionServer` (1236), `PricingServer` (1238), `ChargingStationServer` (1234) |

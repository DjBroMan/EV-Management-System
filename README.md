# Distributed EV Charging Network Management System
### Java RMI, Docker, Lamport Logical Clocks, Cristian's Physical Clock Synchronization & Primary-Backup State Replication

A distributed, microservice-based **EV Charging Network Management System** built with **Java RMI**, **Docker Compose**, **Lamport Logical Clocks**, **Cristian's Physical Clock Synchronization Algorithm**, and **Primary-Backup In-Memory State Replication**.

---

## Quick Start

### 1. Build and Run with Docker Compose
```bash
# Build all Docker container images
docker compose build

# Start all microservices in background
docker compose up -d
```

### 2. Run Primary-Backup Replication Verification Test
```bash
# Compile Java source files
javac -d bin Clock/*.java ChargingStation/*.java Reservation/*.java ChargingSession/*.java Pricing/*.java Payment/*.java EVClient.java MultithreadTest.java ReplicationTest.java

# Run full replication test suite (8 test scenarios)
java -cp bin ReplicationTest
```

### 3. Run Multithreaded Test (10 EV Threads)
```bash
java -cp bin MultithreadTest
```

### 4. Run Interactive CLI Client
```bash
java -cp bin EVClient
```

---

## Microservice Architecture & Port Allocation

| Microservice Container | Bound RMI Service Name | Registry Port | Export Port | Role / Function |
|------------------------|------------------------|---------------|-------------|-----------------|
| `time-server` | `TimeServer` | `1239` | `2239` | Cristian Time Reference Server |
| `charging-station` | `ChargingStationServer` | `1234` | `2234` | Physical Charging Ports (`P1`-`P4`) |
| `reservation-primary` | `ReservationService`, `ReservationReplicationService` | `1235` | `2235` | Active Reservation Primary Server |
| `reservation-secondary` | `ReservationService`, `ReservationReplicationService` | `1245` | `2245` | Passive Backup Reservation Server |
| `reservation-manager` | `ReservationManager` | `1240` | `2240` | Replication & Failover Coordinator |
| `charging-session` | `ChargingSessionServer` | `1236` | `2236` | Physical Session Duration & Energy ($E = P \times T$) |
| `pricing` | `PricingService` | `1238` | `2238` | Dynamic Pricing Engine |
| `payment` | `PaymentServer` | `1237` | `2237` | Settlement & Post-Payment Port Release |

---

## Key Distributed Features

1. **Primary-Backup In-Memory Replication (`Reservation/` Package)**:
   - Synchronous state replication (`reservations`, `reservationPorts`, `reservationCounter`).
   - Single server class ([`ReservationServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Reservation/ReservationServer.java)) with dual roles (`PRIMARY` and `SECONDARY`).
   - Replication coordinator ([`ReservationServerManager.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Reservation/ReservationServerManager.java)) for heartbeat, full state sync, and automated failover promotion.
   - Result-state replication avoids double-booking ports on `ChargingStationServer`.

2. **Lamport Logical Clock (`Clock/LogicalClock.java`)**:
   - Maintains strict causal ordering across all RMI calls ($L_{\text{client}} \rightarrow L_{\text{primary}} \rightarrow L_{\text{manager}} \rightarrow L_{\text{secondary}}$).
   - Lock-free `AtomicLong` CAS loop implementation.

3. **Cristian Physical Clock Synchronization (`Clock/CristianClient.java`)**:
   - Synchronizes container startup clocks against `TimeServer` (port 1239/2239).
   - Physical timestamps used strictly for session duration and energy calculations.

# Distributed EV Charging Network Management System
### Java RMI, Docker, Lamport Logical Clocks & Cristian's Physical Clock Synchronization Algorithm

A distributed, microservice-based **EV Charging Network Management System** built with **Java RMI**, **Docker Compose**, **Lamport Logical Clocks**, and **Cristian's Physical Clock Synchronization Algorithm**.

---

## Quick Start

### 1. Build and Run with Docker Compose
```bash
# Build all Docker container images
docker compose build

# Start all microservices in background
docker compose up -d
```

### 2. Run Multithreaded Test (10 EV Threads)
```bash
# Compile Java source files
javac -d bin Clock/*.java ChargingStation/*.java Reservation/*.java ChargingSession/*.java Pricing/*.java Payment/*.java EVClient.java MultithreadTest.java

# Run complete multithreaded test suite
java -cp bin MultithreadTest
```

### 3. Run Interactive CLI Client
```bash
java -cp bin EVClient
```

---

## Microservice Architecture & Port Allocation

| Microservice Container | Bound RMI Service Name | Registry Port | Export Port | Simulated Container Time (`libfaketime`) |
|------------------------|------------------------|---------------|-------------|-----------------------------------------|
| `time-server` | `TimeServer` | `1239` | `2239` | Host System Time (Reference) |
| `charging-station` | `ChargingStationServer` | `1234` | `2234` | `2026-08-26 15:30:10` |
| `reservation` | `ReservationService` | `1235` | `2235` | `2026-08-26 15:30:05` |
| `charging-session` | `ChargingSessionServer` | `1236` | `2236` | `2026-08-26 15:29:55` |
| `pricing` | `PricingService` | `1238` | `2238` | `2026-08-26 15:30:03` |
| `payment` | `PaymentServer` | `1237` | `2237` | `2026-08-26 15:29:50` |

---

## Clock Synchronization Features

1. **Lamport Logical Clock (`Clock/LogicalClock.java`)**:
   - Maintains logical event order using lock-free `AtomicLong` CAS loop.
   - Transmits and receives timestamps across RMI calls using `LamportResult<T>` serializable wrapper.
   - Obeys Lamport receive rule: $L_{\text{receive}} = \max(L_{\text{local}}, L_{\text{received}}) + 1$.

2. **Cristian Physical Clock Synchronization (`Clock/CristianClient.java`)**:
   - Executes at server startup against `TimeServer` (port 1239/2239).
   - Measures round-trip time ($RTT = T_1 - T_0$) and calculates physical clock offset ($\text{offset} = T_{\text{server}} + \frac{RTT}{2} - T_1$).

---

## Sample Verified Terminal Output

```
[Physical=2026-08-26 16:09:00.198] [Lamport=64] [CLIENT THREAD-32] USER-10 Reservation ID: RES1004
[Physical=2026-08-26 16:09:04.613] [Lamport=94] [CLIENT THREAD-32] USER-10 Charging Start Response: Charging Started Successfully!
[Physical=2026-08-26 16:09:06.748] [Lamport=106] [CLIENT THREAD-32] USER-10 Charging Stop Response: Charging Stopped Successfully!
[Physical=2026-08-26 16:09:07.159] [Lamport=112] [CLIENT THREAD-32] USER-10 Energy Consumed: 25.0 kWh
[Physical=2026-08-26 16:09:08.897] [Lamport=132] [CLIENT THREAD-32] USER-10 Calculated Price: Rs. 250.0
[Physical=2026-08-26 16:09:12.752] [Lamport=198] [CLIENT THREAD-32] USER-10 Payment Response: Payment Successful! Port P4 released successfully. Now AVAILABLE.
```

# EV Charging Network Management System — Comprehensive Docker & Clock Synchronization Guide

This document provides a detailed, production-grade guide covering **Docker Containerization**, **Container Networking**, **Physical Clock Simulation (`libfaketime`)**, **Cristian's Physical Clock Synchronization Algorithm**, **Lamport Logical Clocks**, and the **Real-Time Charging Session Duration & Energy Calculation Workflow**.

---

# Table of Contents
1. [Part A — Dockerization & Microservices Architecture](#part-a--dockerization--microservices-architecture)
2. [Part B — Physical Clock Simulation (`libfaketime`)](#part-b--physical-clock-simulation-libfaketime)
3. [Part C — Cristian's Physical Clock Synchronization Algorithm](#part-c--cristians-physical-clock-synchronization-algorithm)
4. [Part D — Lamport Logical Clock & Causality](#part-d--lamport-logical-clock--causality)
5. [Part E — Physical Time vs Logical Time Comparison](#part-e--physical-time-vs-logical-time-comparison)
6. [Part F — Coexistence of Distributed Clock Mechanisms](#part-f--coexistence-of-distributed-clock-mechanisms)
7. [Part G — Complete EV Workflow & Clock Integration](#part-g--complete-ev-workflow--clock-integration)
8. [Part H — Terminal Logging Format Specification](#part-h--terminal-logging-format-specification)
9. [Part I — Complete System Architecture Diagram](#part-i--complete-system-architecture-diagram)
10. [Part J — Testing & Verification Procedures](#part-j--testing--verification-procedures)

---

# Part A — Dockerization & Microservices Architecture

## Why Docker is Used
In a Distributed Computing environment, software modules are deployed as independent nodes across separate network endpoints. Docker is used in this project to:
1. **Isolate Distributed Microservices**: Run each RMI server inside its own isolated Linux container with independent process spaces.
2. **Standardize Runtime & Dependencies**: Ensure all Java 17 execution environments and system libraries (`libfaketime`) are identical regardless of the host OS.
3. **Simulate Network Hostnames**: Enable RMI services to resolve and communicate with each other using Docker container hostnames (`time-server`, `charging-station`, `reservation`, `charging-session`, `pricing`, `payment`) via Docker DNS.
4. **Simulate Heterogeneous Clock Skew**: Preload `libfaketime` inside each container to give each microservice a distinct, simulated physical clock time without modifying the host machine's system clock.

---

## Docker Services Overview
The system configures **6 microservices** in [`docker-compose.yml`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/docker-compose.yml):

- **`time-server`**: Dedicated RMI reference physical time server (`Clock.TimeServer`).
- **`charging-station`**: Manages physical charging ports `P1`-`P4` (`ChargingStationServer`).
- **`reservation`**: Slot booking management service (`ReservationServer`).
- **`charging-session`**: Session tracking, physical start/end time recording, and real-time energy calculation ($E = P \times T$) (`ChargingSessionServer`).
- **`pricing`**: Dynamic bill calculation based on station demand multipliers (`PricingServer`).
- **`payment`**: Payment settlement and post-payment port release (`PaymentServer`).

---

## Docker Architecture Diagram

```
                                  ev-rmi-network (Bridge Network)
                                                 |
         +---------------------------------------+---------------------------------------+
         |                                       |                                       |
         v                                       v                                       v
    time-server                           charging-station                           pricing
(Port 1239/2239)                          (Port 1234/2234)                       (Port 1238/2238)
         ^                                       ^                                       ^
         | Cristian                              | RMI + Lamport                         | RMI + Lamport
         | Sync                                  |                                       |
         +-------------------+-------------------+-------------------+                   |
                             |                                       |                   |
                             v                                       v                   |
                        reservation                           charging-session           |
                      (Port 1235/2235)                        (Port 1236/2236)           |
                             |                                       |                   |
                             +-------------------+-------------------+                   |
                                                 |                                       |
                                                 v                                       |
                                              payment -----------------------------------+
                                          (Port 1237/2237)
```

---

## Dockerfile Explanation

The project uses a single multi-stage build [`Dockerfile`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Dockerfile) for compiling and executing all microservices:

```dockerfile
1: FROM eclipse-temurin:17-jdk
2: WORKDIR /app
3: RUN apt-get update && apt-get install -y libfaketime && rm -rf /var/lib/apt/lists/*
4: COPY . /app
5: RUN mkdir -p /app/bin && javac -d /app/bin Clock/*.java ChargingStation/*.java Reservation/*.java ChargingSession/*.java Pricing/*.java Payment/*.java EVClient.java MultithreadTest.java
6: ENV CLASSPATH=/app/bin
```

### Detailed Breakdown:
- **`FROM eclipse-temurin:17-jdk`**: Uses OpenJDK 17 base image containing JDK tools (`javac`, `java`, `rmiregistry`). Java 17 is required for modern concurrency utilities (`AtomicLong`, `Duration`, `Instant`) and RMI compatibility.
- **`WORKDIR /app`**: Sets `/app` as the working directory inside the container for all subsequent copy and build steps.
- **`RUN apt-get update && apt-get install -y libfaketime ...`**: Updates Debian package index and installs `libfaketime` library (`/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1`), cleaning apt cache to minimize image size.
- **`COPY . /app`**: Copies local source code, packages (`Clock/`, `ChargingStation/`, `Reservation/`, `ChargingSession/`, `Pricing/`, `Payment/`), and configuration files into `/app`.
- **`RUN mkdir -p /app/bin && javac -d /app/bin ...`**: Creates destination output directory `/app/bin` and compiles all Java source files cleanly in a single compilation pass.
- **`ENV CLASSPATH=/app/bin`**: Sets the Java execution classpath environment variable so classes can be executed via `java <ClassName>`.

---

## Docker Compose Explanation

[`docker-compose.yml`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/docker-compose.yml) orchestrates container creation, environment settings, port mappings, network bridge creation, and container startup dependencies (`depends_on`).

### RMI Port Scheme (Registry Ports vs Remote Object Export Ports)

Java RMI requires **two ports per service**:
1. **Registry Port**: Port on which `LocateRegistry.createRegistry(port)` runs to accept lookup requests.
2. **Remote Object Export Port**: Port on which `UnicastRemoteObject` exports the actual remote object skeleton (`super(port)`).

| Service Name | Container Name | Registry Port | Remote Object Port | Host Port Mapping |
|--------------|----------------|---------------|-------------------|-------------------|
| `time-server` | `time-server` | `1239` | `2239` | `"1239:1239"`, `"2239:2239"` |
| `charging-station` | `charging-station` | `1234` | `2234` | `"1234:1234"`, `"2234:2234"` |
| `reservation` | `reservation` | `1235` | `2235` | `"1235:1235"`, `"2235:2235"` |
| `charging-session` | `charging-session` | `1236` | `2236` | `"1236:1236"`, `"2236:2236"` |
| `payment` | `payment` | `1237` | `2237` | `"1237:1237"`, `"2237:2237"` |
| `pricing` | `pricing` | `1238` | `2238` | `"1238:1238"`, `"2238:2238"` |

---

## Container Networking & DNS Resolution

All 6 services join the custom bridge network `ev-rmi-network`:

```yaml
networks:
  ev-rmi-network:
    driver: bridge
```

Inside Docker, containers resolve other services using **Docker container names / service hostnames** instead of `localhost`:
- `ReservationServer` connects to `ChargingStationServer` at `STATION_HOST=charging-station`.
- `ChargingSessionServer` connects to `STATION_HOST=charging-station` and `RESERVATION_HOST=reservation`.
- `PaymentServer` connects to `SESSION_HOST=charging-session`, `PRICING_HOST=pricing`, and `STATION_HOST=charging-station`.
- All RMI servers perform Cristian synchronization against `TIME_SERVER_HOST=time-server`.

Client applications running on the host machine (`EVClient`, `MultithreadTest`) install a custom `RMISocketFactory` that maps container hostnames (`charging-station`, `reservation`, etc.) to `localhost` so host clients can communicate with Docker containers over mapped ports.

---

## Operational Docker Commands

```bash
# 1. Build container images
docker compose build

# 2. Start all microservices in detached mode
docker compose up -d

# 3. Check status of running containers
docker compose ps

# 4. View live aggregated logs
docker compose logs -f

# 5. View logs for specific service
docker compose logs reservation
docker compose logs charging-session

# 6. Verify container simulated dates (libfaketime)
docker compose exec charging-station date
docker compose exec reservation date
docker compose exec time-server date

# 7. Stop and remove containers and network
docker compose down
```

---

# Part B — Physical Clock Simulation (`libfaketime`)

## Role of `libfaketime`
In real distributed systems, hardware clocks across different physical machines suffer from **clock drift** and **clock skew**. To simulate this accurately in a local environment:
- `libfaketime` is installed in the container image (`/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1`).
- `LD_PRELOAD` intercepts C standard library time calls (`time()`, `gettimeofday()`, `clock_gettime()`) made by JVM system calls.
- `FAKETIME` defines the simulated physical starting time for each container.

## Configured Container Physical Clock Skews

| Container Name | `FAKETIME` Setting | Initial Simulated Physical Time |
|----------------|-------------------|---------------------------------|
| `time-server` | None (Host System Clock) | Reference Physical Time |
| `charging-station` | `FAKETIME="@2026-08-26 15:30:10"` | `2026-08-26 15:30:10` |
| `reservation` | `FAKETIME="@2026-08-26 15:30:05"` | `2026-08-26 15:30:05` |
| `charging-session` | `FAKETIME="@2026-08-26 15:29:55"` | `2026-08-26 15:29:55` |
| `pricing` | `FAKETIME="@2026-08-26 15:30:03"` | `2026-08-26 15:30:03` |
| `payment` | `FAKETIME="@2026-08-26 15:29:50"` | `2026-08-26 15:29:50` |

## Verifying Simulated Physical Clocks

Run `docker compose exec <service> date` to confirm container clock skews:
```bash
$ docker compose exec reservation date
Wed Aug 26 15:30:05 UTC 2026

$ docker compose exec charging-session date
Wed Aug 26 15:29:55 UTC 2026
```

---

# Part C — Cristian's Physical Clock Synchronization Algorithm

## Purpose
Cristian's algorithm enables a distributed client/server to synchronize its physical clock with a centralized reference time server (`TimeServer` running on port `1239`/`2239`) over a network by accounting for network round-trip delay.

## Step-by-Step Algorithm Execution

```
  Server (Client)                                   TimeServer (Port 1239/2239)
         |                                                     |
         | --- 1. T0 = Record local physical time ------------>|
         |        Send RMI request                             |
         |                                                     | --- 2. Record server physical time T_server
         |                                                     |
         |<-- 3. Return T_server via RMI response -------------|
         |
  4. T1 = Record local physical time
  5. Calculate RTT = T1 - T0
  6. Calculate Estimated Server Time = T_server + (RTT / 2)
  7. Calculate Clock Offset = Estimated Server Time - T1
  8. Apply Clock Offset to application-level PhysicalClock
```

### Formulas:
1. **Round-Trip Time ($RTT$)**:
   $$RTT = T_1 - T_0$$
2. **Estimated TimeServer Time**:
   $$\text{Estimated Server Time} = T_{\text{server}} + \frac{RTT}{2}$$
3. **Physical Clock Offset**:
   $$\text{Clock Offset} = \text{Estimated Server Time} - T_1 = T_{\text{server}} + \frac{RTT}{2} - T_1$$

The calculated offset is stored in memory by `Clock.PhysicalClock` and applied to subsequent application time formatting without modifying the host operating system clock.

---

## Numerical Example

- **Local time before request ($T_0$)**: `15:30:05.100`
- **TimeServer physical time response ($T_{\text{server}}$)**: `10:20:31.800`
- **Local time after response ($T_1$)**: `15:30:05.104`
- **Round-Trip Time ($RTT$)**: $15:30:05.104 - 15:30:05.100 = 4\text{ ms}$
- **One-Way Delay Estimate ($RTT / 2$)**: $4\text{ ms} / 2 = 2\text{ ms}$
- **Estimated TimeServer Time**: $10:20:31.800 + 2\text{ ms} = 10:20:31.802$
- **Calculated Offset**: $10:20:31.802 - 15:30:05.104 = -18,573,302\text{ ms}$

---

# Part D — Lamport Logical Clock & Causality

## Purpose
Lamport logical clocks provide a mechanism for establishing a **causal ordering of distributed events** across separate process spaces where physical clocks cannot be synchronized perfectly.

---

## Lamport State Update Rules

Each distributed component maintains its own `LogicalClock` instance (`AtomicLong` value $L$, initialized to $0$).

1. **Local Event Rule**:
   $$L = L + 1$$
   (`logicalClock.tick()`)

2. **Send Event Rule**:
   $$L = L + 1$$
   (`long sendL = logicalClock.sendEvent()`)
   Attach `sendL` timestamp parameter to outgoing RMI request.

3. **Receive Event Rule**:
   $$L = \max(L_{\text{local}}, L_{\text{received}}) + 1$$
   (`long recvL = logicalClock.receiveEvent(receivedTimestamp)`)

---

## Lamport Timestamp RMI Propagation

RMI remote methods accept `long clientLamport` and return `LamportResult<T>` serializable wrapper containers:

```java
// RMI Call Flow with LamportResult
ReservationServer (L=10)
  │ sendL = logicalClock.sendEvent() -> 11
  │ Logs [Event=SEND] Calling ChargingStationServer.reserveAnyAvailablePort(11)
  v
ChargingStationServer (L=4)
  │ recvL = logicalClock.receiveEvent(11) -> max(4, 11) + 1 = 12
  │ Logs [Event=RECEIVE] Port allocation request received
  │ localL = logicalClock.tick() -> 13
  │ Logs [Event=LOCAL] Port P1 allocated -> RESERVED
  │ respL = logicalClock.sendEvent() -> 14
  │ Logs [Event=SEND] Returning port reservation response P1
  v (returns LamportResult("P1", 14))
ReservationServer (L=11)
  │ logicalClock.receiveEvent(14) -> max(11, 14) + 1 = 15
  │ Logs [Event=RECEIVE] ChargingStationServer response received: P1
```

---

## Interaction with Multithreading

The application processes concurrent client requests using multithreaded worker pools (`ExecutorService`, RMI TCP connection threads).
- **Thread Safety**: [`Clock/LogicalClock.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Clock/LogicalClock.java) uses atomic lock-free compare-and-swap (`AtomicLong.compareAndSet`) loops inside `receiveEvent` to guarantee thread safety without deadlocks.
- **Thread ID vs Lamport Timestamp**:
  - `Thread ID` (`[Thread=27 | RMI TCP Connection(2)]`): Identifies the Java worker thread executing the code.
  - `Lamport Timestamp` (`[Lamport=14]`): Identifies logical event ordering in the distributed system.
  - Thread ID and Lamport timestamp are completely independent variables.

---

# Part E — Physical Time vs Logical Time Comparison

| Feature | Physical Clock (`java.time.Instant`) | Lamport Logical Clock (`Clock/LogicalClock`) |
|---------|-------------------------------------|----------------------------------------------|
| **Primary Purpose** | Real-world wall-clock time & session duration | Distributed causal event ordering |
| **Data Type** | `java.time.Instant` (nanoseconds) | `long` scalar counter |
| **Synchronization Method** | Cristian's algorithm against `TimeServer` | Lamport $\max(L_{\text{local}}, L_{\text{received}}) + 1$ |
| **Source of Clock Skew** | Docker container settings (`libfaketime`) | Independent execution rates |
| **Duration Calculation ($E = P \times T$)** | **YES** ($T_{\text{end}} - T_{\text{start}}$) | **NO** (Never calculate duration from Lamport time) |
| **Causal Order Guarantee ($L(A) < L(B)$)** | No (susceptible to clock drift) | **YES** (Guaranteed across RMI calls) |

---

# Part F — Coexistence of Distributed Clock Mechanisms

In this architecture, both clock mechanisms coexist harmoniously:
1. **Physical Clock (`Instant.now()`)**: Computes physical session duration and energy consumption ($E = P \times T$).
2. **Cristian Algorithm**: Synchronizes physical timestamps across microservices to minimize real-world time skew.
3. **Lamport Logical Clock**: Orders distributed messages and verifies causal dependencies across RMI calls.

---

# Part G — Complete EV Workflow & Clock Integration

```
[1. RESERVE] ---> [2. START CHARGING] ---> [3. STOP CHARGING] ---> [4. PRICING] ---> [5. PAYMENT] ---> [6. PORT RELEASE]
```

### Complete Step-by-Step Execution:

1. **Reservation (`RESERVE`)**:
   - `EVClient` sends `reserveSlot(userId, vehicleId, clientLamport)` ($L_{\text{client}} \rightarrow 1$).
   - `ReservationServer` receives call, updates $L = \max(L, 1) + 1 = 2$, calls `ChargingStationServer.reserveAnyAvailablePort(3)`.
   - `ChargingStationServer` allocates port `P1` $\rightarrow$ `RESERVED`, returns `LamportResult("P1", 6)`.
   - `ReservationServer` returns `LamportResult(details, 8)`. Client updates $L_{\text{client}} = 9$.

2. **Start Charging (`START CHARGING`)**:
   - Client sends `startCharging(reservationId, clientLamport)`.
   - `ChargingSessionServer` records physical start time $T_{\text{start}} = \text{Instant.now()}$ (`2026-08-26 15:30:51.695`), sets charging power $P = 7.2\text{ kW}$, creates `SESSION-1001`, and returns `LamportResult(details, sessL)`.

3. **Stop Charging & Real-Time Energy Calculation (`STOP CHARGING`)**:
   - Client sends `stopCharging(sessionId, clientLamport)`.
   - `ChargingSessionServer` records physical end time $T_{\text{end}} = \text{Instant.now()}$ (`2026-08-26 15:30:53.811`).
   - **Calculations**:
     $$\text{Duration (seconds)} = \text{Duration.between}(T_{\text{start}}, T_{\text{end}}).\text{toMillis}() / 1000.0 = 2.116\text{ seconds}$$
     $$\text{Duration (hours)} = \frac{2.116}{3600.0} = 0.0005877\text{ hours}$$
     $$\text{Energy Consumed (kWh)} = 7.2\text{ kW} \times 0.0005877\text{ hours} = \mathbf{0.004232\text{ kWh}}$$
   - Session status marked `COMPLETED`. Port `P1` remains locked in `CHARGING` state.

4. **Pricing Computation (`PRICING`)**:
   - `PaymentServer` retrieves energy `0.004232 kWh` from `ChargingSessionServer` and invokes `PricingServer.calculatePrice("S01", 0.004232, sendL)`.
   - `PricingServer` calculates bill:
     $$\text{Bill} = \text{Base Price (Rs. 10.0)} \times 0.004232 \times 1.0 = \mathbf{\text{Rs. } 0.04232}$$

5. **Payment Settlement & Post-Payment Port Release (`PAYMENT` $\rightarrow$ `PORT RELEASE`)**:
   - `PaymentServer` creates receipt `PAY-1001` with status `SUCCESS`.
   - `PaymentServer` calls `ChargingStationServer.releasePort("P1", sendL)`.
   - `ChargingStationServer` releases port `P1` $\rightarrow$ `AVAILABLE`.

---

# Part H — Terminal Logging Format Specification

All server logs use strictly **ASCII formatting** (using `->` instead of Unicode arrows) to prevent Windows PowerShell encoding artifacts (`â??`):

```
[Physical=2026-08-26 15:31:25.214]
[Lamport=88]
[Server=ReservationServer]
[Thread=44 | RMI TCP Connection(18)-172.18.0.1]
[Event=RECEIVE]
ChargingStationServer returned port P1 (Station Lamport: 52)
```

### Log Field Definitions:
- **`[Physical=...]`**: Physical time formatted as `yyyy-MM-dd HH:mm:ss.SSS` (reflecting Cristian offset + `libfaketime`).
- **`[Lamport=...]`**: Current Lamport logical clock counter.
- **`[Server=...]`**: Microservice server name.
- **`[Thread=...]`**: Java Thread ID and Thread Name.
- **`[Event=...]`**: Event type (`RECEIVE`, `SEND`, `LOCAL`).

---

# Part I — Complete System Architecture Diagram

```
                                      Docker Network (ev-rmi-network)
                                                     |
         +-------------------------------------------+-------------------------------------------+
         |                                           |                                           |
         v                                           v                                           v
   time-server                                charging-station                                pricing
 (Port 1239/2239)                             (Port 1234/2234)                            (Port 1238/2238)
         ^                                           ^                                           ^
         | Cristian Sync                             | RMI + Lamport                             | RMI + Lamport
         |                                           |                                           |
         +--------------------+----------------------+--------------------+                      |
                              |                                           |                      |
                              v                                           v                      |
                         reservation                               charging-session              |
                       (Port 1235/2235)                            (Port 1236/2236)              |
                              |                                           |                      |
                              +---------------------+---------------------+                      |
                                                    |                                            |
                                                    v                                            |
                                                 payment ----------------------------------------+
                                             (Port 1237/2237)

Each Server Component Contains:
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ - LogicalClock (Lamport AtomicLong CAS loop)                                          │
│ - PhysicalClock (libfaketime system time + Cristian offset)                           │
│ - RMI Registry & Remote Object Export Skeleton                                        │
│ - DistributedLogger (ASCII formatted terminal event logging)                          │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# Part J — Testing & Verification Procedures

## 1. Verify Docker Deployment
```bash
docker compose build
docker compose up -d
docker compose ps
```
**Expected Outcome**: All 6 containers (`time-server`, `charging-station`, `reservation`, `charging-session`, `pricing`, `payment`) are `Up`.

## 2. Verify Container Clock Skews (`libfaketime`)
```bash
docker compose exec charging-station date
docker compose exec reservation date
docker compose exec charging-session date
```
**Expected Outcome**: Containers display their distinct simulated `FAKETIME` dates (`15:30:10`, `15:30:05`, `15:29:55`).

## 3. Execute Multithreaded RMI Concurrency & Clock Test
```bash
javac -d bin Clock/*.java ChargingStation/*.java Reservation/*.java ChargingSession/*.java Pricing/*.java Payment/*.java EVClient.java MultithreadTest.java
java -cp bin MultithreadTest
```
**Expected Outcome**:
- 10 EV client threads execute concurrently in parallel.
- Lamport timestamps advance non-zero monotonically ($64 \rightarrow 94 \rightarrow 106 \rightarrow 112 \rightarrow 132 \rightarrow 198$).
- Session duration and energy consumption calculated ($E = P \times T$).
- Post-payment port release confirmed.

## 4. Run Interactive CLI Client
```bash
java -cp bin EVClient
```

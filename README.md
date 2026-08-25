# Java RMI EV Charging Network Management System (Dockerized)

A distributed Java RMI (Remote Method Invocation) system that manages an EV charging network across 5 containerized microservices orchestrated with Docker Compose.

---

## 🚀 Architecture Overview

The system consists of **5 independent RMI Servers** running inside dedicated Docker containers and **2 Client Applications** running on the host machine.

```
+-----------------------------------------------------------------------------------+
|                                  WINDOWS HOST                                     |
|                                                                                   |
|   +-----------------------+                    +------------------------------+   |
|   |     EVClient.java     |                    |    MultithreadTest.java      |   |
|   +-----------+-----------+                    +--------------+---------------+   |
|               |                                               |                   |
|               +-----------------------+-----------------------+                   |
|                                       | (Host RMI Lookups)                        |
+---------------------------------------|-------------------------------------------+
                                        v
+-----------------------------------------------------------------------------------+
|                        DOCKER NETWORK (ev-rmi-network)                            |
|                                                                                   |
|  +------------------------+  (Port 1234/2234)  +-----------------------------+  |
|  |    charging-station    | <------------------ |         reservation         |  |
|  +-----------+------------+                     +--------------+--------------+  |
|              ^                                                 ^                 |
|              |                                                 |                 |
|              +-------------------------+                       |                 |
|              |                         |                       |                 |
|              v                         v                       v                 |
|  +-----------+------------+  (Port 1238/2238)  +--------------+--------------+  |
|  |        pricing         | <------------------ |      charging-session       |  |
|  +-----------+------------+                     +--------------+--------------+  |
|              ^                                                 ^                 |
|              |                                                 |                 |
|              +-------------------+   +-------------------------+                 |
|                                  |   |                                           |
|                                  v   v                                           |
|                             +----+---+----------------+                          |
|                             |         payment         |                          |
|                             +-------------------------+                          |
+-----------------------------------------------------------------------------------+
```

---

## ⚡ Services & Port Allocations

Each RMI service exposes both an **RMI Registry Port** and an explicit **Remote Object Port** to ensure reliable cross-container communication.

| Service Name | RMI Service Name | Registry Port | Remote Object Port | Upstream Dependencies |
|--------------|------------------|---------------|-------------------|-|
| `charging-station` | `ChargingStationServer` | `1234` | `2234` | None |
| `pricing` | `PricingService` | `1238` | `2238` | None |
| `reservation` | `ReservationService` | `1235` | `2235` | `charging-station` |
| `charging-session` | `ChargingSessionServer` | `1236` | `2236` | `charging-station`, `reservation` |
| `payment` | `PaymentServer` | `1237` | `2237` | `charging-station`, `charging-session`, `pricing` |

---

## 📋 Prerequisites

- **Java JDK**: 17 or higher
- **Docker**: 20.10+
- **Docker Compose**: v2+

---

## 🛠️ Quick Start Guide

### 1. Compile the Java Source Code
Compile all Java modules into the `bin/` directory:

```powershell
javac -d bin ChargingStation/*.java Reservation/*.java ChargingSession/*.java Pricing/*.java Payment/*.java EVClient.java MultithreadTest.java
```

### 2. Build & Start Docker Containers
Launch all 5 RMI microservices in detached mode:

```powershell
docker compose up -d --build
```

Verify that all 5 containers are running:
```powershell
docker compose ps
```

---

## 🖥️ Running the Clients

### Interactive Console App (`EVClient`)
Run the unified EV client menu to step through the charging lifecycle interactively:

```powershell
java -cp bin EVClient
```

#### Workflow Example:
1. Enter User ID (e.g. `USER-101`) and Vehicle ID (e.g. `EV-202`).
2. Select **`1`** to check Station Status.
3. Select **`4`** to Reserve a Charging Slot (assigns port e.g. `P1`).
4. Select **`7`** to Start Charging (creates Session ID).
5. Select **`9`** to Stop Charging (calculates energy e.g. `25.0 kWh`).
6. Select **`10`** to Calculate Bill (queries `PricingServer`).
7. Select **`11`** to Make Payment (processes payment & automatically releases port `P1`).

### Concurrent Multithreaded Test (`MultithreadTest`)
Simulate 10 EV threads concurrently making RMI calls to test port synchronization and server concurrency:

```powershell
java -cp bin MultithreadTest
```

---

## 🔍 Log Commands

Inspect logs for all container services:
```powershell
docker compose logs -f
```

Inspect logs for a specific service:
```powershell
docker compose logs charging-station
docker compose logs reservation
docker compose logs charging-session
docker compose logs pricing
docker compose logs payment
```

Inspect the Dockerization change log:
- [`log/docker_implementation.log`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/log/docker_implementation.log)

---

## 🛑 Stopping the System

Stop and remove all running containers and networks:

```powershell
docker compose down
```

---

## 📚 Documentation

For detailed technical specs, network topology, and troubleshooting guides, see:
- [`docs/DOCKER.md`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/docs/DOCKER.md)

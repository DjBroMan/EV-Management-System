# Docker Deployment Guide — EV Charging Network Management System

This document describes the Docker architecture, container networking, port allocations, startup dependencies, and operational commands for the Java RMI EV Charging Network Management System.

---

## Architecture Overview

The application is deployed across 5 isolated containers connected via a dedicated bridge network (`ev-rmi-network`).

```
+-----------------------------------------------------------------------------------+
|                                  WINDOWS HOST                                     |
|                                                                                   |
|   +-----------------------+                    +------------------------------+   |
|   |     EVClient.java     |                    |    MultithreadTest.java      |   |
|   +-----------+-----------+                    +--------------+---------------+   |
|               | (Host RMI Lookups to localhost:1234-1238) |                       |
+---------------+-----------------------------------------------+-------------------+
                |                                               |
                v                                               v
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

## Docker Services & Port Mapping Table

| Service Name | Container Name | Registry Port (Container/Host) | Remote Object Port (Container/Host) | Environment / Service Host Setup |
|--------------|----------------|-------------------------------|-------------------------------------|-----------------------------------|
| `charging-station` | `charging-station` | `1234` : `1234` | `2234` : `2234` | `RMI_SERVER_HOST=charging-station` |
| `reservation` | `reservation` | `1235` : `1235` | `2235` : `2235` | `STATION_HOST=charging-station` |
| `charging-session` | `charging-session` | `1236` : `1236` | `2236` : `2236` | `STATION_HOST=charging-station`, `RESERVATION_HOST=reservation` |
| `payment` | `payment` | `1237` : `1237` | `2237` : `2237` | `STATION_HOST=charging-station`, `SESSION_HOST=charging-session`, `PRICING_HOST=pricing` |
| `pricing` | `pricing` | `1238` : `1238` | `2238` : `2238` | `RMI_SERVER_HOST=pricing` |

---

## Startup Dependencies & Retry Mechanism

1. **ChargingStationServer**: Independent (starts first).
2. **PricingServer**: Independent (starts first).
3. **ReservationServer**: Connects to `ChargingStationServer` (`charging-station:1234`).
4. **ChargingSessionServer**: Connects to `ChargingStationServer` (`charging-station:1234`) and `ReservationServer` (`reservation:1235`).
5. **PaymentServer**: Connects to `ChargingStationServer` (`charging-station:1234`), `ChargingSessionServer` (`charging-session:1236`), and `PricingServer` (`pricing:1238`).

### Retry Mechanism
Every dependent server uses an automatic retry loop (10 retries with 2-second sleep intervals):
```
Connecting to ChargingStationServer at rmi://charging-station:1234//ChargingStationServer...
Waiting for ChargingStationServer...
Retry 1/10...
ChargingStationServer connected.
```

---

## Host vs. Container Communication

### Container-to-Container Communication
- Uses Docker service names over `ev-rmi-network` (e.g. `rmi://charging-station:1234//ChargingStationServer`).
- Exported Remote Objects communicate over explicit Remote Object ports (`2234`-`2238`).

### Host-to-Container Communication
- Host clients (`EVClient` and `MultithreadTest`) connect to containers via published ports on `localhost` (e.g. `rmi://localhost:1234//ChargingStationServer`).
- If container RMI stubs export service names (e.g. `charging-station`), the Windows host can resolve them by mapping service names to `127.0.0.1` in `C:\Windows\System32\drivers\etc\hosts`:
  ```
  127.0.0.1 charging-station reservation charging-session pricing payment
  ```

---

## Operational Commands

### 1. Build Docker Images
```powershell
docker compose build
```

### 2. Start Containers (Detached Mode)
```powershell
docker compose up -d
```

### 3. Check Running Containers
```powershell
docker compose ps
```

### 4. View Container Logs
```powershell
docker compose logs -f
# Or view logs for a specific service:
docker compose logs charging-station
docker compose logs reservation
docker compose logs charging-session
docker compose logs pricing
docker compose logs payment
```

### 5. Run Host Clients
```powershell
# Compile Java source code
javac -d bin ChargingStation/*.java Reservation/*.java ChargingSession/*.java Pricing/*.java Payment/*.java EVClient.java MultithreadTest.java

# Run interactive client
java -cp bin EVClient

# Run multithreaded test client
java -cp bin MultithreadTest
```

### 6. Stop Containers
```powershell
docker compose down
```

---

## Troubleshooting

- **Connection Refused during startup**: Dependent containers automatically retry until upstream RMI registries are active. Check container logs (`docker compose logs <service>`) to verify connection status.
- **UnknownHostException on Host**: Ensure host port publishing is active (`docker compose ps`) and verify service name resolution in `C:\Windows\System32\drivers\etc\hosts`.
- **RMI Port Conflict**: Ensure no local Java RMI registries are already running on ports 1234-1238 or 2234-2238 before running `docker compose up`.

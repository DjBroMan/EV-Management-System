# Docker & Networking Deployment Guide

This document describes the Dockerized architecture, `libfaketime` physical clock simulation, port mappings, and container verification commands.

---

## 🐳 Docker Services Architecture

The system runs 6 microservices on the `ev-rmi-network` bridge driver:

1. **`time-server`**: Reference physical clock server.
2. **`charging-station`**: Manages charging ports P1-P4.
3. **`pricing`**: Dynamic pricing calculation.
4. **`reservation`**: Slot booking management.
5. **`charging-session`**: Session tracking & energy metering.
6. **`payment`**: Payment processing & post-payment port release.

---

## ⏱️ `libfaketime` Physical Clock Simulation

Physical clock skews across Docker containers are simulated using `libfaketime`:
- **Dockerfile**: Installs `libfaketime` package via `apt-get install -y libfaketime`.
- **Preload**: `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1`.
- **Environment Variable**: `FAKETIME` configures fixed/simulated initial physical timestamps per container:

```yaml
  charging-station:
    environment:
      - LD_PRELOAD=/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1
      - FAKETIME=@2026-08-26 15:30:10

  reservation:
    environment:
      - LD_PRELOAD=/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1
      - FAKETIME=@2026-08-26 15:30:05
```

---

## 🔌 RMI Port Mapping Reference

| Service Name | Container Name | Registry Port | Remote Object Port | Host Port Mapping |
|--------------|----------------|---------------|-------------------|-------------------|
| `time-server` | `time-server` | 1239 | 2239 | `1239:1239`, `2239:2239` |
| `charging-station` | `charging-station` | 1234 | 2234 | `1234:1234`, `2234:2234` |
| `reservation` | `reservation` | 1235 | 2235 | `1235:1235`, `2235:2235` |
| `charging-session` | `charging-session` | 1236 | 2236 | `1236:1236`, `2236:2236` |
| `payment` | `payment` | 1237 | 2237 | `1237:1237`, `2237:2237` |
| `pricing` | `pricing` | 1238 | 2238 | `1238:1238`, `2238:2238` |

---

## 🛠️ Operational Commands

### Build Containers
```powershell
docker compose build
```

### Start Services in Detached Mode
```powershell
docker compose up -d
```

### Verify Running Container Services
```powershell
docker compose ps
```

### View Live Logs
```powershell
docker compose logs -f
docker compose logs time-server
docker compose logs reservation
```

### Verify Container Simulated Dates (`libfaketime`)
```powershell
docker compose exec reservation date
docker compose exec charging-station date
docker compose exec time-server date
```

### Shutdown Services
```powershell
docker compose down
```

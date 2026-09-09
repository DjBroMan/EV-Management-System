# EV Charging Network Management System — RMI Communication Architecture

## Overview
Communication between client applications and microservice servers—as well as inter-server microservice communication and primary-backup state replication—is performed exclusively using **Java Remote Method Invocation (Java RMI)**.

Each microservice exports its remote objects on specific RMI registry and remote ports, and methods accept a `long clientLamport` parameter and return a `LamportResult<T>` serializable wrapper object containing both the operational result and the updated server Lamport timestamp.

---

## Service Port Allocation & Registry Scheme

| Microservice | RMI Service Name | Registry Port | Remote Object Export Port | Interfaces Implemented |
|--------------|------------------|---------------|---------------------------|------------------------|
| `TimeServer` | `TimeServer` | `1239` | `2239` | `TimeServerInterface` |
| `ChargingStationServer` | `ChargingStationServer` | `1234` | `2234` | `ChargingStationInterface` |
| `ReservationServer (PRIMARY)` | `ReservationService`, `ReservationReplicationService` | `1235` | `2235` | `ReservationInterface`, `ReservationReplicationInterface` |
| `ReservationServer (SECONDARY)` | `ReservationService`, `ReservationReplicationService` | `1245` | `2245` | `ReservationInterface`, `ReservationReplicationInterface` |
| `ReservationServerManager` | `ReservationManager` | `1240` | `2240` | `ReservationManagerInterface` |
| `ChargingSessionServer` | `ChargingSessionServer` | `1236` | `2236` | `ChargingSessionInterface` |
| `PaymentServer` | `PaymentServer` | `1237` | `2237` | `PaymentInterface` |
| `PricingServer` | `PricingService` | `1238` | `2238` | `PricingInterface` |

---

## Inter-Service RMI Dependencies & Replication

```
+------------------+
|     EVClient /   |
| MultithreadTest  |
+--------+---------+
         |
         | RMI request + clientLamport (LamportResult response returned)
         v
+------------------+     RMI + sendL     +-----------------------+
|ReservationPrimary+-------------------->+ ChargingStationServer |
+--------+---------+                     +-----------^-----------+
         |                                           |
         | RMI + sendL (replicateReservation)        | RMI + sendL (post-payment release)
         v                                           |
+------------------+                                 |
|ReservationManager|                                 |
+--------+---------+                                 |
         |                                           |
         | RMI + sendL (applyReservationUpdate)      |
         v                                           |
+------------------+                                 |
|ReservationSecond |                                 |
+------------------+                                 |
         |                                           |
         |                                           |
+--------v---------+     RMI + sendL                 |
|ChargingSessionSrv+---------------------------------+
+--------+---------+
         |
         | RMI + sendL
         v
+------------------+     RMI + sendL     +-----------------------+
|  PaymentServer   +-------------------->+     PricingServer     |
+------------------+                     +-----------------------+
```

---

## Lamport Timestamp Rules in RMI Calls

1. **Client / Sender SEND Event**:
   `long sendL = clock.sendEvent();` ($L = L + 1$)
   Sender passes `sendL` as parameter to remote RMI method.

2. **Server RECEIVE Event**:
   `long recvL = clock.receiveEvent(clientLamport);` ($L = \max(L_{\text{local}}, \text{clientLamport}) + 1$)
   Server logs `[Event=RECEIVE]` tag.

3. **Server LOCAL Event**:
   `clock.tick();` ($L = L + 1$)
   Server updates internal state, logs `[Event=LOCAL]` tag.

4. **Server Inter-Service RMI Call (Primary -> Manager -> Secondary)**:
   `long interSendL = clock.sendEvent();`
   Server logs `[Event=SEND]` tag and invokes downstream RMI server.

5. **Server Return Response**:
   `long respL = clock.sendEvent();`
   Server logs `[Event=SEND]` and returns `new LamportResult<>(data, respL)`.

6. **Client / Sender RECEIVE Response Event**:
   `clock.receiveEvent(response.getTimestamp());`
   Caller updates local clock to $\max(L_{\text{caller}}, \text{response.getTimestamp()}) + 1$.

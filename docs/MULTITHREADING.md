# Multithreading & Concurrency Model

This document describes the concurrency model, thread-safety mechanisms, and thread logging format of the system including primary-backup state replication.

---

## 🧵 Thread Model Overview

```
MultithreadTest (10 EV Client Threads)
       |
       |  (Concurrent RMI Invocations)
       v
RMI Server Thread Pool (RMI TCP Connections)
       |
       |-- ChargingStationServer (Synchronized Port Map)
       |-- ReservationServer PRIMARY (Synchronized State Writes & Synchronous Replication Dispatch)
       |    +-- ReservationServerManager (Dispatches replication to Secondary)
       |    +-- ReservationServer SECONDARY (Synchronized Replicated Map Updates)
       |-- ChargingSessionServer (Concurrent requests + Synchronized Session Writes)
       |-- PricingServer (Stateless calculation)
       +-- PaymentServer (Concurrent requests + Synchronized Payment Writes)
```

---

## 🔒 Thread Safety & Atomic Clock Updates

1. **`LogicalClock` Thread Safety**:
   - Implemented using `java.util.concurrent.atomic.AtomicLong`.
   - `receiveEvent(long timestamp)` uses a lock-free compare-and-set atomic update loop:
     ```java
     while (true) {
         long current = clock.get();
         long updated = Math.max(current, receivedTimestamp) + 1;
         if (clock.compareAndSet(current, updated)) return updated;
     }
     ```
   - Prevents race conditions when multiple RMI TCP connection threads update the server's Lamport clock simultaneously.

2. **`PhysicalClock` Thread Safety**:
   - Offset stored as `private static volatile long clockOffsetMs`.
   - Read/write access to offset is volatile-guaranteed across worker threads.

3. **Replication State Synchronization**:
   - `ReservationServer` state maps (`reservations`, `reservationPorts`) and `reservationCounter` are modified within synchronized mutex blocks.
   - Prevents race conditions when multiple concurrent client threads generate reservation IDs or update replication state.

4. **Charging Port Synchronization**:
   - `ChargingStationServer.reserveAnyAvailablePort()` remains `synchronized` to prevent double-booking.

---

## 📊 Terminal Thread Logging Format

Server logs explicitly format thread information alongside physical and Lamport timestamps:

```
[Physical=2026-08-26 15:30:05.123]
[Lamport=17]
[Server=ReservationServer[PRIMARY:1235]]
[Thread=23 | RMI TCP Connection(5)]
[Event=RECEIVE]
RESERVE SLOT request received from User USER-1
```

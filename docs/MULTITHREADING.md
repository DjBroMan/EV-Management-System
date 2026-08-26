# Multithreading & Concurrency Model

This document describes the concurrency model, thread-safety mechanisms, and thread logging format of the system.

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
       |-- ReservationServer (Concurrent requests + Synchronized Map Writes)
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

3. **Charging Port Synchronization**:
   - `ChargingStationServer.reserveAnyAvailablePort()` remains `synchronized` to prevent double-booking.
   - `ReservationServer.reserveSlot()` allows concurrent thread entry while delegating port state protection to `ChargingStationServer`.

---

## 📊 Terminal Thread Logging Format

Server logs explicitly format thread information alongside physical and Lamport timestamps:

```
[Physical=2026-08-26 15:30:05.123]
[Lamport=17]
[Server=ReservationServer]
[Thread=23 | RMI TCP Connection(5)]
Reservation request received from USER-1
```

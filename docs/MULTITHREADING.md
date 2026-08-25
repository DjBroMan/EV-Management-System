# EV Charging Network Management System — Multithreading & Synchronization Guide

## Java RMI Concurrent Request Processing Model

In Java RMI, remote objects managed by `UnicastRemoteObject` execute client requests using a thread pool managed by the RMI runtime framework.

- Each incoming remote method call from an RMI client is assigned to an **RMI Server Thread** from the RMI runtime pool.
- Multiple clients (or multiple threads from `MultithreadTest`) calling the same RMI server execute **concurrently** on separate RMI server threads.

---

## Double-Booking Prevention & Shared State Synchronization

The primary shared mutable state in this distributed system is the **charging port availability array** (`portStatus[]`) managed by `ChargingStationServer`.

### 1. `ChargingStationServer` Synchronization
- Methods such as `reserveAnyAvailablePort()`, `startPortCharging()`, `releasePort()`, `reservePort()`, and `checkPortAvailability()` are declared `synchronized`.
- When multiple RMI threads call `ChargingStationServer.reserveAnyAvailablePort()` simultaneously via `ReservationServer`:
  1. Thread A acquires the monitor lock of `ChargingStationServer`.
  2. Thread A scans ports (`P1`–`P4`), finds `P1` `AVAILABLE`, changes `P1` to `RESERVED`, and releases the lock.
  3. Thread B then acquires the lock, scans ports, sees `P1` is `RESERVED`, and reserves `P2`.
  4. **Guaranteed Outcome**: Two concurrent EVs will **NEVER** be allocated the same charging port.

### 2. Fine-Grained Synchronization in Middle-Tier Servers
To avoid performance bottlenecks during long simulated delays (`simulateProcessing(400-700ms)`), coarse method-level `synchronized` declarations were removed from `ReservationServer`, `ChargingSessionServer`, `PricingServer`, and `PaymentServer`.

Instead, **fine-grained `synchronized(this)` blocks** are used exclusively around shared data operations:
- Sequential counter increments (`reservationCounter++`, `sessionCounter++`, `paymentCounter++`).
- Thread-unsafe `HashMap` read/write operations (`reservations`, `reservationPorts`, `sessionStatus`, `energyConsumed`, `sessionPort`, `paymentStatus`, `paymentDetails`).

This design allows RMI threads to perform sleep delays and inter-server RMI network round-trips **concurrently** without locking out other remote callers.

---

## MultithreadTest Architecture & Execution

`MultithreadTest.java` is a dedicated stress client designed to simulate high concurrency:

```
                          MultithreadTest (Main)
                                     │
                 ┌───────────────────┴───────────────────┐
                 │  ExecutorService (Fixed 10 Threads)   │
                 └───────────────────┬───────────────────┘
                                     │
                     CountDownLatch (startSignal.await())
                                     │
                     [Release all 10 EV Threads simultaneously]
                                     │
          ┌──────────┬──────────┬────┴─────┬──────────┬──────────┐
          ▼          ▼          ▼          ▼          ▼          ▼
        EV-1       EV-2       EV-3       EV-4       EV-5 ...   EV-10
        Thread     Thread     Thread     Thread     Thread     Thread
          │          │          │          │          │          │
          └──────────┴──────────┼──────────┴──────────┴──────────┘
                                ▼
                       RMI Servers (1234–1238)
```

### Execution Mechanics
1. **Thread Pool Creation**: `Executors.newFixedThreadPool(10)` creates 10 worker client threads.
2. **Dynamic RMI Lookups**: Each EV thread independently performs `Naming.lookup(...)` calls for all 5 remote services.
3. **Start Signal Barrier (`CountDownLatch startSignal = new CountDownLatch(1)`)**:
   - All 10 EV threads call `startSignal.await()`.
   - The main thread sleeps 2000ms to allow all threads to prepare, then calls `startSignal.countDown()`.
   - All 10 threads start sending remote RMI requests simultaneously.
4. **Graceful Capacity Handling**:
   - The station has 4 ports (`P1`–`P4`).
   - Threads 1–4 successfully reserve ports `P1`–`P4`.
   - Threads 5–10 receive `"NONE"` / `"No charging ports available"` from `reserveSlot()`, log port unavailability, and terminate their workflow gracefully without throwing unhandled exceptions.
5. **Completion Barrier (`CountDownLatch completionSignal = new CountDownLatch(10)`)**:
   - The main thread waits for `completionSignal.await()` before reporting total execution time and test results.

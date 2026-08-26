# EV Charging Network Management System — Module Reference

## Core Modules

### 1. `Clock/` Package
- [`LogicalClock.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Clock/LogicalClock.java): Lock-free thread-safe Lamport logical clock (`AtomicLong`) supporting `tick()`, `sendEvent()`, and `receiveEvent(receivedTimestamp)`.
- [`PhysicalClock.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Clock/PhysicalClock.java): Utilities for formatting local container system time (hooked by `libfaketime`) and application-level Cristian synchronized time ($T_{\text{local}} + \text{offset}$).
- [`TimeServerInterface.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Clock/TimeServerInterface.java) & [`TimeServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Clock/TimeServer.java): RMI physical time reference server running on port `1239`/`2239`.
- [`CristianClient.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Clock/CristianClient.java): Cristian clock synchronization algorithm implementation measuring $RTT$ and clock offset.
- [`LamportResult.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Clock/LamportResult.java): Generic serializable RMI wrapper object carrying return data and server Lamport timestamp.
- [`DistributedLogger.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Clock/DistributedLogger.java): ASCII logger formatting `Physical`, `Lamport`, `Server`, `Thread`, and `Event` tags.

### 2. `ChargingStation/` Package
- `ChargingStationInterface.java` & `ChargingStationServer.java` (Registry Port `1234`, Export Port `2234`): Manages physical ports (`P1`-`P4`).

### 3. `Reservation/` Package
- `ReservationInterface.java` & `ReservationServer.java` (Registry Port `1235`, Export Port `2235`): Slot reservation management.

### 4. `ChargingSession/` Package
- `ChargingSessionInterface.java` & `ChargingSessionServer.java` (Registry Port `1236`, Export Port `2236`): Session tracking.

### 5. `Pricing/` Package
- `PricingInterface.java` & `PricingServer.java` (Registry Port `1238`, Export Port `2238`): Dynamic bill calculation.

### 6. `Payment/` Package
- `PaymentInterface.java` & `PaymentServer.java` (Registry Port `1237`, Export Port `2237`): Settlement and post-payment port release.

### 7. Client Applications
- `EVClient.java`: Interactive CLI client.
- `MultithreadTest.java`: Concurrent multithreaded test suite (10 EV threads).

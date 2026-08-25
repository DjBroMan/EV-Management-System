# EV Charging Network System Audit

## 1. Overall Result

**WORKFLOW REQUIRES FIXES**

---

## 2. Actual Current Architecture

The codebase consists of:
- **Primary Application Client**: `EVClient.java` (unified console UI driving the system via remote RMI calls).
- **Concurrency Test Client**: `MultithreadTest.java` (spawns 10 EV client threads hitting RMI servers concurrently).
- **5 RMI Servers**:
  1. `ChargingStationServer` (Port 1234, Service: `ChargingStationServer`)
  2. `ReservationServer` (Port 1235, Service: `ReservationService`)
  3. `ChargingSessionServer` (Port 1236, Service: `ChargingSessionServer`)
  4. `PaymentServer` (Port 1237, Service: `PaymentServer`)
  5. `PricingServer` (Port 1238, Service: `PricingService`)

---

## 3. Workflow Comparison

| Step | Intended Behavior | Current Behavior | Correct? |
|------|-------------------|------------------|----------|
| **1. Reservation Request** | `EVClient` requests reservation from `ReservationServer`. | `EVClient` calls `ReservationServer.reserveSlot()`. | **Yes** |
| **2. Port Allocation** | `ReservationServer` contacts `ChargingStationServer` to find & reserve port atomically (`AVAILABLE` → `RESERVED`). | `ReservationServer` calls `ChargingStationServer.reserveAnyAvailablePort()`. | **Yes** |
| **3. Reservation Record** | `ReservationServer` creates reservation record (`RES1001`, User ID, Vehicle ID, Port, `CONFIRMED`). | Reservation created and returned to client. | **Yes** |
| **4. Start Charging** | `ChargingSessionServer` validates reservation with `ReservationServer`, gets port, calls `ChargingStationServer.startPortCharging()` (`RESERVED` → `CHARGING`), and creates session (`SESSION-1001`). | `ChargingSessionServer` validates reservation, gets port, calls `startPortCharging()`, and creates session. | **Yes** |
| **5. Stop Charging** | `ChargingSessionServer` marks session `COMPLETED`, records energy consumed, and retains session for billing. | `ChargingSessionServer.stopCharging()` marks session `COMPLETED`, records energy, **AND immediately calls `ChargingStationServer.releasePort(portId)` (`CHARGING` → `AVAILABLE`)**. | **No (Premature Port Release)** |
| **6. Energy Tracking** | `ChargingSessionServer` tracks energy consumed per session. | `ChargingSessionServer` records `25.0 kWh` on session completion. | **Yes** |
| **7. Pricing** | `PricingServer` calculates bill using `BASE_PRICE * energy * demand_multiplier`. | `PricingServer.calculatePrice()` calculates price based on station demand level (`S01` = LOW = 1.0 multiplier). | **Yes** |
| **8. Payment** | `PaymentServer` validates session completed, fetches energy from `ChargingSessionServer`, fetches price from `PricingServer`, creates payment record (`PAY-1001`), and marks payment `SUCCESS`. | `PaymentServer.makePayment()` validates completed session, gets energy, calls `PricingServer`, creates payment record. | **Yes** |
| **9. Final Port Release** | **ONLY AFTER SUCCESSFUL PAYMENT** should `PaymentServer` release charging port (`CHARGING` → `AVAILABLE`). | `PaymentServer` has no connection to `ChargingStationServer` and does **NOT** release the port because `stopCharging()` already released it prematurely. | **No (Missing Post-Payment Release)** |

---

## 4. Multithreading Audit

### Client Threads
- `EVClient` operates as a single-threaded interactive client.
- `MultithreadTest` creates a thread pool of 10 worker threads (`ExecutorService`) synchronized via `CountDownLatch` (start signal & completion signal) to fire 10 concurrent requests at the RMI backend.

### Server Threads & Synchronization Bottlenecks
- **`ChargingStationServer`**: All remote methods (`reserveAnyAvailablePort`, `startPortCharging`, `releasePort`, `checkPortAvailability`, etc.) are declared `synchronized`. This ensures double-booking protection on `portStatus[]`.
- **`ReservationServer`**: `reserveSlot()` is un-synchronized at method level and uses `synchronized(this)` for counter generation and HashMap insertion. However, `cancelReservation`, `getReservation`, and `getReservationPort` are method-level `synchronized` and hold locks during `simulateProcessing(...)` delays.
- **`ChargingSessionServer`**: All remote methods (`startCharging`, `stopCharging`, `getSessionStatus`, `getEnergyConsumed`) are method-level `synchronized` and hold lock during multiple `simulateProcessing(...)` delays (totaling 2–3 seconds per call). This serializes all concurrent sessions across RMI threads.
- **`PricingServer`**: `calculatePrice` and `getDemandMultiplier` are method-level `synchronized` holding lock during `simulateProcessing(...)`.
- **`PaymentServer`**: `makePayment`, `getPaymentStatus`, and `getPaymentDetails` are method-level `synchronized` holding lock during `simulateProcessing(...)` and RMI sub-calls (totaling ~3 seconds per call).

### Race Conditions & Double-Booking Protection
- Double-booking of charging ports is currently prevented by `ChargingStationServer`'s synchronized methods.
- Concurrent map access in `ChargingSessionServer` and `PaymentServer` is safe due to synchronization, but **concurrency performance is severely bottlenecked** by holding locks during simulated sleep delays.

---

## 5. Problems

### Problem 1: Premature Charging Port Release
- **Severity**: High
- **File**: `ChargingSession/ChargingSessionServer.java`
- **Method**: `stopCharging(String sessionId)`
- **Current Behavior**: Lines 447–470 call `chargingStation.releasePort(portId)` directly inside `stopCharging()`.
- **Problem**: Port becomes `AVAILABLE` before the user pays for the session.
- **Required Fix**: Remove `releasePort(portId)` call from `stopCharging()`. Add `getSessionPort(String sessionId)` to `ChargingSessionInterface` and `ChargingSessionServer`.

### Problem 2: Missing Post-Payment Charging Port Release
- **Severity**: High
- **File**: `Payment/PaymentServer.java`
- **Method**: `makePayment(String sessionId)`
- **Current Behavior**: `PaymentServer` does not release the charging port upon payment success.
- **Problem**: Port remains locked forever if `stopCharging()` is fixed not to release it.
- **Required Fix**: Connect `PaymentServer` to `ChargingStationServer` (Port 1234) and call `chargingStation.releasePort(portId)` inside `makePayment()` after setting payment status to `SUCCESS`.

### Problem 3: Method-Level Synchronization Bottlenecks Across RMI Servers
- **Severity**: Medium
- **Files**: `ChargingSessionServer.java`, `PaymentServer.java`, `ReservationServer.java`, `PricingServer.java`
- **Methods**: `startCharging`, `stopCharging`, `makePayment`, `calculatePrice`, `getReservation`, `cancelReservation`, etc.
- **Current Behavior**: Methods use method-level `synchronized` while calling `simulateProcessing(500-700ms)` and RMI sub-lookups.
- **Problem**: Blocks all concurrent RMI threads from processing other requests simultaneously, defeating RMI multi-threading.
- **Required Fix**: Remove method-level `synchronized` from these methods and scope `synchronized(this)` only around atomic map operations and counter increments.

### Problem 4: Hardcoded Initial Port State
- **Severity**: Low
- **File**: `ChargingStation/ChargingStationServer.java`
- **Field**: `portStatus`
- **Current Behavior**: Initialized as `{"AVAILABLE", "CHARGING", "AVAILABLE", "AVAILABLE"}` (Port `P2` starts as `CHARGING`).
- **Problem**: Reduces total available ports from 4 to 3 on server launch.
- **Required Fix**: Change initial state to `{"AVAILABLE", "AVAILABLE", "AVAILABLE", "AVAILABLE"}`.

---

## 6. Recommended Changes

1. **Remove Port Release from `stopCharging()`** in `ChargingSessionServer`.
2. **Add `getSessionPort(String sessionId)`** to `ChargingSessionInterface` and `ChargingSessionServer`.
3. **Connect `PaymentServer` to `ChargingStationServer`** and invoke `chargingStation.releasePort(portId)` upon payment success in `makePayment()`.
4. **Refactor Method-Level Synchronization** across all RMI servers to fine-grained block synchronization around shared state, allowing `simulateProcessing()` and remote RMI calls to run concurrently.
5. **Set Initial Port States to `AVAILABLE`** in `ChargingStationServer`.

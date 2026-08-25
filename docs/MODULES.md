# EV Charging RMI Distributed System - Module Specification & Function Reference

This document provides a comprehensive technical overview of each module in the Java RMI-based EV Charging Distributed System. For every module, it details the general overview, service architecture, RMI configuration, and detailed specifications for all public/remote interface methods and internal helper functions.

---

## System Architecture Summary & Communication Matrix

```
                          +-------------------------+
                          |   EVClient / Driver     |
                          | (Console CLI App / Test)|
                          +------------+------------+
                                       |
       +-------------------------------+-------------------------------+
       |                               |                               |
       v                               v                               v
+--------------+               +--------------+                +---------------+
| Reservation  |-------------->| Charging     |<---------------| Payment       |
| Server       | (Get Port ID) | Session      | (Query Energy/ | Server        |
| (Port 1235)  |               | Server       |  Port Status)  | (Port 1237)   |
+-------+------+               | (Port 1236)  |                +-------+-------+
        |                      +-------+------+                        |
        | (Reserve Port)               | (Start Charging Port)         | (Calculate Bill)
        v                              v                               v
+--------------+               +--------------+                +---------------+
| Charging     |<--------------+ Charging     |                | Pricing       |
| Station      | (Release Port | Station      |                | Server        |
| Server       |  on Payment)  | Server       |                | (Port 1238)   |
| (Port 1234)  |---------------+ (Port 1234)  |                +---------------+
+--------------+               +--------------+
```

| Module / Service | Port | RMI Registry URL & Service Name | Interface & Server Classes | Primary Responsibilities |
| :--- | :---: | :--- | :--- | :--- |
| **ChargingStation** | `1234` | `rmi://localhost:1234//ChargingStationServer` | [`ChargingStationInterface`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/ChargingStation/ChargingStationInterface.java)<br>[`ChargingStationServer`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/ChargingStation/ChargingStationServer.java) | Hardware port management (`P1`-`P4`), atomic physical state transitions (`AVAILABLE`, `RESERVED`, `CHARGING`). |
| **Reservation** | `1235` | `rmi://localhost:1235/ReservationService` | [`ReservationInterface`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Reservation/ReservationInterface.java)<br>[`ReservationServer`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Reservation/ReservationServer.java) | Manages user slot bookings, user/vehicle ID binding, charging port reservation delegation, and cancellations. |
| **ChargingSession** | `1236` | `rmi://localhost:1236/ChargingSessionServer` | [`ChargingSessionInterface`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/ChargingSession/ChargingSessionInterface.java)<br>[`ChargingSessionServer`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/ChargingSession/ChargingSessionServer.java) | Manages live charging session lifecycles (`CHARGING`, `COMPLETED`), reservation validation, and kWh energy consumption tracking. |
| **Pricing** | `1238` | `rmi://localhost:1238/PricingService` | [`PricingInterface`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Pricing/PricingInterface.java)<br>[`PricingServer`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Pricing/PricingServer.java) | Dynamic tariff calculation using base price and demand multipliers (`LOW` = 1.0x, `MEDIUM` = 1.25x, `HIGH` = 1.50x). |
| **Payment** | `1237` | `rmi://localhost:1237/PaymentServer` | [`PaymentInterface`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Payment/PaymentInterface.java)<br>[`PaymentServer`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Payment/PaymentServer.java) | Financial transaction settlement, multi-service price verification, payment logging, and post-payment hardware port release. |
| **EVClient** | N/A | Client Application (Looks up all 5 services) | [`EVClient`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/EVClient.java) | Unified interactive driver CLI menu driving end-to-end charging flows. |
| **MultithreadTest** | N/A | Test Harness (Looks up 4 services concurrently) | [`MultithreadTest`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/MultithreadTest.java) | Concurrency test suite simulating 10 parallel EV charging workflows. |

---

## 1. ChargingStation Module

### General Overview
The **ChargingStation Module** acts as the central resource coordinator for physical charging station hardware (Station ID: `EV-STATION-01`). It manages four physical ports (`P1`, `P2`, `P3`, `P4`) and maintains state transitions across three states: `AVAILABLE`, `RESERVED`, and `CHARGING`.

- **Files**:
  - Interface: [`ChargingStation/ChargingStationInterface.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/ChargingStation/ChargingStationInterface.java)
  - Implementation: [`ChargingStation/ChargingStationServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/ChargingStation/ChargingStationServer.java)
- **Port**: `1234`
- **RMI URL**: `rmi://localhost:1234//ChargingStationServer`
- **Dependencies**: None (standalone core resource server).
- **Concurrency & Thread Safety**: All public state-accessing and state-mutating methods are `synchronized` at the method level. This ensures thread-safe, atomic check-and-reserve operations when multiple clients or `ReservationServer` instances request ports simultaneously.

---

### Remote Interface Functions Overview (`ChargingStationInterface`)

#### `getStationStatus()`
- **Overview**: Returns a summary statement of the station's total capacity and current available port count.
- **Signature**: `String getStationStatus() throws RemoteException`
- **Parameters**: None
- **Returns**: `String` (e.g., `"Station EV-STATION-01: 4 of 4 ports available."`)
- **Synchronization**: `synchronized`

#### `getAvailablePorts()`
- **Overview**: Scans the port array and returns a space-separated list of all port IDs currently marked `AVAILABLE`.
- **Signature**: `String getAvailablePorts() throws RemoteException`
- **Parameters**: None
- **Returns**: `String` listing available port IDs (e.g., `"Available Ports: P1 P2 P3 P4 "`), or `"No ports are currently available."`
- **Synchronization**: `synchronized`

#### `checkPortAvailability(String portId)`
- **Overview**: Checks the specific state (`AVAILABLE`, `RESERVED`, or `CHARGING`) of a requested port ID.
- **Signature**: `String checkPortAvailability(String portId) throws RemoteException`
- **Parameters**:
  - `portId` (`String`): The port identifier (e.g., `"P1"`, `"P2"`).
- **Returns**: `String` describing current port state, or an error message if the port ID is invalid.
- **Synchronization**: `synchronized`

#### `reservePort(String portId)`
- **Overview**: Atomically validates if a requested port is `AVAILABLE` and transitions its status to `RESERVED`.
- **Signature**: `String reservePort(String portId) throws RemoteException`
- **Parameters**:
  - `portId` (`String`): The target port identifier.
- **Returns**: `String` indicating success (e.g., `"Port P1 reserved successfully."`) or refusal message.
- **Synchronization**: `synchronized`

#### `releasePort(String portId)`
- **Overview**: Resets a specified port's status back to `AVAILABLE` regardless of whether it was `RESERVED` or `CHARGING`. Called by `ReservationServer` on reservation cancellation and by `PaymentServer` after successful payment settlement.
- **Signature**: `String releasePort(String portId) throws RemoteException`
- **Parameters**:
  - `portId` (`String`): The port identifier to release.
- **Returns**: `String` confirmation message (e.g., `"Port P1 released successfully. Now AVAILABLE."`).
- **Synchronization**: `synchronized`

#### `reserveAnyAvailablePort()`
- **Overview**: Iterates sequentially through ports `P1`..`P4` and atomically reserves the first `AVAILABLE` port found. Used by `ReservationServer` to decouple client requests from hardcoded port identifiers.
- **Signature**: `String reserveAnyAvailablePort() throws RemoteException`
- **Parameters**: None
- **Returns**: `String` port ID (e.g., `"P1"`), or `"NONE"` if all ports are occupied.
- **Synchronization**: `synchronized`

#### `startPortCharging(String portId)`
- **Overview**: Validates that a port is currently `RESERVED` and transitions its state to `CHARGING`. Invoked remotely by `ChargingSessionServer`.
- **Signature**: `String startPortCharging(String portId) throws RemoteException`
- **Parameters**:
  - `portId` (`String`): The port identifier.
- **Returns**: `String` status code (`"CHARGING_STARTED"`, `"PORT_NOT_FOUND"`, or `"PORT_NOT_RESERVED"`).
- **Synchronization**: `synchronized`

---

### Internal Helper Methods (`ChargingStationServer`)

#### `indexOfPort(String portId)`
- **Overview**: Performs a case-insensitive lookup to find the array index (0 to 3) corresponding to a port ID.
- **Signature**: `private int indexOfPort(String portId)`
- **Parameters**: `portId` (`String`)
- **Returns**: `int` index (0 to 3), or `-1` if port does not exist.

#### `log(String message)`
- **Overview**: Utility method for thread-aware console logging, printing `[Thread-ID | Thread-Name] message`.
- **Signature**: `private void log(String message)`

#### `simulateProcessing(long milliseconds)`
- **Overview**: Introduces controlled execution pauses (`Thread.sleep`) to simulate real-world hardware latency.
- **Signature**: `private void simulateProcessing(long milliseconds)`

#### `main(String[] args)`
- **Overview**: Application entry point. Creates RMI registry on port `1234`, instantiates `ChargingStationServer`, and binds it to `rmi://localhost:1234//ChargingStationServer`.

---

## 2. Reservation Module

### General Overview
The **Reservation Module** handles EV slot booking operations, links driver user IDs and vehicle IDs with allocated hardware ports, maintains reservation details, and manages reservation cancellations.

- **Files**:
  - Interface: [`Reservation/ReservationInterface.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Reservation/ReservationInterface.java)
  - Implementation: [`Reservation/ReservationServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Reservation/ReservationServer.java)
- **Port**: `1235`
- **RMI URL**: `rmi://localhost:1235/ReservationService`
- **Dependencies**: Remote reference to `ChargingStationServer` (`rmi://localhost:1234//ChargingStationServer`).
- **Concurrency & Thread Safety**:
  - `reserveSlot()` is intentionally **non-synchronized** at the method level to maximize concurrency, allowing multiple driver threads to execute validation and setup simultaneously.
  - Resource protection is guaranteed because port allocation delegates to `ChargingStationServer.reserveAnyAvailablePort()`, which is synchronized.
  - Critical sections modifying internal shared structures (`reservationCounter`, `reservations` Map, `reservationPorts` Map) use fine-grained `synchronized(this)` blocks.

---

### Remote Interface Functions Overview (`ReservationInterface`)

#### `reserveSlot(String userId, String vehicleId)`
- **Overview**: Validates inputs, contacts `ChargingStationServer` to obtain an available port, generates a sequential reservation ID (`RES1001`, `RES1002`, ...), stores mapping records, and returns full reservation details.
- **Signature**: `String reserveSlot(String userId, String vehicleId) throws RemoteException`
- **Parameters**:
  - `userId` (`String`): Driver identifier.
  - `vehicleId` (`String`): Vehicle license plate or registration ID.
- **Returns**: `String` formatted reservation record or error message if no ports are available or inputs are invalid.
- **Synchronization**: Concurrently executable; internal map mutations and counter increments are protected by `synchronized(this)`.

#### `cancelReservation(String reservationId)`
- **Overview**: Searches for an existing reservation, removes it from memory maps, and remotely calls `ChargingStationServer.releasePort(portId)` to free up the hardware port.
- **Signature**: `String cancelReservation(String reservationId) throws RemoteException`
- **Parameters**:
  - `reservationId` (`String`): Target reservation ID.
- **Returns**: `String` confirmation message (e.g., `"Reservation RES1001 cancelled successfully."`) or error message if not found.
- **Synchronization**: `synchronized(this)` block for state lookup and removal.

#### `getReservation(String reservationId)`
- **Overview**: Retrieves stored details string for a given reservation ID. Used by clients and by `ChargingSessionServer` to verify validity before starting charging.
- **Signature**: `String getReservation(String reservationId) throws RemoteException`
- **Parameters**:
  - `reservationId` (`String`): Target reservation ID.
- **Returns**: `String` reservation details or `"Reservation RESxxx not found."`
- **Synchronization**: `synchronized(this)` read block.

#### `getReservationPort(String reservationId)`
- **Overview**: Retrieves the specific charging port ID assigned to a reservation ID. Used by `ChargingSessionServer`.
- **Signature**: `String getReservationPort(String reservationId) throws RemoteException`
- **Parameters**:
  - `reservationId` (`String`): Target reservation ID.
- **Returns**: `String` assigned port ID (e.g., `"P1"`), or `"NONE"` if not found.
- **Synchronization**: `synchronized(this)` read block.

---

### Internal Helper Methods (`ReservationServer`)

#### `log(String message)`
- **Overview**: Prints thread-tagged log messages to stdout.
- **Signature**: `private void log(String message)`

#### `simulateProcessing(long milliseconds)`
- **Overview**: Simulates processing latency.
- **Signature**: `private void simulateProcessing(long milliseconds)`

#### `main(String[] args)`
- **Overview**: Connects to `ChargingStationServer` on port `1234`, creates RMI registry on port `1235`, instantiates `ReservationServer`, and binds service to `rmi://localhost:1235/ReservationService`.

---

## 3. ChargingSession Module

### General Overview
The **ChargingSession Module** orchestrates active charging sessions. It validates reservations with `ReservationServer`, triggers hardware status changes via `ChargingStationServer`, records energy consumption (kWh), and updates session status (`CHARGING` -> `COMPLETED`).

- **Files**:
  - Interface: [`ChargingSession/ChargingSessionInterface.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/ChargingSession/ChargingSessionInterface.java)
  - Implementation: [`ChargingSession/ChargingSessionServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/ChargingSession/ChargingSessionServer.java)
- **Port**: `1236`
- **RMI URL**: `rmi://localhost:1236/ChargingSessionServer`
- **Dependencies**: Remote references to `ReservationServer` (`1235`) and `ChargingStationServer` (`1234`).
- **Concurrency & Thread Safety**: Uses `synchronized(this)` blocks around internal collections (`reservationSessions`, `sessionStatus`, `energyConsumed`, `sessionPort`, `sessionCounter`) to ensure thread-safe session tracking.

---

### Remote Interface Functions Overview (`ChargingSessionInterface`)

#### `startCharging(String reservationId)`
- **Overview**: Validates the reservation via `ReservationServer.getReservation()`, fetches assigned port via `ReservationServer.getReservationPort()`, instructs `ChargingStationServer.startPortCharging()` to change port state to `CHARGING`, generates unique session ID (`SESSION-1001`), and initializes session status to `CHARGING`.
- **Signature**: `String startCharging(String reservationId) throws RemoteException`
- **Parameters**:
  - `reservationId` (`String`): Verified reservation ID.
- **Returns**: `String` summary containing session ID, port, and status.
- **Synchronization**: `synchronized(this)` blocks for duplicate checks, ID generation, and map writes.

#### `stopCharging(String sessionId)`
- **Overview**: Validates active session, records energy consumed (simulated 25.0 kWh), and transitions session status to `COMPLETED`. Note: Hardware port is deliberately kept locked until payment is settled.
- **Signature**: `String stopCharging(String sessionId) throws RemoteException`
- **Parameters**:
  - `sessionId` (`String`): Active session ID.
- **Returns**: `String` confirmation with recorded energy consumption and completion note.
- **Synchronization**: `synchronized(this)` state mutation.

#### `getSessionStatus(String sessionId)`
- **Overview**: Returns current status (`CHARGING` or `COMPLETED`) and recorded energy consumption for a given session ID.
- **Signature**: `String getSessionStatus(String sessionId) throws RemoteException`
- **Parameters**:
  - `sessionId` (`String`): Target session ID.
- **Returns**: `String` formatted status report or `"Session not found."`
- **Synchronization**: `synchronized(this)` read.

#### `getEnergyConsumed(String sessionId)`
- **Overview**: Returns the exact double value of kWh energy consumed for a session. Used by `PaymentServer` to compute bill amounts.
- **Signature**: `double getEnergyConsumed(String sessionId) throws RemoteException`
- **Parameters**:
  - `sessionId` (`String`): Target session ID.
- **Returns**: `double` kWh energy value (e.g. `25.0`), or `-1.0` if session not found.
- **Synchronization**: `synchronized(this)` read.

#### `getSessionPort(String sessionId)`
- **Overview**: Retrieves the port ID assigned to a session. Used by `PaymentServer` to trigger post-payment port release on `ChargingStationServer`.
- **Signature**: `String getSessionPort(String sessionId) throws RemoteException`
- **Parameters**:
  - `sessionId` (`String`): Target session ID.
- **Returns**: `String` port ID (e.g. `"P1"`), or `"NONE"` if not found.
- **Synchronization**: `synchronized(this)` read.

---

### Internal Helper Methods (`ChargingSessionServer`)

#### `log(String message)`
- **Overview**: Logs thread identifier and message string.
- **Signature**: `private void log(String message)`

#### `simulateProcessing(long milliseconds)`
- **Overview**: Simulates processing latency.
- **Signature**: `private void simulateProcessing(long milliseconds)`

#### `main(String[] args)`
- **Overview**: Connects to `ChargingStationServer` (`1234`) and `ReservationServer` (`1235`), creates RMI registry on port `1236`, instantiates `ChargingSessionServer`, and binds service to `rmi://localhost:1236/ChargingSessionServer`.

---

## 4. Pricing Module

### General Overview
The **Pricing Module** provides dynamic tariff computation services for EV charging. It calculates total monetary cost based on standard base rates (Rs. 10.0 / kWh), energy consumed, and demand profile multipliers associated with specific station IDs.

- **Files**:
  - Interface: [`Pricing/PricingInterface.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Pricing/PricingInterface.java)
  - Implementation: [`Pricing/PricingServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Pricing/PricingServer.java)
- **Port**: `1238`
- **RMI URL**: `rmi://localhost:1238/PricingService`
- **Dependencies**: None (standalone tariff engine).
- **Tariff Structure**:
  - `BASE_PRICE` = Rs. 10.0 / kWh
  - Station `"S01"` -> `LOW` demand (Multiplier: 1.0x) -> Effective Rate: Rs. 10.0 / kWh
  - Station `"S02"` -> `MEDIUM` demand (Multiplier: 1.25x) -> Effective Rate: Rs. 12.5 / kWh
  - Station `"S03"` -> `HIGH` demand (Multiplier: 1.50x) -> Effective Rate: Rs. 15.0 / kWh

---

### Remote Interface Functions Overview (`PricingInterface`)

#### `calculatePrice(String stationId, double energyConsumed)`
- **Overview**: Calculates final price using formula: `BASE_PRICE * energyConsumed * multiplier`.
- **Signature**: `double calculatePrice(String stationId, double energyConsumed) throws RemoteException`
- **Parameters**:
  - `stationId` (`String`): Station identifier (e.g. `"S01"`).
  - `energyConsumed` (`double`): Energy consumed in kWh.
- **Returns**: `double` final cost in Rupees (e.g., `250.0` for 25 kWh on `S01`), or `-1.0` for invalid negative energy input.
- **Synchronization**: Thread-safe (stateless calculation over constant lookups).

#### `getDemandMultiplier(String stationId)`
- **Overview**: Looks up the station demand level and returns the associated multiplier (1.0, 1.25, or 1.50).
- **Signature**: `double getDemandMultiplier(String stationId) throws RemoteException`
- **Parameters**:
  - `stationId` (`String`): Station identifier.
- **Returns**: `double` multiplier value. Default is `1.0` (`LOW` demand) if station is unlisted.
- **Synchronization**: Thread-safe read.

---

### Internal Helper Methods (`PricingServer`)

#### `log(String message)`
- **Overview**: Thread-aware logger.
- **Signature**: `private void log(String message)`

#### `simulateProcessing(long milliseconds)`
- **Overview**: Simulates processing latency.
- **Signature**: `private void simulateProcessing(long milliseconds)`

#### `main(String[] args)`
- **Overview**: Application entry point. Creates RMI registry on port `1238`, instantiates `PricingServer`, and binds service to `rmi://localhost:1238/PricingService`.

---

## 5. Payment Module

### General Overview
The **Payment Module** coordinates financial settlements for completed charging sessions. It verifies session completion with `ChargingSessionServer`, retrieves energy consumed, queries `PricingServer` for accurate bill calculation, generates payment records (`PAY-1001`), and instructs `ChargingStationServer` to release the physical port back to `AVAILABLE`.

- **Files**:
  - Interface: [`Payment/PaymentInterface.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Payment/PaymentInterface.java)
  - Implementation: [`Payment/PaymentServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Payment/PaymentServer.java)
- **Port**: `1237`
- **RMI URL**: `rmi://localhost:1237/PaymentServer`
- **Dependencies**: Remote references to `ChargingSessionServer` (`1236`), `PricingServer` (`1238`), and `ChargingStationServer` (`1234`).
- **Concurrency & Thread Safety**: Synchronizes payment record creation, payment ID generation (`PAY-1001`), and internal storage maps (`paymentStatus`, `paymentDetails`) using `synchronized(this)`.

---

### Remote Interface Functions Overview (`PaymentInterface`)

#### `makePayment(String sessionId)`
- **Overview**: Executes complete financial settlement flow:
  1. Queries `ChargingSessionServer.getSessionStatus()` to verify session is `COMPLETED`.
  2. Queries `ChargingSessionServer.getEnergyConsumed()` to get consumed kWh.
  3. Queries `PricingServer.calculatePrice()` to compute total amount due.
  4. Generates payment ID (`PAY-xxxx`) and saves transaction record with status `SUCCESS`.
  5. Retrieves assigned port via `ChargingSessionServer.getSessionPort()` and calls `ChargingStationServer.releasePort(portId)` to release hardware port.
- **Signature**: `String makePayment(String sessionId) throws RemoteException`
- **Parameters**:
  - `sessionId` (`String`): Completed session ID.
- **Returns**: `String` detailed receipt containing Payment ID, Session ID, Energy, Amount, Status, and Port Release confirmation.
- **Synchronization**: `synchronized(this)` for ID counter and record maps.

#### `getPaymentStatus(String paymentId)`
- **Overview**: Looks up payment status (`SUCCESS`) for a given payment ID.
- **Signature**: `String getPaymentStatus(String paymentId) throws RemoteException`
- **Parameters**:
  - `paymentId` (`String`): Target payment ID.
- **Returns**: `String` payment status or `"Payment not found."`
- **Synchronization**: `synchronized(this)` read block.

#### `getPaymentDetails(String paymentId)`
- **Overview**: Retrieves full stored itemized payment breakdown by payment ID.
- **Signature**: `String getPaymentDetails(String paymentId) throws RemoteException`
- **Parameters**:
  - `paymentId` (`String`): Target payment ID.
- **Returns**: `String` itemized receipt details or `"Payment not found."`
- **Synchronization**: `synchronized(this)` read block.

---

### Internal Helper Methods (`PaymentServer`)

#### `PaymentServer(ChargingSessionInterface, PricingInterface, ChargingStationInterface)`
- **Overview**: Primary constructor receiving remote references to dependent microservices.

#### `PaymentServer(ChargingSessionInterface, PricingInterface)`
- **Overview**: Overloaded constructor providing backwards compatibility by setting `chargingStation` reference to `null`.

#### `log(String message)`
- **Overview**: Thread-aware logger.
- **Signature**: `private void log(String message)`

#### `simulateProcessing(long milliseconds)`
- **Overview**: Simulates processing latency.
- **Signature**: `private void simulateProcessing(long milliseconds)`

#### `main(String[] args)`
- **Overview**: Connects to `ChargingStationServer` (`1234`), `ChargingSessionServer` (`1236`), and `PricingServer` (`1238`), creates RMI registry on port `1237`, instantiates `PaymentServer`, and binds to `rmi://localhost:1237/PaymentServer`.

---

## 6. EVClient (Interactive CLI Driver)

### General Overview
The **EVClient** module is a unified, console-based mobile-app-style driver client. It acts as an RMI client only (never exports RMI objects) and dynamically looks up all 5 remote RMI services to provide an interactive 14-option menu covering the complete EV lifecycle.

- **File**: [`EVClient.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/EVClient.java)
- **Role**: Console Client Application.
- **State Management**: Maintains local user context (`userId`, `vehicleId`) and caches recent IDs (`lastReservationId`, `lastSessionId`, `lastPaymentId`) to default user prompts in subsequent menu choices.

---

### Menu Operations & Functions Reference (`EVClient`)

#### `main(String[] args)`
- **Overview**: Prompts for User ID and Vehicle ID, then runs infinite menu loop until exit option (14) is selected.

#### Remote Lookup Functions
- `lookupChargingStation()`: Performs RMI lookup for `rmi://localhost:1234//ChargingStationServer`.
- `lookupReservation()`: Performs RMI lookup for `rmi://localhost:1235/ReservationService`.
- `lookupChargingSession()`: Performs RMI lookup for `rmi://localhost:1236/ChargingSessionServer`.
- `lookupPayment()`: Performs RMI lookup for `rmi://localhost:1237/PaymentServer`.
- `lookupPricing()`: Performs RMI lookup for `rmi://localhost:1238/PricingService`.

#### Menu Option Action Handlers
1. `viewStationStatus()`: Displays total vs available station ports.
2. `viewAvailablePorts()`: Lists currently available port IDs.
3. `checkPortAvailability(Scanner)`: Prompts for port ID and checks status.
4. `reserveSlot()`: Calls `ReservationServer.reserveSlot()` and updates `lastReservationId`.
5. `checkReservation(Scanner)`: Displays reservation record.
6. `cancelReservation(Scanner)`: Cancels reservation and clears cached reservation ID.
7. `startCharging(Scanner)`: Calls `ChargingSessionServer.startCharging()` and updates `lastSessionId`.
8. `checkSession(Scanner)`: Displays session status and energy consumed.
9. `stopCharging(Scanner)`: Stops charging and records energy.
10. `calculateBill(Scanner)`: Fetches real energy consumed from `ChargingSessionServer` and estimates bill via `PricingServer`.
11. `makePayment(Scanner)`: Calls `PaymentServer.makePayment()` and updates `lastPaymentId`.
12. `checkPaymentStatus(Scanner)`: Queries payment status.
13. `viewPaymentDetails(Scanner)`: Displays complete payment receipt.
14. Exit handler: Closes scanner and terminates client.

#### Helper Utilities
- `printMenu()`: Prints 14-option user menu.
- `promptWithDefault(Scanner sc, String label, String defaultValue)`: Displays prompt line with default value option when available.
- `extractField(String text, String label)`: Parses key-value tokens (e.g. `"Reservation ID: RES1001"`) out of server response strings.
- Error notification helpers: `stationUnavailable()`, `reservationUnavailable()`, `sessionUnavailable()`, `pricingUnavailable()`, `paymentUnavailable()`.

---

## 7. MultithreadTest (Concurrency Verification Suite)

### General Overview
The **MultithreadTest** module is an automated stress and concurrency test harness. It simulates 10 concurrent EV drivers executing the end-to-end distributed workflow simultaneously.

- **File**: [`MultithreadTest.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/MultithreadTest.java)
- **Role**: Concurrency Test Suite.
- **Workflow Executed per Simulated EV**:
  `RESERVE SLOT` -> `START CHARGING` -> `STOP CHARGING` -> `PRICING ESTIMATION` -> `PAYMENT & PORT RELEASE`

---

### Key Components & Functions Reference (`MultithreadTest`)

#### `main(String[] args)`
- **Overview**:
  1. Instantiates `FixedThreadPool` with 10 threads.
  2. Uses `CountDownLatch startSignal = new CountDownLatch(1)` to synchronize simultaneous thread release.
  3. Uses `CountDownLatch completionSignal = new CountDownLatch(10)` to wait for all simulated EVs to complete.
  4. Each thread creates independent RMI lookups for all four remote servers.
  5. Triggers concurrent workflow, measures total execution time, and reports success matrix for all 5 servers.

#### `extractReservationId(String response)`
- **Overview**: Parses `"Reservation ID:"` substring token out of `ReservationServer` response string.
- **Signature**: `private static String extractReservationId(String response)`

#### `extractSessionId(String response)`
- **Overview**: Parses `"Session ID:"` substring token out of `ChargingSessionServer` response string.
- **Signature**: `private static String extractSessionId(String response)`

---

## Summary Table of File Locations

| Module | File Path |
| :--- | :--- |
| ChargingStation Interface | [`ChargingStation/ChargingStationInterface.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/ChargingStation/ChargingStationInterface.java) |
| ChargingStation Implementation | [`ChargingStation/ChargingStationServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/ChargingStation/ChargingStationServer.java) |
| Reservation Interface | [`Reservation/ReservationInterface.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Reservation/ReservationInterface.java) |
| Reservation Implementation | [`Reservation/ReservationServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Reservation/ReservationServer.java) |
| ChargingSession Interface | [`ChargingSession/ChargingSessionInterface.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/ChargingSession/ChargingSessionInterface.java) |
| ChargingSession Implementation | [`ChargingSession/ChargingSessionServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/ChargingSession/ChargingSessionServer.java) |
| Pricing Interface | [`Pricing/PricingInterface.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Pricing/PricingInterface.java) |
| Pricing Implementation | [`Pricing/PricingServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Pricing/PricingServer.java) |
| Payment Interface | [`Payment/PaymentInterface.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Payment/PaymentInterface.java) |
| Payment Implementation | [`Payment/PaymentServer.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/Payment/PaymentServer.java) |
| EV Client Application | [`EVClient.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/EVClient.java) |
| Multithread Test Harness | [`MultithreadTest.java`](file:///c:/Users/asus/Desktop/College/Sem%205/DC/Java/MultithreadTest.java) |

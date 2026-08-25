# EV Charging Network Management System — RMI Interfaces & Communication Protocol

## Remote Interfaces Summary

The system defines 5 Java RMI Remote Interfaces, extending `java.rmi.Remote`, with all methods throwing `java.rmi.RemoteException`.

---

## Interface Specifications

### 1. `ChargingStationInterface`
- **Package / Location**: `ChargingStation/ChargingStationInterface.java`
- **Implemented By**: `ChargingStationServer` (Port 1234, URL: `rmi://localhost:1234//ChargingStationServer`)

```java
public interface ChargingStationInterface extends Remote {
    String getStationStatus() throws RemoteException;
    String getAvailablePorts() throws RemoteException;
    String checkPortAvailability(String portId) throws RemoteException;
    String reservePort(String portId) throws RemoteException;
    String releasePort(String portId) throws RemoteException;
    String reserveAnyAvailablePort() throws RemoteException;
    String startPortCharging(String portId) throws RemoteException;
}
```

| Method | Parameters | Return Value | Description |
|--------|------------|--------------|-------------|
| `getStationStatus` | None | String summary | Returns total available vs total ports on station. |
| `getAvailablePorts` | None | Space-separated port IDs | Lists all currently AVAILABLE ports. |
| `checkPortAvailability` | `portId` | Port status string | Checks status of a specific port (e.g. "P1 is AVAILABLE."). |
| `reservePort` | `portId` | Confirmation string | Reserves specific port if AVAILABLE. |
| `releasePort` | `portId` | Confirmation string | Releases port (RESERVED/CHARGING) back to AVAILABLE. |
| `reserveAnyAvailablePort` | None | Port ID or `"NONE"` | Atomically finds and reserves first available port. |
| `startPortCharging` | `portId` | `"CHARGING_STARTED"`, `"PORT_NOT_FOUND"`, `"PORT_NOT_RESERVED"` | Transitions RESERVED port to CHARGING. |

---

### 2. `ReservationInterface`
- **Package / Location**: `Reservation/ReservationInterface.java`
- **Implemented By**: `ReservationServer` (Port 1235, URL: `rmi://localhost:1235/ReservationService`)

```java
public interface ReservationInterface extends Remote {
    String reserveSlot(String userId, String vehicleId) throws RemoteException;
    String cancelReservation(String reservationId) throws RemoteException;
    String getReservation(String reservationId) throws RemoteException;
    String getReservationPort(String reservationId) throws RemoteException;
}
```

| Method | Parameters | Return Value | Description |
|--------|------------|--------------|-------------|
| `reserveSlot` | `userId`, `vehicleId` | Reservation details string | Allocates port via `ChargingStationServer` & creates reservation record. |
| `cancelReservation` | `reservationId` | Status string | Cancels reservation & releases assigned port on station. |
| `getReservation` | `reservationId` | Reservation details string | Retrieves full reservation details. |
| `getReservationPort` | `reservationId` | Port ID or `"NONE"` | Returns charging port assigned to reservation. |

---

### 3. `ChargingSessionInterface`
- **Package / Location**: `ChargingSession/ChargingSessionInterface.java`
- **Implemented By**: `ChargingSessionServer` (Port 1236, URL: `rmi://localhost:1236/ChargingSessionServer`)

```java
public interface ChargingSessionInterface extends Remote {
    String startCharging(String reservationId) throws RemoteException;
    String stopCharging(String sessionId) throws RemoteException;
    String getSessionStatus(String sessionId) throws RemoteException;
    double getEnergyConsumed(String sessionId) throws RemoteException;
    String getSessionPort(String sessionId) throws RemoteException;
}
```

| Method | Parameters | Return Value | Description |
|--------|------------|--------------|-------------|
| `startCharging` | `reservationId` | Session confirmation string | Validates reservation, starts port charging, creates session. |
| `stopCharging` | `sessionId` | Session summary string | Marks session COMPLETED & records energy consumed (port stays locked). |
| `getSessionStatus` | `sessionId` | Status details string | Returns current status and energy consumed. |
| `getEnergyConsumed` | `sessionId` | double (kWh) or `-1` | Returns numerical energy consumed for billing. |
| `getSessionPort` | `sessionId` | Port ID or `"NONE"` | Returns port ID associated with session for post-payment release. |

---

### 4. `PricingInterface`
- **Package / Location**: `Pricing/PricingInterface.java`
- **Implemented By**: `PricingServer` (Port 1238, URL: `rmi://localhost:1238/PricingService`)

```java
public interface PricingInterface extends Remote {
    double calculatePrice(String stationId, double energyConsumed) throws RemoteException;
    double getDemandMultiplier(String stationId) throws RemoteException;
}
```

| Method | Parameters | Return Value | Description |
|--------|------------|--------------|-------------|
| `calculatePrice` | `stationId`, `energyConsumed` | double (Rs.) or `-1` | Computes price = `BASE_PRICE * energy * demandMultiplier`. |
| `getDemandMultiplier` | `stationId` | double multiplier | Returns multiplier based on station demand level (LOW=1.0, MED=1.25, HIGH=1.5). |

---

### 5. `PaymentInterface`
- **Package / Location**: `Payment/PaymentInterface.java`
- **Implemented By**: `PaymentServer` (Port 1237, URL: `rmi://localhost:1237/PaymentServer`)

```java
public interface PaymentInterface extends Remote {
    String makePayment(String sessionId) throws RemoteException;
    String getPaymentStatus(String paymentId) throws RemoteException;
    String getPaymentDetails(String paymentId) throws RemoteException;
}
```

| Method | Parameters | Return Value | Description |
|--------|------------|--------------|-------------|
| `makePayment` | `sessionId` | Payment summary string | Verifies completed session, gets energy & price, creates receipt, and releases charging port on station. |
| `getPaymentStatus` | `paymentId` | Status string | Checks payment status (`SUCCESS`). |
| `getPaymentDetails` | `paymentId` | Full receipt string | Returns full payment breakdown. |

---

## Inter-Server Call Paths Matrix

```
Client (EVClient / MultithreadTest)
   │
   ├──► ReservationServer.reserveSlot()
   │       └──► ChargingStationServer.reserveAnyAvailablePort()
   │
   ├──► ChargingSessionServer.startCharging()
   │       ├──► ReservationServer.getReservation()
   │       ├──► ReservationServer.getReservationPort()
   │       └──► ChargingStationServer.startPortCharging()
   │
   ├──► ChargingSessionServer.stopCharging()
   │       └──► (Updates local state only; does NOT touch ChargingStationServer)
   │
   ├──► PricingServer.calculatePrice()
   │       └──► (Calculates price based on demand map)
   │
   └──► PaymentServer.makePayment()
           ├──► ChargingSessionServer.getSessionStatus()
           ├──► ChargingSessionServer.getEnergyConsumed()
           ├──► PricingServer.calculatePrice()
           ├──► ChargingSessionServer.getSessionPort()
           └──► ChargingStationServer.releasePort()  <-- Post-Payment Release!
```

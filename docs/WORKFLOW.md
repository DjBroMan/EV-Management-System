# EV Charging Network Management System — Workflow & Clock Lifecycle

## Complete Business Workflow & Event Timeline

The business workflow consists of 6 sequential steps, with Lamport logical timestamps propagating across every stage and physical time tracking charging duration:

```
[1. RESERVE & REPLICATE] ---> [2. START CHARGING] ---> [3. STOP CHARGING] ---> [4. GET ENERGY & PRICING] ---> [5. MAKE PAYMENT] ---> [6. PORT RELEASE]
```

### Step-by-Step Event Lifecycle & Energy Calculation

1. **Reserve Slot & Replicate State**:
   - Client sends `reserveSlot(userId, vehicleId, clientLamport)` with Lamport $L_{\text{client\_send1}}$.
   - `ReservationServer` (PRIMARY) receives call, updates $L = \max(L_{\text{server}}, L_{\text{client\_send1}}) + 1$, logs `[Event=RECEIVE]`.
   - `ReservationServer` calls `ChargingStationServer.reserveAnyAvailablePort(sendL)`.
   - `ChargingStationServer` allocates port `P1` $\rightarrow$ `RESERVED`, logs `[Event=LOCAL]`, returns `LamportResult("P1", stationL)`.
   - `ReservationServer` updates local in-memory state: `RES1001 -> P1`.
   - `ReservationServer` calls `ReservationServerManager.replicateReservation(...)`.
   - `ReservationServerManager` forwards state update to `ReservationServer` (SECONDARY).
   - `ReservationServer` (SECONDARY) stores `RES1001 -> P1` and increments counter.
   - Primary receives acknowledgment and returns `LamportResult(details, resL)` to client.

2. **Start Charging**:
   - Client sends `startCharging(reservationId, clientLamport)`.
   - `ChargingSessionServer` verifies reservation and port status.
   - `ChargingStationServer` transitions port `P1` $\rightarrow$ `CHARGING`.
   - `ChargingSessionServer` records physical start time $T_{\text{start}} = \text{Instant.now()}$, sets default charging power ($7.2\text{ kW}$), creates `SESSION-1001`, and returns `LamportResult(details, sessL)`.

3. **Stop Charging & Real-Time Energy Calculation**:
   - Client sends `stopCharging(sessionId, clientLamport)`.
   - `ChargingSessionServer` records physical end time $T_{\text{end}} = \text{Instant.now()}$.
   - **Duration Calculation**:
     $$\text{Duration (seconds)} = T_{\text{end}} - T_{\text{start}}$$
     $$\text{Duration (hours)} = \frac{\text{Duration (seconds)}}{3600.0}$$
   - **Energy Consumption Formula**:
     $$\text{Energy (kWh)} = \text{Charging Power (kW)} \times \text{Duration (hours)}$$
   - `ChargingSessionServer` updates session status $\rightarrow$ `COMPLETED`, logs `[Event=LOCAL]` energy calculation, and returns session breakdown (Start Time, End Time, Duration, Charging Power, Energy Consumed). Port `P1` remains locked in `CHARGING` state.

4. **Pricing Computation**:
   - `PaymentServer` queries `ChargingSessionServer.getEnergyConsumed(sessionId)`.
   - `PaymentServer` invokes `PricingServer.calculatePrice("S01", energyConsumed, sendL)`.
   - `PricingServer` computes total bill using:
     $$\text{Bill} = \text{Base Price (Rs. 10.0)} \times \text{Energy (kWh)} \times \text{Demand Multiplier}$$

5. **Payment Settlement & Post-Payment Port Release**:
   - `PaymentServer` verifies bill, creates payment receipt `PAY-1001` with status `SUCCESS`.
   - `PaymentServer` invokes `ChargingStationServer.releasePort("P1", sendL)`.
   - `ChargingStationServer` transitions port `P1` $\rightarrow$ `AVAILABLE`.

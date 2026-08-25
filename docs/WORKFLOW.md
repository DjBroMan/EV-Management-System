# EV Charging Network Management System — End-to-End Business Workflow

## Complete Charging & Payment Lifecycle

```
EVClient
   │
   │ 1. reserveSlot(userId, vehicleId)
   ▼
ReservationServer
   │
   │ 2. reserveAnyAvailablePort()
   ▼
ChargingStationServer ────────► Port State: AVAILABLE → RESERVED
   │
   │ 3. Reservation ID generated (RES1001)
   ▼
EVClient
   │
   │ 4. startCharging(reservationId)
   ▼
ChargingSessionServer
   │
   ├─► 5. getReservation(reservationId) ──► ReservationServer (Validate)
   ├─► 6. getReservationPort(reservationId) ─► ReservationServer (Fetch Port)
   │
   │ 7. startPortCharging(portId)
   ▼
ChargingStationServer ────────► Port State: RESERVED → CHARGING
   │
   │ 8. Session ID generated (SESSION-1001)
   ▼
EVClient (EV Charging...)
   │
   │ 9. stopCharging(sessionId)
   ▼
ChargingSessionServer ────────► Session State: CHARGING → COMPLETED
   │                            Energy Consumed Recorded: 25.0 kWh
   │                            (Port remains CHARGING / locked)
   ▼
EVClient
   │
   │ 10. makePayment(sessionId)
   ▼
PaymentServer
   │
   ├─► 11. getSessionStatus(sessionId) ──► ChargingSessionServer (Verify COMPLETED)
   ├─► 12. getEnergyConsumed(sessionId) ─► ChargingSessionServer (Fetch kWh)
   ├─► 13. calculatePrice(stationId, energy) ─► PricingServer (Calculate Bill)
   │
   │ 14. Payment ID generated (PAY-1001, Status: SUCCESS)
   │ 15. getSessionPort(sessionId) ──────► ChargingSessionServer (Fetch Port)
   │ 16. releasePort(portId)
   ▼
ChargingStationServer ────────► Port State: CHARGING → AVAILABLE
   │
   ▼
EVClient (Transaction Completed)
```

---

## Detailed Step-by-Step Execution Sequence

### Phase 1: Slot Reservation
1. `EVClient` calls `ReservationServer.reserveSlot("USER-1", "EV-1")`.
2. `ReservationServer` contacts `ChargingStationServer.reserveAnyAvailablePort()`.
3. `ChargingStationServer` scans ports (`P1`–`P4`), finds the first `AVAILABLE` port (e.g., `P1`), updates its status to `RESERVED`, and returns `P1`.
4. `ReservationServer` generates a unique reservation ID (`RES1001`), creates a confirmation record, maps `RES1001 → P1`, and returns details to `EVClient`.

### Phase 2: Charging Session Initiation
5. `EVClient` calls `ChargingSessionServer.startCharging("RES1001")`.
6. `ChargingSessionServer` queries `ReservationServer.getReservation("RES1001")` to verify the reservation is valid and active.
7. `ChargingSessionServer` queries `ReservationServer.getReservationPort("RES1001")` to retrieve the assigned port (`P1`).
8. `ChargingSessionServer` calls `ChargingStationServer.startPortCharging("P1")`.
9. `ChargingStationServer` updates port `P1` status from `RESERVED` to `CHARGING`.
10. `ChargingSessionServer` creates a session record (`SESSION-1001`), sets status to `CHARGING`, initializes energy to `0.0 kWh`, and returns success to `EVClient`.

### Phase 3: Charging Session Termination
11. `EVClient` calls `ChargingSessionServer.stopCharging("SESSION-1001")`.
12. `ChargingSessionServer` calculates total energy consumed (`25.0 kWh`).
13. `ChargingSessionServer` updates session status from `CHARGING` to `COMPLETED`.
14. **Crucial Rule**: The port (`P1`) **remains in CHARGING state** (locked) and is **NOT released** during `stopCharging()`.

### Phase 4: Pricing & Payment
15. `EVClient` calls `PaymentServer.makePayment("SESSION-1001")`.
16. `PaymentServer` checks `ChargingSessionServer.getSessionStatus("SESSION-1001")` to ensure charging is `COMPLETED`.
17. `PaymentServer` calls `ChargingSessionServer.getEnergyConsumed("SESSION-1001")` to retrieve `25.0 kWh`.
18. `PaymentServer` calls `PricingServer.calculatePrice("S01", 25.0)` to compute the bill (`Rs. 250.00`).
19. `PaymentServer` generates a payment receipt (`PAY-1001`) with status `SUCCESS`.

### Phase 5: Post-Payment Port Release
20. **Upon successful payment**, `PaymentServer` calls `ChargingSessionServer.getSessionPort("SESSION-1001")` to retrieve port `P1`.
21. `PaymentServer` calls `ChargingStationServer.releasePort("P1")`.
22. `ChargingStationServer` updates port `P1` status from `CHARGING` to `AVAILABLE`.
23. Port `P1` is now ready for future reservations.

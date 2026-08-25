# EV Charging Network Management System — Project Structure

## Final Project Layout

```
c:/Users/asus/Desktop/College/Sem 5/DC/Java/
│
├── EVClient.java                         # Primary interactive console application client
├── MultithreadTest.java                  # Multi-threaded concurrency & stress test client
│
├── ChargingStation/                      # Station & Port Management Subsystem
│   ├── ChargingStationInterface.java     # Remote interface for station operations
│   └── ChargingStationServer.java        # RMI Server (Port 1234)
│
├── Reservation/                          # Slot Booking Subsystem
│   ├── ReservationInterface.java         # Remote interface for reservations
│   └── ReservationServer.java            # RMI Server (Port 1235)
│
├── ChargingSession/                      # Session Tracking Subsystem
│   ├── ChargingSessionInterface.java     # Remote interface for charging sessions
│   └── ChargingSessionServer.java        # RMI Server (Port 1236)
│
├── Pricing/                              # Dynamic Pricing Subsystem
│   ├── PricingInterface.java             # Remote interface for pricing calculations
│   └── PricingServer.java                # RMI Server (Port 1238)
│
├── Payment/                              # Payment Processing & Port Release Subsystem
│   ├── PaymentInterface.java             # Remote interface for payment processing
│   └── PaymentServer.java                # RMI Server (Port 1237)
│
├── bin/                                  # Compiled bytecode output directory (.class files)
│
└── docs/                                 # Documentation Directory
    ├── AUDIT.md                          # Full code audit report & problem statements
    ├── ARCHITECTURE.md                   # System architecture overview & dependencies
    ├── WORKFLOW.md                       # Complete end-to-end business workflow
    ├── RMI_COMMUNICATION.md              # RMI interfaces & remote communication details
    ├── MULTITHREADING.md                 # Threading model & double-booking protection
    ├── STATE_MANAGEMENT.md               # State machine specifications & transitions
    ├── TESTING.md                        # Compilation & execution instructions
    └── PROJECT_STRUCTURE.md              # This file
```

---

## Architectural Rules & Verification
- **Primary Client**: `EVClient.java` is the ONLY interactive application client.
- **Concurrency Test**: `MultithreadTest.java` is the single load test.
- **No Dummy Clients**: Dummy per-module clients (`ChargingStationClient`, `ReservationClient`, `ChargingSessionClient`, `PricingClient`, `PaymentClient`) have been removed and are NOT recreated.
- **Backend Architecture**: Consists of 5 independent RMI servers communicating over designated RMI registry ports (1234–1238).

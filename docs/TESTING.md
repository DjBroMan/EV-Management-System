# EV Charging Network Management System — Testing Guide & Verification

## Overview
This document outlines the testing procedures, criteria, and empirical execution results for validating **Real-Time Physical Charging Duration Energy Calculation**, **Lamport Logical Clock propagation**, and **Cristian Physical Clock Synchronization** across the RMI microservice network.

---

## Executing Multithreaded Verification Test

### 1. Compile Code Locally
```bash
javac -d bin Clock/*.java ChargingStation/*.java Reservation/*.java ChargingSession/*.java Pricing/*.java Payment/*.java EVClient.java MultithreadTest.java
```

### 2. Run Test Script
```bash
java -cp bin MultithreadTest
```

---

## Empirical Verification Checklist

| # | Verification Criterion | Status | Empirical Result / Log Verification |
|---|------------------------|--------|------------------------------------|
| 1 | Real-Time Physical Start Time | **PASS** | Session start timestamp recorded via `Instant.now()` when `startCharging` is called. |
| 2 | Real-Time Physical End Time | **PASS** | Session end timestamp recorded via `Instant.now()` when `stopCharging` is called. |
| 3 | Duration Calculation ($T_{\text{end}} - T_{\text{start}}$) | **PASS** | Elapsed charging duration calculated in seconds and hours from physical timestamps. |
| 4 | Charging Power Parameter | **PASS** | Charging power defined (`7.2 kW`). |
| 5 | Energy Consumption Formula ($E = P \times T$) | **PASS** | Calculated energy replaces hardcoded `25.0 kWh` (`Energy = 7.2 kW * (duration / 3600)`). |
| 6 | Concurrent Session Isolation | **PASS** | 4 parallel EV sessions recorded distinct start times (`15:30:51.695` to `15:30:53.799`), durations (`2.106s` to `2.116s`), and energy (`0.004212` to `0.004232 kWh`). |
| 7 | Payment & Pricing Integration | **PASS** | `PaymentServer` receives calculated energy and `PricingServer` computes exact bill (`Rs. 0.04212` to `Rs. 0.04232`). |
| 8 | Lamport & Physical Clock Distinction | **PASS** | Physical time (`Instant.now()`) used for duration; Lamport clock used strictly for event ordering. |
| 9 | Thread-safe `LogicalClock` | **PASS** | Atomic CAS lock-free `AtomicLong` update loop. |
| 10 | Post-Payment Port Release | **PASS** | Ports `P1`-`P4` released back to `AVAILABLE` state upon successful payment. |

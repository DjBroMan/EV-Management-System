# EV Charging Network Management System — Testing Guide & Verification

## Overview
This document outlines the testing procedures, criteria, and empirical execution results for validating **Primary-Backup State Replication**, **Real-Time Physical Charging Duration Energy Calculation**, **Lamport Logical Clock propagation**, and **Cristian Physical Clock Synchronization** across the RMI microservice network.

---

## 1. Primary-Backup State Replication Verification (`ReplicationTest.java`)

### Compile and Run
```bash
# Compile all Java sources
javac -d bin Clock/*.java ChargingStation/*.java Reservation/*.java ChargingSession/*.java Pricing/*.java Payment/*.java EVClient.java MultithreadTest.java ReplicationTest.java

# Run Replication Test Suite
java -cp bin ReplicationTest
```

### Test Scenarios & Results

| # | Test Scenario | Expected Outcome | Status |
|---|---------------|------------------|--------|
| 1 | Startup of Primary, Secondary, and Manager | Nodes bind to 1235, 1245, and 1240 registries | **PASS** |
| 2 | Single Reservation Replication | Primary confirms `RES1001 -> P1`; Secondary mirrors state without port double-booking | **PASS** |
| 3 | Multiple Reservations Consistency | Both nodes maintain identical maps (`RES1001`, `RES1002`, `RES1003`) | **PASS** |
| 4 | Cancellation Replication | Cancellation of `RES1002` removes record on both nodes | **PASS** |
| 5 | Multithreaded Concurrent Load | Parallel thread reservations safely replicate without map corruption | **PASS** |
| 6 | Full State Synchronization | Late-joining Secondary pulls complete snapshot from Primary | **PASS** |
| 7 | Primary Failure & Failover Promotion | Manager detects failure and promotes Secondary to PRIMARY | **PASS** |
| 8 | Post-Failover Client Execution | Promoted Primary creates new reservations continuing sequence (`RES1006`) | **PASS** |

---

## 2. Multithreaded Workflow Verification (`MultithreadTest.java`)

```bash
java -cp bin MultithreadTest
```

### Empirical Verification Checklist

| # | Verification Criterion | Status | Empirical Result / Log Verification |
|---|------------------------|--------|------------------------------------|
| 1 | Real-Time Physical Start Time | **PASS** | Session start timestamp recorded via `Instant.now()`. |
| 2 | Real-Time Physical End Time | **PASS** | Session end timestamp recorded via `Instant.now()`. |
| 3 | Duration Calculation ($T_{\text{end}} - T_{\text{start}}$) | **PASS** | Elapsed charging duration calculated in seconds and hours. |
| 4 | Energy Consumption Formula ($E = P \times T$) | **PASS** | Calculated energy replaces static values (`Energy = 7.2 kW * (duration / 3600)`). |
| 5 | Concurrent Session Isolation | **PASS** | Parallel EV sessions record distinct start times, durations, and energy. |
| 6 | Primary-Backup State Replication | **PASS** | Verified across all 8 replication test cases. |
| 7 | Lamport & Physical Clock Distinction | **PASS** | Physical time used for duration; Lamport clock used strictly for event ordering. |
| 8 | Thread-safe `LogicalClock` | **PASS** | Atomic CAS lock-free `AtomicLong` update loop. |
| 9 | Post-Payment Port Release | **PASS** | Ports `P1`-`P4` released back to `AVAILABLE` state upon successful payment. |

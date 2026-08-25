# EV Charging Network Management System — Testing & Verification Guide

## Workspace Directory
All commands should be executed from the project root directory:
`c:\Users\asus\Desktop\College\Sem 5\DC\Java`

---

## 1. Compilation Guide

To compile all Java files into the `bin/` directory, open PowerShell and execute:

```powershell
javac -d bin EVClient.java MultithreadTest.java ChargingStation/*.java Reservation/*.java ChargingSession/*.java Payment/*.java Pricing/*.java
```

Verify that compilation completes cleanly with zero errors.

---

## 2. Server Startup Sequence (Mandatory Order)

Due to inter-server RMI dependencies, start the servers in separate PowerShell terminals in the exact order below:

### Terminal 1: ChargingStationServer (Port 1234)
```powershell
java -cp bin ChargingStationServer
```
*Expected Output*: `Charging Station Server is running... RMI Registry running on port 1234.`

### Terminal 2: ReservationServer (Port 1235)
```powershell
java -cp bin ReservationServer
```
*Expected Output*: `RESERVATION RMI SERVER... Connected to ChargingStationServer on port 1234.`

### Terminal 3: ChargingSessionServer (Port 1236)
```powershell
java -cp bin ChargingSessionServer
```
*Expected Output*: `Charging Session Server Ready.... Bound to registry on port 1236.`

### Terminal 4: PricingServer (Port 1238)
```powershell
java -cp bin PricingServer
```
*Expected Output*: `PRICING RMI SERVER... Port: 1238.`

### Terminal 5: PaymentServer (Port 1237)
```powershell
java -cp bin PaymentServer
```
*Expected Output*: `Payment Server Ready.... Bound to registry on port 1237.`

---

## 3. Client Execution

### Interactive Application Client (`EVClient`)
Open a 6th terminal:
```powershell
java -cp bin EVClient
```
- Enter User ID (e.g. `USER1`) and Vehicle ID (e.g. `EV1`).
- Select Option `4` to Reserve Slot → note Reservation ID (e.g. `RES1001`).
- Select Option `7` to Start Charging using `RES1001` → note Session ID (e.g. `SESSION-1001`).
- Select Option `9` to Stop Charging using `SESSION-1001` → note status `COMPLETED` and message that port will be released after payment.
- Select Option `2` (View Available Ports) → verify assigned port is **still locked (`CHARGING`)**.
- Select Option `11` to Make Payment using `SESSION-1001` → note Payment ID (e.g. `PAY-1001`) and port release message.
- Select Option `2` (View Available Ports) → verify port is **now AVAILABLE**.

### Concurrency Stress Test (`MultithreadTest`)
```powershell
java -cp bin MultithreadTest
```
- Simulates 10 concurrent EVs firing requests simultaneously across 10 client threads.
- Confirms double-booking prevention: ports `P1`–`P4` are assigned to 4 threads; remaining 6 threads receive port capacity limits and terminate safely.
- Observes thread IDs in server logs demonstrating concurrent RMI thread execution.

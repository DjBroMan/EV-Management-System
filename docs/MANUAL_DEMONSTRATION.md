# Manual Terminal Demonstration (Windows / PowerShell)

Step-by-step script for demonstrating every distributed-system concept in
this project to a professor, using exact PowerShell commands. Two ways to
run the system are covered: **Docker Compose** (recommended — 32 services,
exactly matches production wiring) and a **manual multi-JVM run** (useful if
Docker isn't available; this is exactly the setup that was live-tested
while building this feature).

All commands below assume the working directory is the repository root
(`C:\Users\aravk\Downloads\New folder (4)`, adjust to wherever you cloned it).

---

## 1. How to build

**Docker (builds automatically on `docker compose up`, or build explicitly):**
```powershell
docker compose build
```

**Manual (compile everything with javac):**
```powershell
if (-not (Test-Path lib)) { New-Item -ItemType Directory lib | Out-Null }
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" -OutFile "lib\mysql-connector-j-8.0.33.jar"
if (Test-Path bin) { Remove-Item -Recurse -Force bin }
New-Item -ItemType Directory bin | Out-Null
javac -cp "lib\mysql-connector-j-8.0.33.jar" -d bin `
  Clock\*.java Common\*.java `
  ChargingStation\*.java Reservation\*.java ChargingSession\*.java Pricing\*.java Payment\*.java `
  DBConnectionHelper.java EVClient.java MultithreadTest.java ReplicationTest.java tests\*.java
```

---

## 2. How to start Docker (full 32-service stack: 1 TimeServer + 15 MySQL + 15 app instances + 1 Manager)

```powershell
docker compose up -d
docker compose ps
```
Wait until every `mysql-*` container shows `healthy` (30-90s), then the app
containers will finish connecting.

---

## 3. How to start the Manager

**Docker:** already started as part of `docker compose up -d` (service `manager`, port 1240).

**Manual** (after the ChargingStation cluster — step 4 below — is running):
```powershell
$env:STATION_INSTANCES="1:localhost:1234,2:localhost:1244,3:localhost:1254"
$env:SESSION_INSTANCES="1:localhost:1236,2:localhost:1246,3:localhost:1256"
$env:PRICING_INSTANCES="1:localhost:1238,2:localhost:1248,3:localhost:1258"
$env:PAYMENT_INSTANCES="1:localhost:1237,2:localhost:1247,3:localhost:1257"
$env:RESERVATION_INSTANCES="3:localhost:1235,1:localhost:1245,2:localhost:1255"
$env:PRIMARY_HOST="localhost"; $env:SECONDARY_HOST="localhost"; $env:MANAGER_PORT="1240"
Start-Process java -ArgumentList "-cp","bin;lib\mysql-connector-j-8.0.33.jar","ReservationServerManager"
```

### Manual full cluster bring-up (all 17 processes, in dependency order)

```powershell
$CP = "bin;lib\mysql-connector-j-8.0.33.jar"

# 1. Time server
Start-Process java -ArgumentList "-cp",$CP,"Clock.TimeServer"
Start-Sleep -Seconds 2

# 2. ChargingStation cluster (CS1/CS2/CS3)
$env:SERVER_ID="1"; $env:RMI_REGISTRY_PORT="1234"; $env:RMI_EXPORT_PORT="2234"; $env:PEERS="2:localhost:1244,3:localhost:1254"
Start-Process java -ArgumentList "-cp",$CP,"ChargingStationServer"
$env:SERVER_ID="2"; $env:RMI_REGISTRY_PORT="1244"; $env:RMI_EXPORT_PORT="2244"; $env:PEERS="1:localhost:1234,3:localhost:1254"
Start-Process java -ArgumentList "-cp",$CP,"ChargingStationServer"
$env:SERVER_ID="3"; $env:RMI_REGISTRY_PORT="1254"; $env:RMI_EXPORT_PORT="2254"; $env:PEERS="1:localhost:1234,2:localhost:1244"
Start-Process java -ArgumentList "-cp",$CP,"ChargingStationServer"
Start-Sleep -Seconds 4

# 3. Manager (see block above) -- start it now
Start-Sleep -Seconds 4

# 4. Reservation cluster (R3=primary@1235 highest id, R1=secondary@1245, R2=secondary@1255)
$env:SERVER_ID="3"; $env:PEERS="1:localhost:1245,2:localhost:1255"; $env:MANAGER_HOST="localhost"; $env:MANAGER_PORT="1240"
Start-Process java -ArgumentList "-cp",$CP,"ReservationServer","primary","1235"
$env:SERVER_ID="1"; $env:PEERS="3:localhost:1235,2:localhost:1255"
Start-Process java -ArgumentList "-cp",$CP,"ReservationServer","secondary","1245"
$env:SERVER_ID="2"; $env:RMI_REGISTRY_PORT="1255"; $env:PEERS="3:localhost:1235,1:localhost:1245"
Start-Process java -ArgumentList "-cp",$CP,"ReservationServer","secondary","1255"
Start-Sleep -Seconds 5

# 5. ChargingSession cluster
$env:SERVER_ID="1"; $env:RMI_REGISTRY_PORT="1236"; $env:RMI_EXPORT_PORT="2236"; $env:PEERS="2:localhost:1246,3:localhost:1256"
Start-Process java -ArgumentList "-cp",$CP,"ChargingSessionServer"
$env:SERVER_ID="2"; $env:RMI_REGISTRY_PORT="1246"; $env:RMI_EXPORT_PORT="2246"; $env:PEERS="1:localhost:1236,3:localhost:1256"
Start-Process java -ArgumentList "-cp",$CP,"ChargingSessionServer"
$env:SERVER_ID="3"; $env:RMI_REGISTRY_PORT="1256"; $env:RMI_EXPORT_PORT="2256"; $env:PEERS="1:localhost:1236,2:localhost:1246"
Start-Process java -ArgumentList "-cp",$CP,"ChargingSessionServer"

# 6. Pricing cluster (no MANAGER_HOST needed -- no cross-service calls)
$env:SERVER_ID="1"; $env:RMI_REGISTRY_PORT="1238"; $env:RMI_EXPORT_PORT="2238"; $env:PEERS="2:localhost:1248,3:localhost:1258"
Start-Process java -ArgumentList "-cp",$CP,"PricingServer"
$env:SERVER_ID="2"; $env:RMI_REGISTRY_PORT="1248"; $env:RMI_EXPORT_PORT="2248"; $env:PEERS="1:localhost:1238,3:localhost:1258"
Start-Process java -ArgumentList "-cp",$CP,"PricingServer"
$env:SERVER_ID="3"; $env:RMI_REGISTRY_PORT="1258"; $env:RMI_EXPORT_PORT="2258"; $env:PEERS="1:localhost:1238,2:localhost:1248"
Start-Process java -ArgumentList "-cp",$CP,"PricingServer"
Start-Sleep -Seconds 5

# 7. Payment cluster
$env:SERVER_ID="1"; $env:RMI_REGISTRY_PORT="1237"; $env:RMI_EXPORT_PORT="2237"; $env:PEERS="2:localhost:1247,3:localhost:1257"
Start-Process java -ArgumentList "-cp",$CP,"PaymentServer"
$env:SERVER_ID="2"; $env:RMI_REGISTRY_PORT="1247"; $env:RMI_EXPORT_PORT="2247"; $env:PEERS="1:localhost:1237,3:localhost:1257"
Start-Process java -ArgumentList "-cp",$CP,"PaymentServer"
$env:SERVER_ID="3"; $env:RMI_REGISTRY_PORT="1257"; $env:RMI_EXPORT_PORT="2257"; $env:PEERS="1:localhost:1237,2:localhost:1247"
Start-Process java -ArgumentList "-cp",$CP,"PaymentServer"
```
(No `DB_HOST` env vars are set above, so the manual run operates in
in-memory-only mode; add `DB_HOST`/`DB_NAME`/etc. per instance, matching
`docker-compose.yml`, if you also want manual-mode persistence.)

---

## 4. How to verify all server instances

```powershell
Get-Process java | Select-Object Id, StartTime
# or, per Docker:
docker compose ps
```
Or run the topology test (works against either deployment):
```powershell
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" BullyElectionTest
```
Expected: every cluster reports exactly one `PRIMARY` and the rest
`SECONDARY`.

---

## 5. How to add required business data

Business data is **never pre-seeded by SQL** (`docs/DATABASE_SCHEMA.md`) —
`ChargingStationServer` self-seeds ports P1–P4 and `PricingServer`
self-seeds tariffs S01–S03 automatically on first boot of each instance.
No manual step is required; verify with:
```powershell
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" EVClient
# choose option 1 (View Station Status) -- should report 4 of 4 ports available
```

---

## 6. How to run EVClient

```powershell
$env:MANAGER_HOST="localhost"; $env:MANAGER_PORT="1240"
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" EVClient
```
Enter a User ID / Vehicle ID, then walk the menu: 1 (station status) → 4
(reserve) → 7 (start charging) → 9 (stop charging) → 10 (calculate bill) →
11 (make payment) → 1 again (port back to AVAILABLE).

---

## 7. How to run MultithreadTest

```powershell
$env:MANAGER_HOST="localhost"; $env:MANAGER_PORT="1240"
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" MultithreadTest
```
10 simulated EVs run the full workflow concurrently through the Manager;
watch for thread-safe, collision-free reservation/session/payment IDs.

---

## 8. How to demonstrate load balancing

```powershell
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" LoadBalancingTest
```
(Test classes under `tests/` have no `package` declaration, so they compile
into the same default package as everything else — just the plain class
name is needed.)
Then check the Manager's own console/log for `[LOAD BALANCER]` lines:
reads should rotate across `ChargingStationService-1/2/3`; writes should
all say `routing to leader ..., bypassing round-robin`.

---

## 9. How to demonstrate replication

```powershell
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" ReplicationTest
```
Runs all 8 original replication scenarios (discovery, single/multi
replication, cancellation, concurrent load, full sync, failover,
transparent post-failover routing) against the classic 2-node path. Watch
the Manager's console for `[REPLICATION] Update sent to ...` lines during
any ChargingStation/ChargingSession/Payment write for the generalized
N-instance fan-out.

---

## 10. How to kill a Primary

**Docker:**
```powershell
docker stop reservation-3    # kills the current Reservation leader (R3)
```
**Manual:**
```powershell
Get-CimInstance Win32_Process -Filter "Name='java.exe'" |
  Where-Object { $_.CommandLine -like "*ReservationServer primary*" } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
```
Any of the 5 clusters can be killed the same way — substitute the
container name (`charging-station-3`, `pricing-3`, `payment-3`,
`charging-session-3`, ...) or the matching `CommandLine` filter.

---

## 11. How to show Bully election

Watch the console output of a surviving instance (or `docker compose logs
-f reservation-1`) — within ~6-9 seconds of the kill you will see:
```
[BULLY] Heartbeat to coordinator (3) failed 1x
[BULLY] Heartbeat to coordinator (3) failed 2x
[BULLY] Coordinator 3 presumed DOWN after 2 missed heartbeats. Starting election.
[BULLY] Sending ELECTION to higher-ID peer 2
[BULLY] Received OK from peer 2
[BULLY] No COORDINATOR announcement arrived in time... (only if 2 was also down)
```
And on the winning node:
```
[BULLY] No higher-ID peer responded. Reservation-2 ELECTED as new coordinator.
[BULLY] This instance is now PRIMARY (coordinator) of the Reservation cluster.
[BULLY] Sent COORDINATOR announcement to Reservation-1
```

---

## 12. How to show the new Primary

```powershell
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" BullyElectionTest
```
Or check the Manager's own log/console for:
```
[MANAGER] New leader for Reservation cluster = Reservation-2
```

---

## 13. How to send another request after failure

```powershell
$env:MANAGER_HOST="localhost"; $env:MANAGER_PORT="1240"
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" EVClient
# option 4 (reserve) -- succeeds, transparently served by the new leader
```
The reservation counter continues from where it left off (e.g. `RES1002`
after `RES1001`), proving the new leader used its already-replicated state
rather than starting over.

---

## 14. How to inspect the database

**Docker** (each instance's MySQL is also published to the host):
```powershell
docker exec -it mysql-reservation-3 mysql -uroot -pevroot -e "SELECT * FROM ev_reservation_primary_db.reservations;"
docker exec -it mysql-station-1 mysql -uroot -pevroot -e "SELECT * FROM ev_station_db.charging_ports;"
docker exec -it mysql-payment-1 mysql -uroot -pevroot -e "SELECT * FROM ev_payment_db.payments;"
```
**Manual** (with a local MySQL client, using the published ports from
`docker-compose.yml`'s reservation containers, e.g. `3307`/`3308`/`3309`):
```powershell
mysql -h 127.0.0.1 -P 3307 -uroot -pevroot -e "SELECT * FROM ev_reservation_primary_db.reservations;"
```

---

## 15. How to demonstrate Lamport clocks

Every server log line already prints `[Lamport=N]`. To see it explicitly
increase across a causal chain, run `EVClient` and watch a single
`reserveSlot` call's log lines across Manager → Primary → ChargingStation →
back — the Lamport value strictly increases at every hop, and the response
carries the max-seen value back to the client, which folds it into its own
clock (`clientClock.receiveEvent(res.getTimestamp())` in `EVClient.java`).

```powershell
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" HealthCheckTest
```
prints each instance's current Lamport value alongside its physical time in
one consolidated report.

---

## 16. How to demonstrate Cristian physical clocks

Every server calls `synchronizeClock()` once at startup (visible in its
boot log as a full `CRISTIAN CLOCK SYNCHRONIZATION` report with T0, T1, RTT,
estimated server time, and computed offset) and Docker deliberately injects
different simulated clock skew per container via `libfaketime`
(`FAKETIME=@2026-08-26 15:3x:xx` — a different offset per instance in
`docker-compose.yml`), so the corrected offsets are visibly non-zero and
different per container. Trigger it again on demand:
```powershell
# any running instance also re-syncs whenever its synchronizeClock() RMI
# method is invoked directly, e.g. via a short one-off client, or simply
# restart the container/process to see the boot-time report again.
docker compose restart charging-station-2
docker compose logs charging-station-2 | Select-String "CRISTIAN"
```

---

## Combined demonstration (load balancing + failover + continued requests)

```powershell
$env:MANAGER_HOST="localhost"; $env:MANAGER_PORT="1240"
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" CombinedFailoverLoadTest
# while it's running (30s window), in another terminal:
docker stop reservation-3   # or the manual Stop-Process command from step 10
```
Expected result line: a small failure count clustered around the kill,
then 100% success for the rest of the run.

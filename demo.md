# EV Management System — Full Live Demo Script

> Every runnable command below is in a shaded code block so it's easy to spot while presenting — everything else is explanation. Copy only what's inside the boxes.

## What this project is

A distributed EV charging network built with Java RMI. It started as a single-instance-per-service system and has been extended into a fully distributed, fault-tolerant, load-balanced architecture:

- 5 independent services: ChargingStation, Reservation, ChargingSession, Pricing, Payment
- Each service runs as a **cluster of 3 instances** (not 1)
- Each cluster independently runs the **Bully algorithm** to elect its own leader — there is no single global election, each of the 5 clusters elects on its own
- State changes are **replicated** from the leader to the other instances in the same cluster (primary-backup replication)
- Every instance persists to its **own MySQL database** (15 databases total)
- A **Manager** process is the single entry point every client talks to. It load-balances read-only requests round-robin across healthy instances, and always routes write requests to whichever instance is currently the elected leader
- **Lamport logical clocks** and **Cristian's physical clock synchronization** run throughout, visible in every log line
- If a leader dies, the surviving instances detect it, hold a real election, and the Manager automatically starts routing to the new leader — already-replicated state is reused, nothing is rebuilt

This script walks through demonstrating every one of those pieces, live, with an explanation of what you're looking at and why it matters before each step.

## Prerequisites (already done if you just ran the two commands below)

```powershell
docker compose down -v
docker compose up -d --build
```

This tears down any previous run (including database contents, via `-v`) and boots a completely fresh, empty-schema deployment: 1 TimeServer, 1 Manager, 15 MySQL databases, and 15 clustered application instances (3 per service) = 32 containers total. Wait about 30–60 seconds after `up -d --build` finishes for MySQL health checks and the app containers' Bully elections to settle before starting the demo.

All commands below are PowerShell, run from the project root (the folder containing `docker-compose.yml`).

---

## Step 0 — Verify everything is up

```powershell
docker compose ps
```

What to point out:
- 32 containers listed, all "Up"
- The 15 `mysql-*` containers additionally show **"(healthy)"** — Docker's healthcheck (`mysqladmin ping`) confirms MySQL is actually accepting connections, not just that the process started
- Every other service is grouped in 3s: `charging-station-1/2/3`, `reservation-1/2/3`, `charging-session-1/2/3`, `pricing-1/2/3`, `payment-1/2/3` — this is the "3 instances per cluster" architecture

---

## Step 1 — Show the Bully election that already happened on boot

**Explanation:** when 3 instances of a service boot up, none of them initially knows who the leader is. Each instance has a unique `SERVER_ID` (1, 2, or 3). The Bully algorithm's rule is simple: the highest surviving ID always wins. Within a few seconds of boot, each cluster elects instance #3 as its leader, because #3 is the highest ID and all 3 instances started healthy.

```powershell
docker compose logs manager | Select-String "New leader for"
```

Expected output (order may vary slightly):
```
New leader for ChargingStation cluster = ChargingStationService-3
New leader for Reservation cluster = Reservation-3
New leader for ChargingSession cluster = ChargingSessionService-3
New leader for Pricing cluster = PricingService-3
New leader for Payment cluster = PaymentService-3
```

**What to say:** "This is the Manager independently confirming, for each of the 5 clusters, which instance is currently the leader. It found this out by directly asking each instance 'what is your role?' over RMI — it's not guessing."

To see the actual election messages exchanged between instances (not just the Manager's summary):

```powershell
docker compose logs charging-station-3 | Select-String "BULLY|coordinator|election"
```

You'll see lines like:
```
Starting election. My ID=3
No higher-ID peer responded. ChargingStationService-3 ELECTED as new coordinator.
This instance is now PRIMARY (coordinator) of the ChargingStation cluster.
Sent COORDINATOR announcement to ChargingStationService-1
Sent COORDINATOR announcement to ChargingStationService-2
```

And on a follower:

```powershell
docker compose logs charging-station-1 | Select-String "coordinator"
```
```
ChargingStationService-3 announced itself as the new COORDINATOR.
Learned new coordinator: ChargingStation-3
```

---

## Step 2 — Build the client (one-time, if not already built)

**Explanation:** EVClient and the small demo helper programs are plain Java RMI clients. They only need the compiled classes and the MySQL JDBC driver on the classpath — they don't run inside Docker themselves, they connect to the Manager's published port (1240) from your host machine.

```powershell
if (-not (Test-Path lib)) { New-Item -ItemType Directory lib | Out-Null }
if (-not (Test-Path "lib\mysql-connector-j-8.0.33.jar")) {
    Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" -OutFile "lib\mysql-connector-j-8.0.33.jar"
}
if (Test-Path bin) { Remove-Item -Recurse -Force bin }
New-Item -ItemType Directory bin | Out-Null
javac -cp "lib\mysql-connector-j-8.0.33.jar" -d bin `
  Clock\*.java Common\*.java `
  ChargingStation\*.java Reservation\*.java ChargingSession\*.java Pricing\*.java Payment\*.java `
  DBConnectionHelper.java EVClient.java MultithreadTest.java ReplicationTest.java WalletDemo.java tests\*.java

$env:MANAGER_HOST="localhost"
$env:MANAGER_PORT="1240"
```

Leave those two `$env:` variables set for the rest of this session — every command below relies on them.

---

## Step 3 — The Manager as the single entry point

**Explanation:** EVClient never talks to ChargingStation, Reservation, ChargingSession, Pricing, or Payment directly. It only ever looks up services on the Manager (port 1240). The Manager decides, behind the scenes, which of the 3 instances of each service actually handles the call.

```powershell
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" EVClient
```

When prompted:
```
Enter User ID: demo-user
Enter Vehicle ID: demo-car
```

You'll see a numbered menu. Keep this running — you'll use it throughout the demo below. (Menu options: 1=station status, 2=available ports, 3=check port, 4=reserve, 5=check reservation, 6=cancel, 7=start charging, 8=check session, 9=stop charging, 10=calculate bill, 11=make payment, 12=check payment status, 13=payment details, 14=exit)

---

## Step 4 — Complete end-to-end EV workflow

**Explanation:** this is the full business flow the whole system exists to support: Reservation → physical port allocation → start charging → stop charging → energy calculation → dynamic pricing → payment → port released back to AVAILABLE.

In the EVClient menu, in order:

```
1   -> View Station Status         (should show 4 of 4 ports available on a fresh boot)
4   -> Reserve Charging Slot       (note the Reservation ID it prints, e.g. RES1001)
7   -> Start Charging              (press Enter to accept the reservation ID default; note the Session ID, e.g. SESSION-1001)
9   -> Stop Charging               (press Enter to accept the session ID default)
10  -> Calculate Bill              (shows energy consumed x dynamic price)
11  -> Make Payment                (press Enter to accept the session ID default)
1   -> View Station Status again   (port should be back to AVAILABLE)
```

**What to say:** "Every one of those calls just went through the Manager, which forwarded reserve/start/stop/pay to whichever instance is currently the leader of that service's cluster, and the read-only 'view status' calls were load-balanced round-robin across whichever instances are healthy."

---

## Step 5 — Verify database persistence (in a new terminal, leave EVClient running)

**Explanation:** the reservation/session/payment you just created were written to MySQL by the instance that processed each step. Each of the 15 instances has its own database.

```powershell
docker exec mysql-reservation-3 mysql -uroot -pevroot -e "SELECT * FROM ev_reservation_primary_db.reservations;"
docker exec mysql-session-1 mysql -uroot -pevroot -e "SELECT * FROM ev_session_db.charging_sessions;"
docker exec mysql-payment-1 mysql -uroot -pevroot -e "SELECT * FROM ev_payment_db.payments;"
docker exec mysql-station-1 mysql -uroot -pevroot -e "SELECT * FROM ev_station_db.charging_ports;"
```

What to point out: the reservation row, the session row (with `start_time`, `end_time`, `energy_consumed_kwh` all filled in), the payment row (with the computed `total_amount`), and the port back to `AVAILABLE` — each written using `PhysicalClock` (Cristian-corrected time), not MySQL's own clock.

---

## Step 6 — Verify replication across all 3 instances of a cluster

**Explanation:** the write above only happened on ONE instance (the leader, instance 3). It should have been replicated to the other 2 instances' databases too — this is the "no single point of failure" property.

```powershell
docker exec mysql-reservation-1 mysql -uroot -pevroot -e "SELECT * FROM ev_reservation_secondary_db.reservations;"
docker exec mysql-reservation-2 mysql -uroot -pevroot -e "SELECT * FROM ev_reservation_tertiary_db.reservations;"
```

**What to say:** "Same reservation row, in 3 separate MySQL databases, on 3 separate instances, even though the client only ever talked to one of them through the Manager." You can also show the replication happening live:

```powershell
docker compose logs manager | Select-String "REPLICATION"
```

---

## Step 7 — Load balancing (read-only calls spread across instances)

**Explanation:** load balancing is different from leader election. Reads that don't change state (station status, pricing lookups) can safely be answered by ANY healthy instance, since all 3 hold replicated state. The Manager uses simple round-robin for these. Writes NEVER get round-robined — they always go to the current leader.

In the EVClient menu, press option `1` (View Station Status) about 6 times in a row, quickly. Then:

```powershell
docker compose logs manager | Select-String "Selected ChargingStationService"
```

Expected: cycling output —
```
Selected ChargingStationService-1 (read, round-robin)
Selected ChargingStationService-2 (read, round-robin)
Selected ChargingStationService-3 (read, round-robin)
Selected ChargingStationService-1 (read, round-robin)
...
```

Compare against a write, e.g. after pressing option `4` (Reserve) once:

```powershell
docker compose logs manager | Select-String "routing to leader"
```
```
Write operation -> routing to leader ChargingStationService-3, bypassing round-robin
```

**What to say:** "Reads rotate across all 3. Writes always go to whichever one is currently the leader, no matter how many round-robin reads happened in between."

---

## Step 8 — Real leader failure + Bully re-election (the centerpiece)

**Explanation:** this is the most important distributed-systems concept to show. We are going to kill the actual container that is currently the Reservation leader (instance 3) and watch the surviving instances detect it, hold a real election, and pick a new leader — with the Manager automatically re-routing to it.

**8a — confirm the current leader and pre-failure state:**

```powershell
docker compose logs manager | Select-String "New leader for Reservation" | Select-Object -Last 1
docker exec mysql-reservation-3 mysql -uroot -pevroot -e "SELECT * FROM ev_reservation_primary_db.reservations;"
```

**8b — kill it:**

```powershell
docker stop reservation-3
```

**8c — wait about 10–15 seconds, then watch the election happen:**

```powershell
docker compose logs reservation-1 reservation-2 | Select-String "heartbeat|election|coordinator"
```

Expected output, in this order (approximately):
```
Heartbeat to coordinator (3) failed 1x
Heartbeat to coordinator (3) failed 2x
Coordinator 3 presumed DOWN after 2 missed heartbeats. Starting election.
Starting election. My ID=1
Sending ELECTION to higher-ID peer 2
Received OK from a higher-ID peer. Waiting for its COORDINATOR announcement...
No higher-ID peer responded. Reservation-2 ELECTED as new coordinator.
This instance is now PRIMARY (coordinator) of the Reservation cluster.
Sent COORDINATOR announcement to Reservation-1
Reservation-2 announced itself as the new COORDINATOR.
Learned new coordinator: Reservation-2
```

**What to say while pointing at this:** "Instance 1 noticed the leader (3) stopped responding to heartbeats. It started a Bully election by sending an ELECTION message to every instance with a HIGHER id than itself — that means it only had to ask instance 2. Instance 2 didn't get any response from a higher ID than ITSELF either (3 is dead), so instance 2 declared itself the new coordinator and told everyone."

**8d — confirm the Manager picked this up:**

```powershell
docker compose logs manager | Select-String "New leader for Reservation"
```

Expected: now shows BOTH lines —
```
New leader for Reservation cluster = Reservation-3
New leader for Reservation cluster = Reservation-2
```

**8e — send a brand new request through EVClient (it's still running!) and prove it succeeds via the NEW leader:**

In the EVClient window: press `4` (Reserve Charging Slot) again.

Expected: it succeeds, and the new Reservation ID continues the sequence (e.g. `RES1002`, not a reset back to `RES1001`) — proving the new leader picked up exactly where the old one left off, using its already-replicated state, with no data loss and no rebuild.

**8f — verify the new reservation replicated too:**

```powershell
docker exec mysql-reservation-1 mysql -uroot -pevroot -e "SELECT * FROM ev_reservation_secondary_db.reservations;"
docker exec mysql-reservation-2 mysql -uroot -pevroot -e "SELECT * FROM ev_reservation_tertiary_db.reservations;"
```

Both should now show BOTH reservations — proving the surviving cluster kept replicating correctly even with one node down.

**8g — bring the old leader back (optional, shows clean rejoin):**

```powershell
docker start reservation-3
```

Wait ~10 seconds, then:

```powershell
docker compose logs reservation-3 | Select-String "coordinator"
```

What you'll see: instance 3 comes back, and since 3 is still the highest ID, the Bully algorithm gives it leadership back immediately — this is standard Bully behavior (highest ID always wins, including on rejoin). Instance 2 gracefully steps down the moment it hears about it, with no lost data.

---

## Step 9 — Repeat the failover for a different cluster (optional, shows it's generic)

```powershell
docker stop charging-station-3
# wait 10-15s
docker compose logs charging-station-1 charging-station-2 | Select-String "coordinator"
docker compose logs manager | Select-String "New leader for ChargingStation"
docker start charging-station-3
```

**What to say:** "Every one of the 5 clusters runs this exact same independent election — killing one cluster's leader has zero effect on any of the other 4 clusters."

---

## Step 10 — Multithreading / concurrency

**Explanation:** multiple simulated EVs hit the system concurrently through the same Manager, proving there's no race condition or ID collision even under concurrent load.

```powershell
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" MultithreadTest
```

What to point out: 10 simulated EV clients run reserve → start → stop → pay concurrently; watch the interleaved Lamport-timestamped log output and confirm every one gets a unique Reservation/Session/Payment ID with no duplicates or corrupted state.

---

## Step 11 — Lamport logical clocks

**Explanation:** every RMI call carries a Lamport timestamp. Every log line already shows it as `[Lamport=N]`. The rule: on receiving a message, a server sets its clock to `max(its own clock, the received timestamp) + 1` — this creates a consistent causal ordering across independent processes that don't share a physical clock.

This step traces a *single* request's causal chain, so grep across the services it actually touches rather than one container alone — a single-container view will otherwise mix in Step 10's 10 concurrent threads and look jumbled:

```powershell
docker compose logs manager reservation-1 reservation-2 reservation-3 charging-station-1 charging-station-2 charging-station-3 | Select-String "Lamport" | Select-Object -First 20
```

**What to say:** "Watch the Lamport number strictly increase across a single request as it hops from the Manager to the Reservation leader to the ChargingStation cluster and back — that's the logical clock advancing on every send/receive, independent of wall-clock time."

---

## Step 12 — Cristian physical clock synchronization

**Explanation:** each container's system clock is deliberately skewed on boot (via `libfaketime`, different `FAKETIME` per container in `docker-compose.yml`) to simulate real clock drift across machines. Every server synchronizes against the TimeServer using Cristian's algorithm at startup: it measures round-trip time, estimates the TimeServer's real time, and computes an offset correction.

```powershell
docker compose logs charging-station-2 | Select-String "CRISTIAN" -Context 0,15
```

What to point out: the printed report shows T0 (local time before), the TimeServer's reported time, the round-trip time, and the final computed offset in milliseconds — this offset is what `PhysicalClock` adds to every timestamp written to the database.

To trigger it again on demand and see a fresh report:

```powershell
docker compose restart charging-station-2
docker compose logs charging-station-2 | Select-String "CRISTIAN" -Context 0,15
```

---

## Step 13 — Wallet balance + cross-server Lamport clocks (ChargingSession <-> Payment)

**Explanation:** every earlier Lamport example (Step 11) shows one server's clock advancing across its own log. This step demonstrates the same rule holding true across TWO INDEPENDENT CONTAINERS: `ChargingSessionServer` runs a background billing cycle (every 5s, no client involved) that calls `PaymentServer.checkAndDeductBalance` over RMI to bill the active session's owner for energy consumed since the last checkpoint. If the wallet balance runs out, ChargingSession automatically stops the session. The Lamport timestamp travels WITH the RMI call and response, so `payment-N`'s clock jumps to `max(its own, ChargingSession's sent value) + 1` on receipt — a live, repeatable, cross-container causal chain.

Run the standalone demo (drives: top-up -> reserve -> start charging -> wait for auto-stop):

```powershell
java -cp "bin;lib\mysql-connector-j-8.0.33.jar" WalletDemo
```

While it runs, in a SEPARATE terminal, watch the two containers' logs live:

```powershell
docker compose logs charging-session-1 payment-1 -f
```

Or, after it finishes, pull the causal sequence out with:

```powershell
docker compose logs charging-session-1 payment-1 | Select-String "Lamport" | Select-Object -First 40
docker compose logs charging-session-1 payment-1 | Select-String "WALLET|BILLING|INSUFFICIENT"
```

Expected pattern, repeating every ~5 seconds: `charging-session-1` logs `[Event=SEND]` with a Lamport value, `payment-1` logs `[Event=RECEIVE]` with a HIGHER Lamport value (the receive rule `max(local, received) + 1`), then `payment-1` logs `[Event=SEND]` with its own new value, then `charging-session-1` logs `[Event=RECEIVE]` with an even higher value — strictly increasing across the two containers every cycle, ending in `[Event=INSUFFICIENT_BALANCE]` once the small (Rs. 0.25) top-up is exhausted.

**What to say:** "This is the only interaction in the whole system where a server calls another server on its own initiative, not because a client asked — so the Lamport handshake you're watching is happening completely independent of me clicking anything. And notice the actual charging duration and cost are still computed from `PhysicalClock`'s Cristian-synchronized time, not from these Lamport numbers — Lamport is strictly for proving the causal order of the SEND/RECEIVE pair, never for the money or time math."

Replication check — confirm the wallet balance replicated identically to all 3 Payment instances:

```powershell
docker exec mysql-payment-1 mysql -uroot -pevroot -e "SELECT * FROM ev_payment_db.wallets;"
docker exec mysql-payment-2 mysql -uroot -pevroot -e "SELECT * FROM ev_payment_db_2.wallets;"
docker exec mysql-payment-3 mysql -uroot -pevroot -e "SELECT * FROM ev_payment_db_3.wallets;"
```

See [docs/WALLET.md](docs/WALLET.md) for the full write-up of why this demonstrates cross-server Lamport clocks, how it differs from physical time, and how replication/failover affects wallet state.

---

## Cleanup (after the demo)

```powershell
docker compose down          # stops everything, keeps DB data
docker compose down -v       # also wipes DB data (fresh empty run next time)
```

---

## Quick reference — what proves what

| Concept | Where to look |
|---|---|
| Multiple instances per service | `docker compose ps` (3× per cluster) |
| Bully election | Step 1 and Step 8b/8c |
| Replication | Step 6 |
| Load balancing (reads) | Step 7 |
| Leader-only routing (writes) | Step 7 (second half) |
| Real failover + continued service | Step 8 (the centerpiece) |
| Database persistence | Step 5 |
| Complete EV workflow | Step 4 |
| Multithreading / concurrency safety | Step 10 |
| Lamport logical clocks | Step 11 |
| Cristian physical clock sync | Step 12 |
| Cross-server Lamport clocks (2 different containers) | Step 13 |

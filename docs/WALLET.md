# Wallet Feature — Cross-Server Lamport Clock Demonstration

## Overview

The Wallet feature adds real, DB-persisted, replication-compatible balance tracking to `PaymentServer`, and a background billing loop to `ChargingSessionServer` that periodically checks/deducts a session's owner's wallet balance while charging is in progress. Functionally it's a small addition to the Payment cluster. Architecturally, it exists to make one specific property of this system directly observable: **Lamport logical clocks propagating causally across two independent server containers**, on a schedule that no client controls.

Every other cross-service call in this system (see [RMI_COMMUNICATION.md](RMI_COMMUNICATION.md)) is triggered by an incoming client request — a human or `EVClient` presses a button, and the resulting SEND/RECEIVE chain follows from that one action. The wallet billing cycle is different: `ChargingSessionServer` runs a `ScheduledExecutorService` that fires every 5 seconds on its own, with no client involved, and on each tick calls `PaymentServer.checkAndDeductBalance(...)` over RMI. That call physically leaves the `charging-session-N` container's JVM and enters the currently Bully-elected `payment-N` container's JVM. This is the cleanest, most repeatable demonstration available in this codebase of two genuinely separate distributed processes exchanging Lamport timestamps with no synchronized physical clock or human interaction involved.

## Why this demonstrates cross-server Lamport clocks

Lamport's algorithm exists to give events in a distributed system with no shared clock a consistent partial ordering, based purely on causality: if event A could have influenced event B (a message from A arrives before B happens), then A's Lamport timestamp must be less than B's. The billing cycle produces a clean, repeatable instance of this:

1. `ChargingSessionServer` (on `charging-session-N`) calls `logicalClock.sendEvent()`, gets back its next value, and passes it as the `clientLamport` argument to `PaymentServer.checkAndDeductBalance(userId, amount, sessionId, clientLamport)`.
2. `PaymentServer` (on `payment-N`, a different container/JVM) receives the call and does `logicalClock.receiveEvent(clientLamport)`, which sets its own clock to `max(its own value, clientLamport) + 1` — guaranteeing its clock is now strictly ahead of the sender's at send-time, regardless of either container's wall-clock skew.
3. `PaymentServer` does its own work (checking/deducting the balance, persisting, replicating), then calls `sendEvent()` again before returning the `LamportResult<String>` response.
4. `ChargingSessionServer` receives the response and does `logicalClock.receiveEvent(response.getTimestamp())`, advancing its own clock past Payment's.

Because this repeats every `BILLING_INTERVAL_SECONDS` (5s) for as long as a session is charging, the two containers' Lamport clocks visibly leapfrog each other in their independent `docker compose logs` output — proof that the ordering guarantee holds across process/container boundaries, not just within one server's own log.

## Why Lamport time is different from physical time

Lamport timestamps only prove **relative causal ordering** — "this event happened after that one, causally." They carry no information about wall-clock duration and are never comparable to real elapsed time; two causally-unrelated events on different servers can have wildly different Lamport values with no meaningful relationship to when they actually occurred.

Because of this, Lamport values are never used for any calculation that needs to be numerically meaningful — energy consumed, charging duration, or cost. Those all use `PhysicalClock.getSynchronizedPhysicalTimeMillis()`, which is Cristian-corrected against the shared `TimeServer` (see [MULTITHREADING.md](MULTITHREADING.md)) and does represent real elapsed wall-clock time. Concretely in this feature:

- The billing cycle's "energy consumed since last checkpoint" is computed from `Instant`/`Duration` (wall-clock time), the same formula already used by `getSessionStatus`/`stopCharging` — never from the Lamport clock.
- `wallets.last_updated` is written using `PhysicalClock.getSynchronizedPhysicalTimeMillis()`, exactly like `payments.payment_time`.
- The Lamport value that travels alongside each `checkAndDeductBalance` call exists purely so both servers' logical clocks stay causally consistent — deleting it wouldn't change the money or time math by a single cent or millisecond, only the demonstrable ordering guarantee.

## How the wallet/charging interaction crosses server boundaries

`ChargingSessionServer` never talks to a `payment-N` container directly. It resolves the target through `Common.ManagerRouting.resolvePaymentUrl()`, which — like every other inter-service call in this system — defaults to the Manager's proxy bound name (`rmi://manager:1240/PaymentService`) unless an explicit override env var is set. The "Manager" (`Reservation/ReservationServerManager.java`) is itself the object bound at that name; it implements `PaymentInterface` and forwards every call to whichever `payment-N` instance currently holds Bully leadership (`pickForWrite("Payment", ...)`), the same routing every write in this system already goes through.

So the physical path for one billing-cycle call is: `charging-session-N` JVM → RMI → `manager` JVM (routing decision only, no business logic) → RMI → `payment-N` JVM (the current leader) → response retraces the same path. Two, sometimes three, separate JVMs/containers are involved in a single round trip, and the Lamport handshake described above happens directly between `charging-session-N` and `payment-N`'s own clocks — the Manager's proxy call itself also ticks its own clock via the same `sendEvent`/`receiveEvent` pattern, but does not alter the causal relationship between the two endpoint clocks.

## How event timestamps propagate between servers

Every wallet RMI method follows the exact dual-overload convention already used everywhere in this codebase (documented in [RMI_COMMUNICATION.md](RMI_COMMUNICATION.md)): a legacy `Foo(args)` overload for convenience, and a `Foo(args, long clientLamport) -> LamportResult<T>` overload that actually carries the timestamp. For one `checkAndDeductBalance` round trip:

```
ChargingSessionServer.billSession():
    sendL = logicalClock.sendEvent()
    result = payment.checkAndDeductBalance(userId, cost, sessionId, sendL)   // RMI call, crosses container boundary
    logicalClock.receiveEvent(result.getTimestamp())

PaymentServer.checkAndDeductBalance(userId, amount, sessionId, clientLamport):
    recvL = logicalClock.receiveEvent(clientLamport)   // clock now > both previous values
    ... check/deduct balance, persist, replicate ...
    respL = logicalClock.sendEvent()
    return new LamportResult<>(result, respL)
```

Every step logs through the shared `DistributedLogger.log(serverName, clock, eventType, message)` helper, producing the standard `[Physical=...] [Lamport=N] [Server=...] [Thread=...] [Event=...]` block on each side — so the causal chain is visible without any special tooling, just `docker compose logs charging-session-1 payment-1`.

## How replication/failover affects wallet state

Wallet balance changes replicate through the exact same mechanism as every other write in this system — `Common.StateDelta` + `Common.ClusterManagerClient.replicate(...)` + `ClusterNodeInterface.applyUpdate(...)` (see [REPLICATION.md](REPLICATION.md)):

1. The Payment leader applies the change to its own in-memory map and its own MySQL database (`persistWallet`), then calls `managerClient.replicate("PaymentService", serverId, new StateDelta("WALLET_CREDIT"/"WALLET_DEBIT", ...), logicalClock)`.
2. The Manager fans that `StateDelta` out to the other 2 Payment instances, each of which applies it independently to their own in-memory map and their own database via `applyUpdate()`.
3. This is best-effort and asynchronous relative to the client response — the same guarantee (and the same limitation) that every other replicated write in this system already has. A crash between the local write and the replication fan-out is a pre-existing, system-wide characteristic, not something specific to wallets.

**Failover during an active billing cycle**: if the Payment leader dies mid-cycle, `ChargingSessionServer`'s `checkAndDeductBalance` call throws a `RemoteException`. The billing cycle logs a warning and **skips that cycle** — it does not treat a network failure as insufficient balance, and it does not stop the session. Once Bully election completes on the Payment cluster and a new leader is promoted (`promoteToPrimary`), the next billing cycle (5 seconds later) succeeds against the new leader, using the wallet balance that was already replicated to it. This mirrors the failover behavior demonstrated in [demo.md Step 8](../demo.md) for the Reservation cluster — the wallet feature gets that same real, testable resilience for free by building on the existing generalized cluster infrastructure rather than anything bespoke.

# Load Balancing

Implemented entirely inside the Manager (`Reservation/ReservationServerManager.java`,
the "GENERALIZED MULTI-CLUSTER ROUTING" section). EVClient and every other
inter-service caller never talk to a specific ChargingStation/ChargingSession/
Pricing/Payment/Reservation instance directly — they always go through the
Manager, which decides which instance actually serves the call.

## Clear separation of concerns (as required)

| Concept | Who decides | Mechanism |
|---|---|---|
| **Load balancing** | Manager | round-robin over the cluster's currently-healthy instances (`pickForRead`) |
| **Bully election** | Each cluster, independently | `Common/BullyElection.java` — see `docs/BULLY_ALGORITHM.md` |
| **Replication** | Each PRIMARY + Manager fan-out | `Common/ClusterManagerInterface.replicateUpdate` — see `docs/REPLICATION.md` |
| **Failover** | Bully (role change) + Manager (leader re-discovery) | see `docs/FAILOVER.md` |

Load balancing never decides *who is leader* — it only distributes
*read* traffic among instances that are already known to be healthy.

## Strategy: round-robin for reads, leader-only for writes

```java
private PeerHandle pickForRead(String cluster) {
    // cycles an AtomicInteger cursor over the currently-healthy instance list
}
private PeerHandle pickForWrite(String cluster) {
    // always returns cfg.currentLeader (the Bully-elected / discovered PRIMARY)
}
```

Read-only, replica-safe operations routed via `pickForRead` (round-robin):
`getStationStatus`, `getAvailablePorts`, `checkPortAvailability`,
`getSessionStatus`, `getEnergyConsumed`, `getSessionPort`, `calculatePrice`,
`getDemandMultiplier`, `getPaymentStatus`, `getPaymentDetails`,
`getReservation`, `getReservationPort` (when `RESERVATION_INSTANCES` cluster
mode is enabled).

Leader-only, state-changing operations routed via `pickForWrite`:
`reservePort`, `releasePort`, `reserveAnyAvailablePort`, `startPortCharging`,
`startCharging`, `stopCharging`, `makePayment`, `reserveSlot`,
`cancelReservation`. **These never get round-robined** — this is the
explicit "do not blindly load-balance operations that must go to the
current Primary/Leader" requirement.

Pricing is a special case: since no client call ever mutates pricing state,
*every* Pricing operation (even `calculatePrice`) is safely load-balanced —
there is no leader-owned Pricing write path to protect.

## Health awareness

`ClusterConfig.healthy` (a `Map<Integer,Boolean>`) is updated by the same
background health-monitor tick that discovers the current leader (see
`docs/FAILOVER.md`). `pickForRead` only cycles over instances currently
marked healthy, falling back to the full instance list only if the health
map is (transiently) empty.

## Verified behavior

Live-tested in this session (see the Final Report): 6 consecutive
`getStationStatus()` calls through the Manager rotated
`ChargingStationService-1` → `ChargingStationService-2` → ... while 6
consecutive `reserveAnyAvailablePort()` writes in the same run were **all**
routed to the single current leader instance, confirmed via the Manager's
own `[LOAD BALANCER]` log lines. `tests/LoadBalancingTest.java` reproduces
this exact scenario on demand.

# Project Structure Reference

> **Update:** this tree predates the distributed-system extension (Bully
> election, N-instance clusters, the generalized Manager, `Common/`,
> `tests/`, and `db/init/07`-`15`). It is kept here as a historical
> reference to the original single-instance-per-service layout; for the
> current architecture see `docs/DISTRIBUTED_SYSTEM_ROADMAP.md`,
> `docs/BULLY_ALGORITHM.md`, `docs/REPLICATION.md`, `docs/FAILOVER.md`,
> `docs/LOAD_BALANCING.md`, and `docs/DATABASE_SCHEMA.md`. New top-level
> additions since this was written: `Common/` (shared Bully/replication/
> routing infrastructure), `tests/` (BullyElectionTest, LoadBalancingTest,
> HealthCheckTest, CombinedFailoverLoadTest), and `db/init/07`-`15` (per
> new-instance database schemas).

```
Java/
│   .gitignore
│   all_java_codes.txt
│   docker-compose.yml
│   Dockerfile
│   EVClient.java
│   get_codes.py
│   MultithreadTest.java
│   README.md
│
├── bin/
│
├── Clock/
│   ├── CristianClient.java
│   ├── DistributedLogger.java
│   ├── LogicalClock.java
│   ├── PhysicalClock.java
│   ├── TimeServer.java
│   └── TimeServerInterface.java
│
├── ChargingSession/
│   ├── ChargingSessionInterface.java
│   └── ChargingSessionServer.java
│
├── ChargingStation/
│   ├── ChargingStationInterface.java
│   └── ChargingStationServer.java
│
├── Payment/
│   ├── PaymentInterface.java
│   └── PaymentServer.java
│
├── Pricing/
│   ├── PricingInterface.java
│   └── PricingServer.java
│
├── Reservation/
│   ├── ReservationInterface.java
│   └── ReservationServer.java
│
├── docs/
│   ├── ARCHITECTURE.md
│   ├── AUDIT.md
│   ├── DOCKER.md
│   ├── MODULES.md
│   ├── MULTITHREADING.md
│   ├── PROJECT_STRUCTURE.md
│   ├── RMI_COMMUNICATION.md
│   ├── STATE_MANAGEMENT.md
│   ├── TESTING.md
│   └── WORKFLOW.md
│
└── log/
    ├── clock_implementation.log
    ├── docker_implementation.log
    └── encoding_fix.log
```

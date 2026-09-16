# Database Schema — Source of Truth

This document is the approved database specification referenced throughout
`docs/DISTRIBUTED_SYSTEM_ROADMAP.md` and the other distributed-system docs.
It is generated directly from the SQL files in `db/init/` — those files are
the actual source of truth; this document explains and indexes them.

MySQL 8.0. Every table uses `InnoDB` / `utf8mb4`. **No cross-database SQL
foreign keys exist anywhere** — `reservation_id`, `session_id`, `port_id`,
and `payment_id` are logical, application-level references resolved only
through RMI calls between services, never through direct cross-database
queries. Every database starts **empty** (schema/indexes only) — the only
records ever present at container-start time are the ones the running Java
servers self-seed on first boot (ChargingStation seeds P1–P4, Pricing seeds
S01–S03 demand levels) — the SQL init scripts themselves never `INSERT`
business data.

## Per-instance database mapping (3 instances per cluster)

| Cluster | Instance | Server ID | RMI Port | Database | Init script |
|---|---|---|---|---|---|
| ChargingStation | CS1 | 1 | 1234 | `ev_station_db` | `01_ev_station_db.sql` |
| ChargingStation | CS2 | 2 | 1244 | `ev_station_db_2` | `07_ev_station_db_2.sql` |
| ChargingStation | CS3 | 3 | 1254 | `ev_station_db_3` | `08_ev_station_db_3.sql` |
| Reservation | R1 (secondary) | 1 | 1245 | `ev_reservation_secondary_db` | `03_ev_reservation_secondary_db.sql` |
| Reservation | R2 (tertiary) | 2 | 1255 | `ev_reservation_tertiary_db` | `09_ev_reservation_tertiary_db.sql` |
| Reservation | R3 (initial primary) | 3 | 1235 | `ev_reservation_primary_db` | `02_ev_reservation_primary_db.sql` |
| ChargingSession | S1 | 1 | 1236 | `ev_session_db` | `04_ev_session_db.sql` |
| ChargingSession | S2 | 2 | 1246 | `ev_session_db_2` | `10_ev_session_db_2.sql` |
| ChargingSession | S3 | 3 | 1256 | `ev_session_db_3` | `11_ev_session_db_3.sql` |
| Pricing | P1 | 1 | 1238 | `ev_pricing_db` | `05_ev_pricing_db.sql` |
| Pricing | P2 | 2 | 1248 | `ev_pricing_db_2` | `12_ev_pricing_db_2.sql` |
| Pricing | P3 | 3 | 1258 | `ev_pricing_db_3` | `13_ev_pricing_db_3.sql` |
| Payment | Pay1 | 1 | 1237 | `ev_payment_db` | `06_ev_payment_db.sql` |
| Payment | Pay2 | 2 | 1247 | `ev_payment_db_2` | `14_ev_payment_db_2.sql` |
| Payment | Pay3 | 3 | 1257 | `ev_payment_db_3` | `15_ev_payment_db_3.sql` |

All 3 instances of any one cluster share an **identical schema** — only the
database name differs. Each instance connects to only its own MySQL
container (`DB_HOST` env var), never another instance's database.

## Table schemas

### `charging_ports` (ChargingStation cluster, all 3 instances)
```sql
CREATE TABLE charging_ports (
    port_id       VARCHAR(16)  NOT NULL,
    station_id    VARCHAR(32)  NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'AVAILABLE',
    last_updated  TIMESTAMP    NOT NULL,
    PRIMARY KEY (port_id),
    INDEX idx_station_id (station_id),
    INDEX idx_status     (status)
);
```
Timestamps are written by `PhysicalClock.getSynchronizedPhysicalTimeMillis()`
(Cristian-corrected), never MySQL `CURRENT_TIMESTAMP`.

### `reservations` (Reservation cluster, all 3 instances)
```sql
CREATE TABLE reservations (
    reservation_id  VARCHAR(32)  NOT NULL,
    user_id         VARCHAR(64)  NOT NULL,
    vehicle_id      VARCHAR(64)  NOT NULL,
    port_id         VARCHAR(16)  NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'CONFIRMED',
    created_at      TIMESTAMP    NOT NULL,
    PRIMARY KEY (reservation_id),
    INDEX idx_user_id (user_id),
    INDEX idx_port_id (port_id),
    INDEX idx_status  (status)
);
```

### `charging_sessions` (ChargingSession cluster, all 3 instances)
```sql
CREATE TABLE charging_sessions (
    session_id           VARCHAR(32)    NOT NULL,
    reservation_id       VARCHAR(32)    NOT NULL,
    port_id              VARCHAR(16)    NOT NULL,
    status               VARCHAR(20)    NOT NULL DEFAULT 'CHARGING',
    start_time           TIMESTAMP(3)   NOT NULL,
    end_time             TIMESTAMP(3)   NULL,
    charging_power_kw    DECIMAL(6,2)   NOT NULL DEFAULT 7.20,
    energy_consumed_kwh  DECIMAL(10,4)  NOT NULL DEFAULT 0.0000,
    PRIMARY KEY (session_id),
    UNIQUE KEY uq_reservation_id (reservation_id),
    INDEX idx_status         (status),
    INDEX idx_reservation_id (reservation_id)
);
```
`TIMESTAMP(3)` preserves millisecond precision for accurate
duration/energy math.

### `station_pricing_tariffs` (Pricing cluster, all 3 instances)
```sql
CREATE TABLE station_pricing_tariffs (
    station_id           VARCHAR(32)   NOT NULL,
    base_price_per_kwh   DECIMAL(8,2)  NOT NULL DEFAULT 10.00,
    demand_level         VARCHAR(16)   NOT NULL DEFAULT 'LOW',
    demand_multiplier    DECIMAL(4,2)  NOT NULL DEFAULT 1.00,
    PRIMARY KEY (station_id),
    INDEX idx_demand_level (demand_level)
);
```

### `payments` (Payment cluster, all 3 instances)
```sql
CREATE TABLE payments (
    payment_id           VARCHAR(32)    NOT NULL,
    session_id           VARCHAR(32)    NOT NULL,
    station_id           VARCHAR(32)    NOT NULL DEFAULT 'S01',
    energy_consumed_kwh  DECIMAL(10,4)  NOT NULL,
    total_amount         DECIMAL(10,2)  NOT NULL,
    payment_status       VARCHAR(20)    NOT NULL DEFAULT 'SUCCESS',
    payment_time         TIMESTAMP      NOT NULL,
    PRIMARY KEY (payment_id),
    INDEX idx_session_id     (session_id),
    INDEX idx_payment_status (payment_status)
);
```

## Connection handling

`DBConnectionHelper.java` (default package, shared by every DAO) reads
`DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` exclusively
from environment variables — no credentials are hardcoded in Java source.
`getConnectionWithRetry(serverName, maxRetries)` retries up to 15 times with
a 2s pause between attempts so servers wait out MySQL container startup.
Every server degrades gracefully to pure in-memory operation (no crash) if
`DB_HOST` is unset or the database is unreachable.

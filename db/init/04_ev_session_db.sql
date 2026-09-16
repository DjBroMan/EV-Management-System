-- ============================================================
-- EV Charging Session Database
-- Instance: ev_session_db (ChargingSessionServer)
-- ============================================================
-- TIMESTAMP(3) is used for start_time and end_time to preserve
-- millisecond precision for accurate energy and duration calculation.
--
-- Timestamps: populated by ChargingSessionServer via
--             PhysicalClock.getSynchronizedPhysicalTimeMillis()
--
-- Operations:
--   startCharging  -> INSERT row, status = CHARGING
--   stopCharging   -> UPDATE end_time, energy_consumed_kwh, status = COMPLETED
-- ============================================================

CREATE DATABASE IF NOT EXISTS ev_session_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ev_session_db;

CREATE TABLE IF NOT EXISTS charging_sessions (
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
    INDEX idx_status           (status),
    INDEX idx_reservation_id   (reservation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

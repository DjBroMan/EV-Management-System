-- ============================================================
-- EV Charging Session Database -- Instance 2 (ChargingSessionServer-2, :1246)
-- ============================================================
-- Identical schema to ev_session_db (instance 1).
-- ============================================================

CREATE DATABASE IF NOT EXISTS ev_session_db_2
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ev_session_db_2;

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

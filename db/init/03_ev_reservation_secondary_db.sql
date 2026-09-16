-- ============================================================
-- EV Reservation Database — SECONDARY INSTANCE
-- Instance: ev_reservation_secondary_db (ReservationServer SECONDARY :1245)
-- ============================================================
-- Schema is IDENTICAL to ev_reservation_primary_db.
-- Secondary receives replicated writes via ReservationServerManager.
-- On failover, secondary is promoted to primary role using this DB
-- as-is — no schema changes, no data reconstruction required.
--
-- Timestamps: populated by ReservationServer SECONDARY via
--             PhysicalClock.getSynchronizedPhysicalTimeMillis()
-- ============================================================

CREATE DATABASE IF NOT EXISTS ev_reservation_secondary_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ev_reservation_secondary_db;

CREATE TABLE IF NOT EXISTS reservations (
    reservation_id  VARCHAR(32)  NOT NULL,
    user_id         VARCHAR(64)  NOT NULL,
    vehicle_id      VARCHAR(64)  NOT NULL,
    port_id         VARCHAR(16)  NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'CONFIRMED',
    created_at      TIMESTAMP    NOT NULL,
    PRIMARY KEY (reservation_id),
    INDEX idx_user_id  (user_id),
    INDEX idx_port_id  (port_id),
    INDEX idx_status   (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- EV Reservation Database -- TERTIARY INSTANCE (Reservation-2, :1255)
-- ============================================================
-- Schema is IDENTICAL to ev_reservation_primary_db / _secondary_db.
-- This is the 3rd node added to the Reservation cluster so a real 3-node
-- Bully election (not a hardcoded 2-node swap) can run: R3 (:1235,
-- ev_reservation_primary_db), R1 (:1245, ev_reservation_secondary_db),
-- R2 (:1255, ev_reservation_tertiary_db, this file).
-- ============================================================

CREATE DATABASE IF NOT EXISTS ev_reservation_tertiary_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ev_reservation_tertiary_db;

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

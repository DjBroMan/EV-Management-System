-- ============================================================
-- EV Charging Station Database -- Instance 3 (ChargingStationServer-3, :1254)
-- ============================================================
-- Identical schema to ev_station_db. Instance 3 has the highest Bully
-- server ID in this cluster and is the initial coordinator/PRIMARY.
-- ============================================================

CREATE DATABASE IF NOT EXISTS ev_station_db_3
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ev_station_db_3;

CREATE TABLE IF NOT EXISTS charging_ports (
    port_id       VARCHAR(16)  NOT NULL,
    station_id    VARCHAR(32)  NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'AVAILABLE',
    last_updated  TIMESTAMP    NOT NULL,
    PRIMARY KEY (port_id),
    INDEX idx_station_id (station_id),
    INDEX idx_status     (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

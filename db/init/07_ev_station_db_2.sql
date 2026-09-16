-- ============================================================
-- EV Charging Station Database -- Instance 2 (ChargingStationServer-2, :1244)
-- ============================================================
-- Identical schema to ev_station_db (instance 1). Part of the 3-instance
-- ChargingStation Bully cluster (CS1/CS2/CS3); each instance owns its own
-- database and self-seeds P1-P4 independently on first startup.
-- ============================================================

CREATE DATABASE IF NOT EXISTS ev_station_db_2
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ev_station_db_2;

CREATE TABLE IF NOT EXISTS charging_ports (
    port_id       VARCHAR(16)  NOT NULL,
    station_id    VARCHAR(32)  NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'AVAILABLE',
    last_updated  TIMESTAMP    NOT NULL,
    PRIMARY KEY (port_id),
    INDEX idx_station_id (station_id),
    INDEX idx_status     (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

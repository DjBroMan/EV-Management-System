-- ============================================================
-- EV Charging Station Database
-- Instance: ev_station_db (ChargingStationServer)
-- ============================================================
-- Timestamps: populated by ChargingStationServer via
--             PhysicalClock.getSynchronizedPhysicalTimeMillis()
--             NOT by DEFAULT CURRENT_TIMESTAMP.
-- Initial data: ChargingStationServer self-seeds P1-P4 on
--               first startup when this table is empty.
-- ============================================================

CREATE DATABASE IF NOT EXISTS ev_station_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ev_station_db;

CREATE TABLE IF NOT EXISTS charging_ports (
    port_id       VARCHAR(16)  NOT NULL,
    station_id    VARCHAR(32)  NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'AVAILABLE',
    last_updated  TIMESTAMP    NOT NULL,
    PRIMARY KEY (port_id),
    INDEX idx_station_id (station_id),
    INDEX idx_status     (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- EV Pricing Database
-- Instance: ev_pricing_db (PricingServer)
-- ============================================================
-- Initial data: PricingServer self-seeds S01/S02/S03 on first
-- startup when this table is empty.
--
-- Seeded values (by PricingServer startup logic):
--   S01: demand=LOW,    multiplier=1.00
--   S02: demand=MEDIUM, multiplier=1.25
--   S03: demand=HIGH,   multiplier=1.50
--
-- base_price_per_kwh default: 10.0 (matches PricingServer.BASE_PRICE)
-- ============================================================

CREATE DATABASE IF NOT EXISTS ev_pricing_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ev_pricing_db;

CREATE TABLE IF NOT EXISTS station_pricing_tariffs (
    station_id           VARCHAR(32)   NOT NULL,
    base_price_per_kwh   DECIMAL(8,2)  NOT NULL DEFAULT 10.00,
    demand_level         VARCHAR(16)   NOT NULL DEFAULT 'LOW',
    demand_multiplier    DECIMAL(4,2)  NOT NULL DEFAULT 1.00,
    PRIMARY KEY (station_id),
    INDEX idx_demand_level (demand_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

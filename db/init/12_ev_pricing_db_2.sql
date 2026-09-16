-- ============================================================
-- EV Pricing Database -- Instance 2 (PricingServer-2, :1248)
-- ============================================================
-- Identical schema to ev_pricing_db. Self-seeds S01/S02/S03 independently
-- on first startup (each Pricing instance is its own read replica).
-- ============================================================

CREATE DATABASE IF NOT EXISTS ev_pricing_db_2
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ev_pricing_db_2;

CREATE TABLE IF NOT EXISTS station_pricing_tariffs (
    station_id           VARCHAR(32)   NOT NULL,
    base_price_per_kwh   DECIMAL(8,2)  NOT NULL DEFAULT 10.00,
    demand_level         VARCHAR(16)   NOT NULL DEFAULT 'LOW',
    demand_multiplier    DECIMAL(4,2)  NOT NULL DEFAULT 1.00,
    PRIMARY KEY (station_id),
    INDEX idx_demand_level (demand_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

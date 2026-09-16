-- ============================================================
-- EV Pricing Database -- Instance 3 (PricingServer-3, :1258)
-- ============================================================
-- Identical schema to ev_pricing_db.
-- ============================================================

CREATE DATABASE IF NOT EXISTS ev_pricing_db_3
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ev_pricing_db_3;

CREATE TABLE IF NOT EXISTS station_pricing_tariffs (
    station_id           VARCHAR(32)   NOT NULL,
    base_price_per_kwh   DECIMAL(8,2)  NOT NULL DEFAULT 10.00,
    demand_level         VARCHAR(16)   NOT NULL DEFAULT 'LOW',
    demand_multiplier    DECIMAL(4,2)  NOT NULL DEFAULT 1.00,
    PRIMARY KEY (station_id),
    INDEX idx_demand_level (demand_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

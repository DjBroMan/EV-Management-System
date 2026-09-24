-- ============================================================
-- EV Payment Database -- Instance 3 (PaymentServer-3, :1257)
-- ============================================================
-- Identical schema to ev_payment_db.
-- ============================================================

CREATE DATABASE IF NOT EXISTS ev_payment_db_3
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ev_payment_db_3;

CREATE TABLE IF NOT EXISTS payments (
    payment_id           VARCHAR(32)    NOT NULL,
    session_id           VARCHAR(32)    NOT NULL,
    station_id           VARCHAR(32)    NOT NULL DEFAULT 'S01',
    energy_consumed_kwh  DECIMAL(10,4)  NOT NULL,
    total_amount         DECIMAL(10,2)  NOT NULL,
    payment_status       VARCHAR(20)    NOT NULL DEFAULT 'SUCCESS',
    payment_time         TIMESTAMP      NOT NULL,
    PRIMARY KEY (payment_id),
    INDEX idx_session_id     (session_id),
    INDEX idx_payment_status (payment_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS wallets (
    user_id       VARCHAR(32)    NOT NULL,
    balance       DECIMAL(10,2)  NOT NULL DEFAULT 0.00,
    last_updated  TIMESTAMP      NOT NULL,
    PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

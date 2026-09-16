-- ============================================================
-- EV Payment Database
-- Instance: ev_payment_db (PaymentServer)
-- ============================================================
-- Timestamps: populated by PaymentServer via
--             PhysicalClock.getSynchronizedPhysicalTimeMillis()
--
-- Operations:
--   makePayment      -> INSERT row
--   getPaymentStatus -> SELECT
--   getPaymentDetails -> SELECT
-- ============================================================

CREATE DATABASE IF NOT EXISTS ev_payment_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ev_payment_db;

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

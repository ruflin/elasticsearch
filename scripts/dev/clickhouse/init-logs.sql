-- Seed a ClickHouse `logs` database with a sample application-log table for the
-- ES|QL ClickHouse data source demo (scripts/dev/clickhouse/seed.sh).
--
-- The table is intentionally log-shaped (timestamp, level, service, host, message,
-- status, duration) so the demo can show realistic ES|QL queries (counts by level,
-- top services, etc.) against data that physically lives in ClickHouse.

CREATE DATABASE IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.app_logs
(
    `timestamp`   DateTime,
    `log_level`   String,
    `service`     String,
    `host`        String,
    `message`     String,
    `status_code` Int32,
    `duration_ms` Float64
)
ENGINE = MergeTree()
ORDER BY (timestamp, service);

INSERT INTO logs.app_logs
    (timestamp, log_level, service, host, message, status_code, duration_ms)
VALUES
    ('2026-06-17 09:00:01', 'INFO',  'checkout',  'host-a', 'order placed successfully',        200, 42.5),
    ('2026-06-17 09:00:05', 'INFO',  'checkout',  'host-b', 'order placed successfully',        200, 51.0),
    ('2026-06-17 09:00:09', 'WARN',  'checkout',  'host-a', 'slow downstream payment call',     200, 812.7),
    ('2026-06-17 09:00:12', 'ERROR', 'checkout',  'host-c', 'payment gateway timeout',          504, 3001.2),
    ('2026-06-17 09:00:15', 'INFO',  'catalog',   'host-a', 'product fetched',                  200, 12.3),
    ('2026-06-17 09:00:18', 'INFO',  'catalog',   'host-b', 'product fetched',                  200, 9.8),
    ('2026-06-17 09:00:21', 'DEBUG', 'catalog',   'host-b', 'cache hit for product 1042',       200, 1.1),
    ('2026-06-17 09:00:24', 'ERROR', 'catalog',   'host-c', 'product not found',                404, 7.4),
    ('2026-06-17 09:00:27', 'INFO',  'auth',      'host-a', 'user login',                       200, 33.0),
    ('2026-06-17 09:00:30', 'WARN',  'auth',      'host-a', 'repeated failed login attempt',    401, 18.6),
    ('2026-06-17 09:00:33', 'ERROR', 'auth',      'host-b', 'token signature verification failed', 401, 22.9),
    ('2026-06-17 09:00:36', 'INFO',  'gateway',   'host-c', 'request routed',                   200, 4.2),
    ('2026-06-17 09:00:39', 'INFO',  'gateway',   'host-c', 'request routed',                   200, 5.0),
    ('2026-06-17 09:00:42', 'WARN',  'gateway',   'host-a', 'upstream returned 5xx, retrying',  502, 120.4),
    ('2026-06-17 09:00:45', 'INFO',  'shipping',  'host-b', 'shipment created',                 201, 64.7),
    ('2026-06-17 09:00:48', 'INFO',  'shipping',  'host-b', 'shipment created',                 201, 60.1),
    ('2026-06-17 09:00:51', 'DEBUG', 'shipping',  'host-a', 'carrier rate lookup',              200, 14.0),
    ('2026-06-17 09:00:54', 'ERROR', 'shipping',  'host-c', 'carrier API unavailable',          503, 1502.8),
    ('2026-06-17 09:00:57', 'INFO',  'checkout',  'host-a', 'order placed successfully',        200, 47.3),
    ('2026-06-17 09:01:00', 'INFO',  'catalog',   'host-b', 'product fetched',                  200, 10.5);

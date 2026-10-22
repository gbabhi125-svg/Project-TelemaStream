CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS sensor_readings (
    time        TIMESTAMPTZ       NOT NULL,
    sensor_id   TEXT              NOT NULL,
    sensor_type TEXT              NOT NULL,
    value       DOUBLE PRECISION  NOT NULL,
    unit        TEXT              NOT NULL,
    lat         DOUBLE PRECISION,
    lng         DOUBLE PRECISION,
    valid       BOOLEAN           NOT NULL DEFAULT true
);

SELECT create_hypertable('sensor_readings', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_sensor_id_time ON sensor_readings (sensor_id, time DESC);

CREATE TABLE IF NOT EXISTS sensor_windows (
    window_end   TIMESTAMPTZ      NOT NULL,
    sensor_id    TEXT             NOT NULL,
    avg_value    DOUBLE PRECISION NOT NULL,
    min_value    DOUBLE PRECISION NOT NULL,
    max_value    DOUBLE PRECISION NOT NULL,
    sample_count INTEGER          NOT NULL
);

SELECT create_hypertable('sensor_windows', 'window_end', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS sensor_alerts (
    time        TIMESTAMPTZ NOT NULL,
    sensor_id   TEXT        NOT NULL,
    alert_type  TEXT        NOT NULL,
    message     TEXT        NOT NULL,
    severity    TEXT        NOT NULL DEFAULT 'warning'
);

SELECT create_hypertable('sensor_alerts', 'time', if_not_exists => TRUE);

SELECT add_retention_policy('sensor_readings', INTERVAL '7 days', if_not_exists => TRUE);

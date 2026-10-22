import { Pool } from "pg";
import { config } from "./config.js";

export const pool = new Pool({ connectionString: config.timescaleDsn, max: 10 });

export async function getRecentWindows(limit = 200) {
  const { rows } = await pool.query(
    `SELECT window_end, sensor_id, avg_value, min_value, max_value, sample_count
     FROM sensor_windows
     ORDER BY window_end DESC
     LIMIT $1`,
    [limit]
  );
  return rows;
}

export async function getRecentAlerts(limit = 50) {
  const { rows } = await pool.query(
    `SELECT time, sensor_id, alert_type, message, severity
     FROM sensor_alerts
     ORDER BY time DESC
     LIMIT $1`,
    [limit]
  );
  return rows;
}

export async function getThroughputSeries(minutes = 10) {
  const { rows } = await pool.query(
    `SELECT time_bucket('10 seconds', time) AS bucket, count(*) AS count
     FROM sensor_readings
     WHERE time > now() - ($1 || ' minutes')::interval
     GROUP BY bucket
     ORDER BY bucket ASC`,
    [minutes]
  );
  return rows;
}

export async function getSensorHealth() {
  const { rows } = await pool.query(
    `SELECT
       count(DISTINCT sensor_id) AS active_sensors,
       count(*) FILTER (WHERE NOT valid) AS invalid_readings,
       count(*) AS total_readings
     FROM sensor_readings
     WHERE time > now() - interval '5 minutes'`
  );
  return rows[0];
}

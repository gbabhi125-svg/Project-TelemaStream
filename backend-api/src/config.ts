export const config = {
  port: Number(process.env.PORT ?? 4000),
  timescaleDsn: process.env.TIMESCALE_DSN ?? "postgres://telemastream:telemastream@localhost:5432/telemastream",
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  readingsTopic: process.env.READINGS_TOPIC ?? "sensor.readings",
  alertsTopic: process.env.ALERTS_TOPIC ?? "sensor.alerts",
};

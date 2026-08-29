-- monitors.sql — Lakehouse Monitoring monitors for the gold tables.
-- Run in Databricks SQL or a notebook. See README Part 2, step 9.

CREATE MONITOR IF NOT EXISTS dev.gold.forecast_features
  ON TABLE dev.gold.forecast_features
  WITH (metric_type = 'table', schedule = 'DAILY');

CREATE MONITOR IF NOT EXISTS dev.gold.forecasts
  ON TABLE dev.gold.forecasts
  WITH (metric_type = 'table', schedule = 'DAILY');

-- Datadog side (external): enable the Databricks integration in Datadog and
-- export workspace metrics; job health + serving latency land there too.

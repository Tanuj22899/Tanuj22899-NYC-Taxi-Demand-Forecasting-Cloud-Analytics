-- Creates:
-- 1) NYC_Taxi.hourly_revenue_actual  (hourly actual revenue)
-- 2) NYC_Taxi.m_hourly_revenue_lr    (cheap model)
-- 3) NYC_Taxi.hourly_revenue_forecast_next_month (ALL hours of next month)

BEGIN

-- 1) Hourly ACTUAL revenue table
CREATE OR REPLACE TABLE `impressive-kite-480816-r4.NYC_Taxi.hourly_revenue_actual`
PARTITION BY DATE(hour_ts)
AS
SELECT
  TIMESTAMP_TRUNC(pickup_datetime, HOUR) AS hour_ts,
  SUM(total_amount) AS revenue,
  EXTRACT(HOUR FROM TIMESTAMP_TRUNC(pickup_datetime, HOUR)) AS hour_of_day,
  EXTRACT(DAYOFWEEK FROM TIMESTAMP_TRUNC(pickup_datetime, HOUR)) AS dow
FROM `impressive-kite-480816-r4.NYC_Taxi.nyc_taxi_cleaned_data`
WHERE pickup_datetime IS NOT NULL
  AND total_amount IS NOT NULL
GROUP BY hour_ts, hour_of_day, dow;

-- 2) Train a cheap model (works for any future month, no horizon limit)
-- Adds a simple trend feature: hour_index
CREATE OR REPLACE MODEL `impressive-kite-480816-r4.NYC_Taxi.m_hourly_revenue_lr`
OPTIONS(
  model_type = 'LINEAR_REG',
  input_label_cols = ['revenue']
) AS
SELECT
  revenue,
  hour_of_day,
  dow,
  -- trend feature (hours since epoch)
  CAST(DIV(UNIX_SECONDS(hour_ts), 3600) AS FLOAT64) AS hour_index
FROM `impressive-kite-480816-r4.NYC_Taxi.hourly_revenue_actual`
WHERE revenue IS NOT NULL;

-- 3) Forecast table for NEXT MONTH (after your latest actual date), ALL hours included
CREATE OR REPLACE TABLE `impressive-kite-480816-r4.NYC_Taxi.hourly_revenue_forecast_next_month`
PARTITION BY DATE(hour_ts)
AS
WITH maxd AS (
  SELECT MAX(DATE(hour_ts)) AS max_date
  FROM `impressive-kite-480816-r4.NYC_Taxi.hourly_revenue_actual`
),
bounds AS (
  SELECT
    DATE_TRUNC(DATE_ADD(max_date, INTERVAL 1 MONTH), MONTH) AS start_next_month,
    DATE_TRUNC(DATE_ADD(max_date, INTERVAL 2 MONTH), MONTH) AS start_month_after
  FROM maxd
),
month_hours AS (
  SELECT ts AS hour_ts
  FROM bounds,
  UNNEST(
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP(start_next_month),
      TIMESTAMP_SUB(TIMESTAMP(start_month_after), INTERVAL 1 HOUR),
      INTERVAL 1 HOUR
    )
  ) AS ts
),
features AS (
  SELECT
    hour_ts,
    EXTRACT(HOUR FROM hour_ts) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM hour_ts) AS dow,
    CAST(DIV(UNIX_SECONDS(hour_ts), 3600) AS FLOAT64) AS hour_index
  FROM month_hours
)
SELECT
  f.hour_ts,
  p.predicted_revenue AS revenue,
  f.hour_of_day,
  f.dow,
  TRUE AS is_forecast,
  CURRENT_TIMESTAMP() AS forecast_generated_at
FROM features f
JOIN ML.PREDICT(
  MODEL `impressive-kite-480816-r4.NYC_Taxi.m_hourly_revenue_lr`,
  (SELECT * FROM features)
) p
USING (hour_ts);

END;

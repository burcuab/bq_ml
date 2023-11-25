
--Create historical sessions data per date and device category
CREATE or replace TABLE `bqml.daily_sessions` as
SELECT 
  PARSE_DATE("%Y%m%d", event_date) AS date,
  IFNULL(device.category, "") AS device_category,
  count(distinct concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id'))) AS sessions,
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20200101' AND '20211231' 
GROUP BY 1,2;



--create sessions model using the sessions table to predict next 30 days, we use only the first year
CREATE OR REPLACE MODEL `bqml.daily_sessions_prediction_model` 
      OPTIONS(
        MODEL_TYPE = 'ARIMA_PLUS',
        TIME_SERIES_TIMESTAMP_COL = 'date',
        TIME_SERIES_ID_COL = 'device_category',
        TIME_SERIES_DATA_COL = 'sessions',
        DATA_FREQUENCY = 'DAILY',
        HORIZON = 30
      ) AS
      SELECT
        date,
        device_category,
        sessions
      FROM `bqml.daily_sessions` where date BETWEEN '2020-01-01' AND '2020-12-31'
;

--evaluate model using next historical year
SELECT
  *
FROM
  ml.EVALUATE(MODEL `burcuproject.bqml.daily_sessions_prediction_model`, (
SELECT
  
        date,
        device_category,
        sessions
FROM
   `burcuproject.bqml.daily_sessions`
WHERE
  date BETWEEN '2021-01-01' AND '2021-12-31'),
    STRUCT(TRUE AS perform_aggregation, 30 AS horizon))
;


--combine forecasted and historical sessions per day and device category
SELECT
        device_category,
        date,
        sessions,
        "history"                       AS `time_serie_type`,
        CAST(NULL AS FLOAT64)           AS `sessions_lower_bound`,
        CAST(NULL AS FLOAT64)           AS `sessions_upper_bound`,
      FROM `burcuproject.bqml.daily_sessions`
	WHERE date BETWEEN '2020-01-01' AND '2020-12-31'
      UNION ALL
      SELECT
        device_category,
        date(forecast_timestamp)              AS `date`,
        forecast_value                  AS `sessions`,
        "forecast"                      AS `time_serie_type`,
        prediction_interval_lower_bound AS `sessions_lower_bound`,
        prediction_interval_upper_bound AS `sessions_upper_bound`,
      FROM ML.FORECAST(
        MODEL `burcuproject.bqml.daily_sessions_prediction_model`,
        STRUCT(30 AS `horizon`, 0.80 AS `confidence_level`))
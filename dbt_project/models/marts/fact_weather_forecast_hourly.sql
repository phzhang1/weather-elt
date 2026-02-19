/*
  Mart table: hourly weather forecast – one row per location per forecast hour.
  Grain: (latitude, longitude, forecast_timestamp).
  Deduplication: within each grain, keeps the latest row by ingested_at using array_agg.-
  -- This ensures that even if multiple syncs deliver the same forecast hour, we only keep the most recent.
  Incremental strategy: merge on unique_key to update existing forecasts with newer versions.
  -- The merge strategy ensures we never have duplicates and always have the latest forecast.
  Business logic: unit conversions (K → °C, m/s → km/h, probability → percent).
*/
{{
  config(
    alias='fact_weather_forecast_hourly',
    materialized='incremental',
    unique_key=['latitude', 'longitude', 'forecast_timestamp'],
    incremental_strategy='merge'
  )
}}

-- Round coordinates for consistent grouping (see note in fact_weather_current)
with staged as (
  select
    round(latitude, 6) as latitude,
    round(longitude, 6) as longitude,
    forecast_timestamp,
    ingested_at,
    temperature_kelvin,
    humidity_percent,
    wind_speed,
    pop
  from {{ ref('stg_weather_forecast') }}
  {% if is_incremental() %}
  -- Incremental filter: only fetch forecast hours newer than the latest already in the table.
  -- Assumes forecast_timestamp increases monotonically with new data.
    where forecast_timestamp > (select max(forecast_timestamp) from {{ this }})
  {% endif %}
),

deduped as (
  select
    latitude,
    longitude,
    forecast_timestamp,
    (array_agg(
      struct(
        ingested_at,
        temperature_kelvin,
        humidity_percent,
        wind_speed,
        pop
      )
      order by ingested_at desc
      limit 1
    ))[offset(0)].*
  from staged
  group by latitude, longitude, forecast_timestamp
)

select
  latitude,
  longitude,
  forecast_timestamp,
  ingested_at,
  temperature_kelvin,
  round(temperature_kelvin - 273.15, 2) as temperature_celsius,
  humidity_percent,
  wind_speed as wind_speed_m_s,
  round(wind_speed * 3.6, 2) as wind_speed_km_h,
  pop as precipitation_probability,
  round(pop * 100, 1) as precipitation_probability_percent
from deduped
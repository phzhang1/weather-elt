/*
  Mart table: current weather conditions – latest snapshot per location.
  Grain: one row per (latitude, longitude).
  Deduplication: uses array_agg to pick the latest observation per location.
  Business logic: unit conversions (K → °C, m/s → km/h).
*/
{{
  config(alias='fact_weather_current')
}}

-- Round coordinates to 6 decimal places to ensure consistent grouping despite floating-point precision issues
with staged as (
  select
    round(latitude, 6) as latitude,
    round(longitude, 6) as longitude,
    observation_timestamp,
    ingested_at,
    temperature_kelvin,
    humidity_percent,
    wind_speed
  from {{ ref('stg_weather_current') }}
),

deduped as (
  select
    latitude,
    longitude,
    (array_agg(
      struct(
        observation_timestamp,
        ingested_at,
        temperature_kelvin,
        humidity_percent,
        wind_speed
      )
      order by observation_timestamp desc, ingested_at desc
      limit 1
    ))[offset(0)].*
  from staged
  group by latitude, longitude
)

select
  latitude,
  longitude,
  observation_timestamp,
  ingested_at,
  temperature_kelvin,
  round(temperature_kelvin - 273.15, 2) as temperature_celsius,
  humidity_percent,
  wind_speed as wind_speed_m_s,
  round(wind_speed * 3.6, 2) as wind_speed_km_h
from deduped
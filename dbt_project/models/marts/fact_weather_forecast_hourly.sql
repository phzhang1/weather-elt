{{
  config(
    alias='fact_weather_forecast_hourly',
    materialized='incremental',
    unique_key=['latitude', 'longitude', 'forecast_timestamp'],
    incremental_strategy='merge'
  )
}}

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
    where forecast_timestamp > (select max(forecast_timestamp) from {{ this }})
  {% endif %}
),

-- One row per (latitude, longitude, forecast_timestamp): pick latest by ingested_at
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
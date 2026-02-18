-- Staging layer: unnest hourly forecast array from One Call API, standardize columns.
-- One row per forecast hour per location. No business logic—light transforms only.
{{
  config(
    alias='stg_weather_forecast',
  )
}}

with source as (
  select * from {{ source('raw', 'onecall') }}
),

flattened as (
  select
    cast(json_value(hour, '$.dt') as int64) as forecast_timestamp,
    cast(json_value(hour, '$.temp') as float64) as temperature_kelvin,
    cast(json_value(hour, '$.humidity') as int64) as humidity_percent,
    cast(json_value(hour, '$.wind_speed') as float64) as wind_speed,
    cast(json_value(hour, '$.pop') as float64) as pop,
    source._airbyte_extracted_at as ingested_at,
    cast(source.lat as float64) as latitude,
    cast(source.lon as float64) as longitude
  from source,
  unnest(json_query_array(source.hourly, '$')) as hour
)

select * from flattened

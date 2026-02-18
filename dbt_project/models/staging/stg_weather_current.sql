-- Staging layer: parse raw JSON, standardize column names and data types.
-- No business logic—only light transforms for downstream marts.
{{
  config( 
    alias='stg_weather_current'
  )
}}

with source as (
  select * from {{ source('raw', 'onecall') }} # source table from Airbyte
),

staged as (
  select
    cast(json_value(`current`, '$.dt') as int64) as observation_timestamp,
    cast(json_value(`current`, '$.temp') as float64) as temperature_kelvin,
    cast(json_value(`current`, '$.humidity') as int64) as humidity_percent,
    cast(json_value(`current`, '$.wind_speed') as float64) as wind_speed,
    _airbyte_extracted_at as ingested_at,
    cast(lat as float64) as latitude,
    cast(lon as float64) as longitude
  from source
)

select * from staged  --select all from the staged table
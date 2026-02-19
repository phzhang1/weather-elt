# OpenWeather ELT Pipeline

## Overview

This project implements a complete ELT pipeline that ingests current and forecast weather data from the OpenWeather API, loads it into BigQuery, and transforms it into analytics‑ready tables using dbt. The pipeline supports multiple geographic locations, maintains historical records, and includes built‑in data quality tests. It demonstrates a modern data stack and the principles of the ELT pipeline.

## Core Architecture

- **Ingestion**: Airbyte (Cloud) - extracts data from the OpenWeather One Call API and loads raw JSON into BigQuery. (OpenWeather API connector)
- **Warehouse**: BigQuery - stores raw data and serves as the transformation engine.
- **Transformation**: dbt Core - parses JSON, cleans and standardises data, and builds modular, tested models.
- **Orchestration (planned/explored)**: Apache Airflow - designed to coordinate the pipeline; integration was prototyped but is not part of the current automated workflow.

## Data Flow

1. **Extract & Load:** Airbyte syncs weather data on a hourly basis and lands it in a raw BigQuery dataset as JSON blobs.

2. **Staging**: dbt staging models parse the JSON, unnest nested structures (e.g., hourly forecasts), and apply light type casting and renaming.

3. **Mart**: dbt mart models build business‑ready tables:

- `fact_weather_current` – latest snapshot per location (deduplicated).

- `fact_weather_forecast_hourly` – historical forecast records, one row per location per hour.

4. **Testing**: dbt tests enforce data quality – not‑null, uniqueness on natural keys, and range checks (e.g., temperature in °C, humidity 0‑100).

## Key Implementation Details

- **Prevent schema drift**: Raw data is stored as JSON, so API changes do not break ingestion. 

- **Idempotent transformations**: dbt models are designed to be re-runnable without duplicating data.

- **Deduplication**: Current weather snapshots use `row_number` to keep only the latest observation per location; forecasts retain all versions but can be filtered to the latest if needed. 

- **Data quality**: Over 30 automated tests ensure correctness - freshness, uniqueness, accepted value ranges.

## Tools
- Airbyte Cloud
- BigQuery
- dbt Core 1.11.4 (dbt-bigquery adapter)
- Python 3.11 (for dbt environment)
- (Prototype) Apache Airflow 2.10.4 - local development instance with DAGs for future orchestration

## Orchestration Status
An airflow DAG was developed to monitor Airbyte sync completion and trigger dbt runs automatically. While this integration confirmed the approach works, the final automated setup is not included due to time constraints and debugging issues. The DAG code is available in the `airflow/dags/` folder as a reference for future extension.

## Results
- Raw weather data is reliably ingested and stored in BigQuery
- Staging models correctly parse JSON into typed columns
- Mart tables contain clean, deduplicated data ready for analysis or machine learning.
- All core dbt tests pass, ensuring data quality. 

## What I Learned
- Designing an ELT pipeline with modern data tools like Airbyte, BigQuery, and dbt. 
- Handling semi-structured JSON in BigQuery and flattening nested arrays.
- Implementig incremental logic in dbt to improve efficiency over full refresh, including using a `merge` strategy instead of simple append.
- Importance of isolated environments (Conda) and containerisation (Docker) for reproducible setups.

## Next Steps/Potential Enhancements
- Complete the Airflow integration for fully automated orchestration. 
- Add a `dim_locations` table for enriched location metadata.
    - Using outside data sources for integration
- Containerise the dbt environment for easier deployment.
- Leverage dbt models for machine learning features 
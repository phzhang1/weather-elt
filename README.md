# OpenWeather ELT Pipeline

## Overview

This project builds a core ELT pipeline for ingesting weather data from the OpenWeather API, loading it into a data warehouse, and transforming it into analytics-ready tables using dbt. The focus of the MVP is on reliable ingestion, clean data modeling, and reproducible transformations rather than advanced monitoring or visualization.

This project builds an ELT pipeline for ingesting multi-location weather data from the OpenWeather API, loading it into BigQuery, and transforming it into analytics-ready tables using dbt. The pipeline supports multiple geographic locations and maintains historical weather observations and forecasts.
## MVP Scope

The MVP implements a minimal but complete ELT workflow:

- Ingest raw OpenWeather API data using Airbyte
- Load raw JSON data into BigQuery without enforcing a rigid schema
- Transform raw data into clean, typed tables using dbt
- Produce analytics-ready fact tables for downstream analysis
- Apply basic data quality tests to ensure correctness and reliability

## Core Architecture

- **Ingestion**: Airbyte (OpenWeather API connector)
- **Warehouse**: BigQuery
- **Transformation**: dbt Core
- **Orchestration**: Airflow

## Data Flow

1. Airbyte extracts weather data from the OpenWeather API and loads raw JSON into BigQuery.
2. dbt staging models parse and standardize raw JSON fields.
3. dbt models transform staged data into analytics-ready fact tables.

## dbt Model Layers

- **Staging**  
  - Parse raw JSON fields from the OpenWeather API  
  - Standardize column names and data types  

- **Analytics Layer**  
  - Fact tables at a defined grain (e.g., hourly weather by location)  
  - Tables designed for downstream querying and analysis  

## Data Quality

The MVP includes basic data quality checks implemented in dbt:
- Not-null and uniqueness tests on primary keys
- Type and range validation for core metrics (e.g., temperature, timestamps)

## Running the Project


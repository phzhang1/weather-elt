"""
DAG: weather_elt_pipeline
Description: Orchestrates Airbyte sync, dbt run, and dbt test.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Configuration
# Airbyte connection ID stored as an Airflow Variable
AIRBYTE_CONNECTION_ID = Variable.get("airbyte_connection_id")
AIRBYTE_API_TOKEN = Variable.get("airbyte_api_token") 

# Path to dbt project (relative to this DAG file)
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
DBT_PROJECT_PATH = os.path.join(DAG_DIR, "..", "dbt_project")  # assumes airflow_local/ and dbt_project/ are siblings

# Conda environment for dbt
DBT_CONDA_ENV = "dbt_clean"
DBT_CMD_PREFIX = f"conda run -n {DBT_CONDA_ENV} dbt"

# Default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 13),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG def
with DAG(
    dag_id="weather_elt_pipeline",
    default_args=default_args,
    description="ELT pipeline for OpenWeather data",
    schedule="@hourly",
    catchup=False,
    tags=["weather", "elt"],
) as dag:

    # Task 1: Wait for sync to complete instead of triggering it
    wait_for_sync = AirbyteJobSensor(
        task_id="wait_for_airbyte_sync",
        airbyte_conn_id="airbyte_default",         
        connection_id=AIRBYTE_CONNECTION_ID,
        # The sensor will automatically find the most recent job for this connection
        # and wait until it reaches a terminal state (success/failure).
        mode="poke",
        poke_interval=60,                            # Check every 60 seconds
        timeout=3600,                                 # Fail after 1 hour if no completion
    )


    # Task 2: Run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_PROJECT_PATH} && {DBT_CMD_PREFIX} run
        """,
    )

    # Task 3: Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {DBT_PROJECT_PATH} && {DBT_CMD_PREFIX} test
        """,
    )

    # Set dependencies
    trigger_airbyte >> dbt_run >> dbt_test
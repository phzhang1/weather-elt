"""
DAG: weather_elt_pipeline
Description: Orchestrates Airbyte sync, dbt run, and dbt test.
"""

import os
import logging
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.hooks.base import BaseHook
from airflow.models import Variable

# Configuration
# Airbyte connection ID stored as an Airflow Variable
AIRBYTE_CONNECTION_ID = Variable.get("airbyte_connection_id")
AIRBYTE_API_TOKEN = Variable.get("airbyte_api_token")  # not used in this version, but kept for compatibility

# Workspace ID for API calls (added, but keep comment structure)
AIRBYTE_WORKSPACE_ID = Variable.get("airbyte_workspace_id")

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

    def get_latest_airbyte_job(**context):
        # List jobs for the connection (you may need to add pagination, but latest is usually first)
        conn = BaseHook.get_connection("airbyte_default")
        workspace_id = context['templates_dict']['workspace_id']
        connection_id = context['templates_dict']['connection_id']

        # Obtain a fresh access token using client credentials
        token_url = f"{conn.host}/v1/applications/token"
        token_resp = requests.post(
            token_url,
            json={"client_id": conn.login, "client_secret": conn.password},
            timeout=10
        )
        token_resp.raise_for_status()
        access_token = token_resp.json()["access_token"]

        # Get the most recent job ID (adjust depending on API response structure)
        jobs_url = f"{conn.host}/v1/jobs?connectionId={connection_id}&workspaceId={workspace_id}&limit=1&orderBy=createdAt%3Adesc"
        headers = {"Authorization": f"Bearer {access_token}"}
        jobs_resp = requests.get(jobs_url, headers=headers, timeout=30)
        jobs_resp.raise_for_status()
        jobs_data = jobs_resp.json()

        if not jobs_data.get('data'):
            raise Exception("No Airbyte jobs found for this connection")
        latest_job_id = jobs_data['data'][0]['jobId']
        logging.info(f"Latest job ID: {latest_job_id}")
        context['ti'].xcom_push(key='latest_job_id', value=latest_job_id)

    get_latest_job = PythonOperator(
        task_id='get_latest_airbyte_job',
        python_callable=get_latest_airbyte_job,
        templates_dict={
            'workspace_id': AIRBYTE_WORKSPACE_ID,
            'connection_id': AIRBYTE_CONNECTION_ID
        },
        provide_context=True,
    )

    # Task 1: Wait for sync to complete instead of triggering it
    wait_for_sync = AirbyteJobSensor(
        task_id="wait_for_airbyte_sync",
        airbyte_conn_id="airbyte_default",
        airbyte_job_id="{{ ti.xcom_pull(task_ids='get_latest_airbyte_job', key='latest_job_id') }}",
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
    get_latest_job >> wait_for_sync >> dbt_run >> dbt_test
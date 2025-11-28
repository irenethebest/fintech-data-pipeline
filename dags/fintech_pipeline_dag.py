from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
}

with DAG(
    'fintech_daily_pipeline',
    default_args=default_args,
    description='Ingest Finance Data -> dbt Transform',
    schedule_interval='0 8 * * *', # Run daily at 8am
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # TASK 1: Run the Python Ingestion Script on Databricks
    # This submits the script located in your Repo to the cluster
    ingest_task = DatabricksSubmitRunOperator(
        task_id='ingest_raw_data',
        databricks_conn_id='databricks_default',
        new_cluster={
            'spark_version': '13.3.x-scala2.12',
            'node_type_id': 'Standard_DS3_v2',
            'num_workers': 1
        },
        spark_python_task={
            'python_file': 'dbfs:/FileStore/scripts/ingest_daily.py', # Path to your script
            'parameters': []
        }
    )

    # TASK 2: Run dbt
    # In a simple setup, we use BashOperator to run 'dbt run' 
    # (assuming Airflow has dbt installed)
    dbt_run = BashOperator(
        task_id='dbt_transform',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .'
    )

    # TASK 3: Run dbt Tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .'
    )

    # Set Dependencies
    ingest_task >> dbt_run >> dbt_test
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='kedro-clean',
    default_args=default_args,
    schedule_interval='0 */3 * * *',
    catchup=False,
    tags=['kedro'],
) as dag:
    
    run_pipeline = BashOperator(
        task_id='run_kedro_pipeline',
        bash_command='cd /opt/kedro-clean && kedro run',
    )
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="kedro-ml-retrain",
    default_args=default_args,
    schedule_interval="0 15 * * *",  # tous les jours à 15h00
    catchup=False,
    tags=["kedro", "ml"],
) as dag:

    run_ml_pipeline = BashOperator(
        task_id="run_kedro_ml_pipeline",
        bash_command="cd /opt/kedro-clean && kedro run --pipeline=ml",
    )
import textwrap
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from apache_airflow_providers_tecton.operators.tecton_trigger_operator import TectonTriggerOperator

with DAG(
        dag_id="triggered_fv_async",
        default_args={
            'retries': 3
        },
        description=textwrap.dedent("""
        A simple dag example 
    """),
        start_date=datetime(2022, 7, 10),
        schedule_interval=timedelta(days=1)
) as dag:
    mark_me_complete = BashOperator(
        task_id="mark_me_complete",
        bash_command='echo "upstream"'
    )
    task = TectonTriggerOperator(
        task_id="trigger_tecton",
        workspace="integration_test_alex",
        feature_view="test_bfv",
        online=True,
        offline=True
    )
    mark_me_complete >> task


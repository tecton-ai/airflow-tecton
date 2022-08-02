import textwrap
from datetime import datetime, timedelta

from airflow import DAG

with DAG(
    dag_id="scheduled_fv_example",
    default_args={
        'retries': 3
    },
    description=textwrap.dedent("""
        A simple dag example 
    """),
    start_date=datetime(2022, 7, 14),
    schedule_interval=timedelta(days=1)
) as dag:
    #wait_for_tecton = TectonSensor()
    #train_tecton = PythonVirtualenvOperator()
    pass

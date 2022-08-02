import textwrap
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from apache_airflow_providers_tecton.operators.tecton_trigger_operator import TectonTriggerOperator
from apache_airflow_providers_tecton.sensors.tecton_sensor import TectonSensor

WORKSPACE = "my_workspace"
FEATURE_VIEW = "my_batch_feature_view"

with DAG(
        dag_id="triggered_fv_async",
        default_args={
            'retries': 3
        },
        description=textwrap.dedent("""
            This example shows a use case where you have a BatchFeatureView with triggered materialization 
            where Tecton handles retries.
            
            Note that because the operator is async, we have to use TectonSensor to detect when the jobs are complete.
            
            In this scenario, we want to kick off a model training when the offline feature store is ready, as well as 
            report when the online feature store is up to date to our monitoring. We use example BashOperators in place 
            of actual training/reporting operators.
    """),
        start_date=datetime(2022, 7, 10),
        schedule_interval=timedelta(days=1)
) as dag:
    process_hive_data = BashOperator(
        task_id="process_hive_data",
        bash_command='echo "hive data processed!"'
    )
    tecton_trigger = TectonTriggerOperator(
        task_id="trigger_tecton",
        workspace=WORKSPACE,
        feature_view=FEATURE_VIEW,
        online=True,
        offline=True
    )
    data_ready = TectonSensor(
        task_id="wait_for_offline",
        workspace=WORKSPACE,
        feature_view=FEATURE_VIEW,
        online=True,
        offline=True
    )
    train_model = BashOperator(
        task_id="train_model",
        bash_command='echo "model trained!"'
    )
    report_online_done = BashOperator(
        task_id="report_online_done",
        bash_command='echo "online data ready!"'
    )
    process_hive_data >> tecton_trigger >> data_ready >> train_model
    data_ready >> report_online_done


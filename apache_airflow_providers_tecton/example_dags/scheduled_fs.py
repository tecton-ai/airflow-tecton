import textwrap
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from apache_airflow_providers_tecton.sensors.tecton_sensor import TectonSensor

WORKSPACE = "my_workspace"
FEATURE_SERVICE = "my_feature_service"

with DAG(
    dag_id="scheduled_fs_example",
    description=textwrap.dedent("""
        This example shows a use case where you have a FeatureService with both online and offline materialization.
        
        This example can be adapted to work with Triggered and Scheduled FeatureViews as well, but the assumption is 
        that this is a FeatureService which can have a mix of both.
        
        In this scenario, we want to kick off a model training when the offline feature store is ready, as well as 
            report when the online feature store is up to date to our monitoring. We use example BashOperators in place 
            of actual training/reporting operators.
    """),
    start_date=datetime(2022, 7, 14),
    schedule_interval=timedelta(days=1)
) as dag:
    wait_for_feature_service_online = TectonSensor(
        task_id="wait_for_fs_online",
        workspace=WORKSPACE,
        feature_service=FEATURE_SERVICE,
        online=True,
        offline=False
    )
    wait_for_feature_service_offline = TectonSensor(
        task_id="wait_for_fs_offline",
        workspace=WORKSPACE,
        feature_service=FEATURE_SERVICE,
        online=False,
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
    wait_for_feature_service_online >> report_online_done
    wait_for_feature_service_offline >> train_model

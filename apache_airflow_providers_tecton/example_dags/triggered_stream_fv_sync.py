import textwrap
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from apache_airflow_providers_tecton.operators.tecton_job_operator import TectonJobOperator
from apache_airflow_providers_tecton.sensors.tecton_sensor import TectonSensor

WORKSPACE = "my_workspace"
FEATURE_VIEW = "my_stream_feature_view"

with DAG(
        dag_id="triggered_fv_sync",
        default_args={
            'retries': 3
        },
        description=textwrap.dedent("""
            This example shows a use case where you have a BatchFeatureView with triggered materialization 
            where Airflow handles retries. Note that the retry parameters
            used are standard Airflow retries. 
            
            Because this is a StreamFeatureView, we do not need to use our
            materialization job to write to the online store.
            
            We use TectonSensor for online only. Note that model training can just wait for `tecton_job` becuase this 
            operator waits for completion. Similarly, the online reporting part can proceed independently.
            
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
    tecton_job = TectonJobOperator(
        task_id="trigger_tecton",
        workspace=WORKSPACE,
        feature_view=FEATURE_VIEW,
        online=False,
        offline=True,
        # retries inherited from default_args
    )
    online_data_ready = TectonSensor(
        task_id="wait_for_online",
        workspace=WORKSPACE,
        feature_view=FEATURE_VIEW,
        online=True,
        offline=False
    )
    train_model = BashOperator(
        task_id="train_model",
        bash_command='echo "model trained!"'
    )
    report_online_done = BashOperator(
        task_id="report_online_done",
        bash_command='echo "online data ready!"'
    )

    process_hive_data >> tecton_job >> train_model
    online_data_ready >> report_online_done

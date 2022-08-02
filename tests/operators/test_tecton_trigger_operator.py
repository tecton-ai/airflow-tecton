import datetime
import unittest
from unittest.mock import patch, MagicMock, Mock

from airflow.utils.context import Context

from apache_airflow_providers_tecton.operators.tecton_trigger_operator import TectonTriggerOperator


class TestTectonTriggerOperator(unittest.TestCase):
    JOB = {
        "job": {
            "id": "abc"
        }

    }
    @patch('apache_airflow_providers_tecton.operators.tecton_trigger_operator.TectonHook.create')
    def test_execute(self, mock_create):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.submit_materialization_job.return_value=self.JOB

        operator = TectonTriggerOperator(
            task_id="abc",
            workspace='prod',
            feature_view='fv',
            online=True,
            offline=True,
            start_time=datetime.datetime(2022, 7, 1),
            end_time=datetime.datetime(2022, 7, 2),
        )
        operator.execute(Context())


import datetime
import unittest
from unittest.mock import patch, MagicMock, Mock

from airflow.utils.context import Context

from apache_airflow_providers_tecton.operators.tecton_job_operator import TectonJobOperator


class TestTectonJobOperator(unittest.TestCase):
    JOB = {
        "job": {
            "id": "abc"
        }

    }
    GET_JOB_RUNNING_NO_ATTEMPT = {
        "job": {
            "id": "abc",
            "state": "RUNNING"
        }
    }
    GET_JOB_RUNNING = {
        "job": {
            "id": "abc",
            "state": "RUNNING",
            "attempts": [
                {
                "state": "RUNNING",
                "run_url": "example.com"
                }
            ]
        }
    }
    GET_JOB_SUCCESS = {
        "job": {
            "id": "abc",
            "state": "SUCCESS",
            "attempts": [
                {
                    "state": "SUCCESS",
                    "run_url": "example.com"
                }
            ]
        }

    }
    GET_JOB_FAILURE_NO_ATTEMPTS = {
        "job": {
            "id": "abc",
            "state": "ERROR"
        }
    }
    GET_JOB_FAILURE = {
        "job": {
            "id": "abc",
            "state": "ERROR",
            "attempts": [
                {
                    "state": "ERROR",
                    "run_url": "example.com"
                }
            ]
        }
    }

    @patch('time.sleep', return_value=None)
    @patch('apache_airflow_providers_tecton.operators.tecton_job_operator.TectonHook.create')
    def test_execute(self, mock_create, mock_sleep):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.get_materialization_job.side_effect=[
            self.GET_JOB_RUNNING_NO_ATTEMPT,
            self.GET_JOB_RUNNING,
            self.GET_JOB_SUCCESS
        ]
        mock_hook.submit_materialization_job.return_value=self.JOB

        operator = TectonJobOperator(
            task_id="abc",
            workspace='prod',
            feature_view='fv',
            online=True,
            offline=True,
            start_time=datetime.datetime(2022, 7, 1),
            end_time=datetime.datetime(2022, 7, 2),
        )
        operator.execute(Context())

    @patch('time.sleep', return_value=None)
    @patch('apache_airflow_providers_tecton.operators.tecton_job_operator.TectonHook.create')
    def test_execute_failed(self, mock_create, mock_time):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.get_materialization_job.side_effect=[
            self.GET_JOB_RUNNING_NO_ATTEMPT,
            self.GET_JOB_RUNNING,
            self.GET_JOB_FAILURE
        ]
        mock_hook.submit_materialization_job.return_value=self.JOB

        operator = TectonJobOperator(
            task_id="abc",
            workspace='prod',
            feature_view='fv',
            online=True,
            offline=True,
            start_time=datetime.datetime(2022, 7, 1),
            end_time=datetime.datetime(2022, 7, 2),
        )
        with self.assertRaises(Exception) as e:
            operator.execute(Context())
        self.assertIn("Final job state", str(e.exception))

    @patch('time.sleep', return_value=None)
    @patch('apache_airflow_providers_tecton.operators.tecton_job_operator.TectonHook.create')
    def test_execute_failed_no_attempts(self, mock_create, mock_time):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.get_materialization_job.side_effect=[
            self.GET_JOB_RUNNING_NO_ATTEMPT,
            self.GET_JOB_RUNNING,
            self.GET_JOB_FAILURE_NO_ATTEMPTS
        ]
        mock_hook.submit_materialization_job.return_value=self.JOB

        operator = TectonJobOperator(
            task_id="abc",
            workspace='prod',
            feature_view='fv',
            online=True,
            offline=True,
            start_time=datetime.datetime(2022, 7, 1),
            end_time=datetime.datetime(2022, 7, 2),
        )
        with self.assertRaises(Exception) as e:
            operator.execute(Context())
        self.assertIn("Final job state", str(e.exception))

    @patch('apache_airflow_providers_tecton.operators.tecton_job_operator.TectonHook.create')
    def test_on_kill(self, mock_create):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.cancel_materialization_job.return_value = True
        operator = TectonJobOperator(
            task_id="abc",
            workspace='prod',
            feature_view='fv',
            online=True,
            offline=True,
            start_time=datetime.datetime(2022, 7, 1),
            end_time=datetime.datetime(2022, 7, 2),
        )
        operator.on_kill()
        self.assertEquals(0, mock_hook.cancel_job.call_count)
        operator.job_id = "abc"
        operator.on_kill()
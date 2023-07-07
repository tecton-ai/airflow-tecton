# Copyright 2022 Tecton, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from datetime import datetime
import json
import pandas as pd
import requests
import unittest
from unittest.mock import patch, MagicMock, Mock

from airflow.utils.context import Context
from airflow_tecton.operators.tecton_job_operator import (
    TectonJobOperator,
)


def mock_requests_put(*args, **kwargs):
    if args[0] == 'upload_url':
        resp = requests.Response()
        resp._content = json.dumps({'success': True}).encode('utf-8')
        resp.status_code = 200
        return resp

    resp = requests.Response()
    resp._content = json.dumps({'success': False}).encode('utf-8')
    resp.status_code = 400
    return resp


class TestTectonJobOperator(unittest.TestCase):
    JOB = {"job": {"id": "abc"}}
    SUCCESS_JOB = {"id": "cba", "state": "success"}
    OTHER_JOB = {"job": {"id": "cba"}}
    OTHER_JOB_TO_CANCEL = {"id": "cba", "state": "running"}
    OTHER_JOB_TO_CANCEL_CANCELLED = {"id": "cba", "state": "manually_cancelled"}
    GET_JOB_RUNNING_NO_ATTEMPT = {"job": {"id": "abc", "state": "RUNNING"}}
    GET_JOB_RUNNING = {
        "job": {
            "id": "abc",
            "state": "RUNNING",
            "attempts": [{"state": "RUNNING", "run_url": "example.com"}],
        }
    }
    FIND_JOB_SUCCESS = {
        "id": "abc",
        "state": "SUCCESS",
        "attempts": [{"state": "SUCCESS", "run_url": "example.com"}],
    }
    GET_JOB_SUCCESS = {"job": FIND_JOB_SUCCESS}
    GET_OTHER_JOB_SUCCESS = {
        "job": {
            "id": "cba",
            "state": "SUCCESS",
            "attempts": [{"state": "SUCCESS", "run_url": "example.com"}],
        }
    }
    GET_JOB_FAILURE_NO_ATTEMPTS = {"job": {"id": "abc", "state": "ERROR"}}
    GET_JOB_FAILURE = {
        "job": {
            "id": "abc",
            "state": "ERROR",
            "attempts": [{"state": "ERROR", "run_url": "example.com"}],
        }
    }

    @patch("time.sleep", return_value=None)
    @patch("airflow_tecton.operators.tecton_job_operator.TectonHook.create")
    def test_execute(self, mock_create, mock_sleep):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.get_materialization_job.side_effect = [
            self.GET_JOB_RUNNING_NO_ATTEMPT,
            self.GET_JOB_RUNNING,
            self.GET_JOB_SUCCESS,
        ]
        mock_hook.find_materialization_job.return_value = None
        mock_hook.submit_materialization_job.return_value = self.JOB

        operator = TectonJobOperator(
            task_id="abc",
            workspace="prod",
            feature_view="fv",
            online=True,
            offline=True,
            start_time=datetime(2022, 7, 1),
            end_time=datetime(2022, 7, 2),
        )
        operator.execute(None)

    @patch("time.sleep", return_value=None)
    @patch("airflow_tecton.operators.tecton_job_operator.TectonHook.create")
    def test_execute_cancel_existing(self, mock_create, mock_sleep):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.get_materialization_job.side_effect = [
            {"job": self.OTHER_JOB_TO_CANCEL},
            {"job": self.OTHER_JOB_TO_CANCEL_CANCELLED},
            self.GET_JOB_RUNNING_NO_ATTEMPT,
            self.GET_JOB_RUNNING,
            self.GET_JOB_SUCCESS,
        ]
        mock_hook.find_materialization_job.return_value = self.OTHER_JOB_TO_CANCEL
        mock_hook.cancel_materialization_job.return_value = None
        mock_hook.submit_materialization_job.return_value = self.JOB

        operator = TectonJobOperator(
            task_id="abc",
            workspace="prod",
            feature_view="fv",
            online=True,
            offline=True,
            start_time=datetime(2022, 7, 1),
            end_time=datetime(2022, 7, 2),
        )
        operator.execute(None)
        assert mock_hook.cancel_materialization_job.call_count == 1

    @patch("airflow_tecton.operators.tecton_job_operator.TectonHook.create")
    def test_execute_existing_success(self, mock_create):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.find_materialization_job.return_value = self.FIND_JOB_SUCCESS

        operator = TectonJobOperator(
            task_id="cba",
            workspace="prod",
            feature_view="fv",
            online=True,
            offline=True,
            start_time=datetime(2022, 7, 1),
            end_time=datetime(2022, 7, 2),
        )
        operator.execute(None)
        assert mock_hook.submit_materialization_job.call_count == 0

    @patch("airflow_tecton.operators.tecton_job_operator.TectonHook.create")
    def test_execute_existing_success_allow_overwrite(self, mock_create):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.find_materialization_job.return_value = self.FIND_JOB_SUCCESS
        mock_hook.submit_materialization_job.return_value = self.OTHER_JOB
        mock_hook.get_materialization_job.side_effect = [
            self.GET_OTHER_JOB_SUCCESS,
        ]

        operator = TectonJobOperator(
            task_id="cba",
            workspace="prod",
            feature_view="fv",
            online=True,
            offline=True,
            start_time=datetime(2022, 7, 1),
            end_time=datetime(2022, 7, 2),
            allow_overwrite=True,
        )
        operator.execute(None)
        assert mock_hook.submit_materialization_job.call_count == 1

    @patch("time.sleep", return_value=None)
    @patch("airflow_tecton.operators.tecton_job_operator.TectonHook.create")
    def test_execute_failed(self, mock_create, mock_time):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.get_materialization_job.side_effect = [
            self.GET_JOB_RUNNING_NO_ATTEMPT,
            self.GET_JOB_RUNNING,
            self.GET_JOB_FAILURE,
        ]
        mock_hook.find_materialization_job.return_value = None
        mock_hook.submit_materialization_job.return_value = self.JOB

        operator = TectonJobOperator(
            task_id="abc",
            workspace="prod",
            feature_view="fv",
            online=True,
            offline=True,
            start_time=datetime(2022, 7, 1),
            end_time=datetime(2022, 7, 2),
        )
        with self.assertRaises(Exception) as e:
            operator.execute(None)
        self.assertIn("Final job state", str(e.exception))

    @patch("time.sleep", return_value=None)
    @patch("airflow_tecton.operators.tecton_job_operator.TectonHook.create")
    def test_execute_failed_no_attempts(self, mock_create, mock_time):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.get_materialization_job.side_effect = [
            self.GET_JOB_RUNNING_NO_ATTEMPT,
            self.GET_JOB_RUNNING,
            self.GET_JOB_FAILURE_NO_ATTEMPTS,
        ]
        mock_hook.find_materialization_job.return_value = None
        mock_hook.submit_materialization_job.return_value = self.JOB

        operator = TectonJobOperator(
            task_id="abc",
            workspace="prod",
            feature_view="fv",
            online=True,
            offline=True,
            start_time=datetime(2022, 7, 1),
            end_time=datetime(2022, 7, 2),
        )
        with self.assertRaises(Exception) as e:
            operator.execute(None)
        self.assertIn("Final job state", str(e.exception))

    @patch("airflow_tecton.operators.tecton_job_operator.TectonHook.create")
    def test_on_kill(self, mock_create):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.cancel_materialization_job.return_value = True
        mock_hook.find_materialization_job.return_value = None
        operator = TectonJobOperator(
            task_id="abc",
            workspace="prod",
            feature_view="fv",
            online=True,
            offline=True,
            start_time=datetime(2022, 7, 1),
            end_time=datetime(2022, 7, 2),
        )
        operator.on_kill()
        self.assertEqual(0, mock_hook.cancel_job.call_count)
        operator.job_id = "abc"
        operator.on_kill()

    @patch('requests.put', side_effect=mock_requests_put)
    @patch("airflow_tecton.operators.tecton_trigger_operator.TectonHook.create")
    def test_execute_with_df_generator(self, mock_create, mock_put):
        def df_generator(a, b, c=None, d=None):
            assert a == 1
            assert b == 2
            assert c == 3

            data = {'name': ['Tom', 'Joseph', 'Krish', 'John'], 'age': [20, 21, 19, 18],
                    'ts': [datetime.fromtimestamp(1674819600), datetime.fromtimestamp(1675211580),
                           datetime.fromtimestamp(1674347580), datetime.fromtimestamp(1674725580)]}
            return pd.DataFrame(data)

        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.find_materialization_job.return_value = self.SUCCESS_JOB
        mock_hook.get_materialization_job.return_value = self.GET_OTHER_JOB_SUCCESS
        mock_hook.get_dataframe_info.return_value = {"df_path": "df_path", "signed_url_for_df_upload": "upload_url"}
        mock_hook.ingest_dataframe.return_value = self.JOB

        operator = TectonJobOperator(
            task_id="abc",
            workspace="prod",
            feature_view="fv",
            online=True,
            offline=True,
            start_time=datetime(2022, 7, 1),
            end_time=datetime(2022, 7, 2),
            allow_overwrite=True,
            df_generator=df_generator,
            op_args=[1, 2],
            op_kwargs={'c': 3},
            templates_dict={'c': 4},
        )
        operator.execute(Context())


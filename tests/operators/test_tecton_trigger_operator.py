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
import datetime
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from airflow_tecton.operators.tecton_trigger_operator import (
    TectonTriggerOperator,
)


class TestTectonTriggerOperator(unittest.TestCase):
    JOB = {"job": {"id": "abc"}}
    SUCCESS_JOB = {"id": "cba", "state": "success"}

    @patch("airflow_tecton.operators.tecton_trigger_operator.TectonHook.create")
    def test_execute(self, mock_create):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.submit_materialization_job.return_value = self.JOB
        mock_hook.find_materialization_job.return_value = None

        operator = TectonTriggerOperator(
            task_id="abc",
            workspace="prod",
            feature_view="fv",
            online=True,
            offline=True,
            start_time=datetime.datetime(2022, 7, 1),
            end_time=datetime.datetime(2022, 7, 2),
        )
        self.assertEqual(["abc"], operator.execute(None))

    @patch("airflow_tecton.operators.tecton_trigger_operator.TectonHook.create")
    def test_execute_existing_job(self, mock_create):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.find_materialization_job.return_value = self.SUCCESS_JOB

        operator = TectonTriggerOperator(
            task_id="abc",
            workspace="prod",
            feature_view="fv",
            online=True,
            offline=True,
            start_time=datetime.datetime(2022, 7, 1),
            end_time=datetime.datetime(2022, 7, 2),
        )
        self.assertEqual(["cba"], operator.execute(None))
        assert mock_hook.submit_materialization_job.call_count == 0

    @patch("airflow_tecton.operators.tecton_trigger_operator.TectonHook.create")
    def test_execute_existing_job_with_overwrite(self, mock_create):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.submit_materialization_job.return_value = self.JOB
        mock_hook.find_materialization_job.return_value = self.SUCCESS_JOB

        operator = TectonTriggerOperator(
            task_id="abc",
            workspace="prod",
            feature_view="fv",
            online=True,
            offline=True,
            start_time=datetime.datetime(2022, 7, 1),
            end_time=datetime.datetime(2022, 7, 2),
            allow_overwrite=True,
        )
        self.assertEqual(["abc"], operator.execute(None))
        assert mock_hook.submit_materialization_job.call_count == 1

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
from unittest.mock import patch, MagicMock, Mock

from airflow.utils.context import Context

from apache_airflow_providers_tecton.operators.tecton_trigger_operator import TectonTriggerOperator
from apache_airflow_providers_tecton.sensors.tecton_sensor import TectonSensor


class TestTectonSensor(unittest.TestCase):

    def _make_resp(self, online_time, offline_time):
        return {
            'online_latest_ready_time': online_time,
            'offline_latest_ready_time': offline_time
        }

    @patch('apache_airflow_providers_tecton.sensors.tecton_sensor.TectonHook')
    def test_poke(self, mock_create):
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook

        sensor = TectonSensor(
            task_id='abc',
            workspace='prod',
            feature_view='fv',
            online=True,
            offline=True,
            ready_time=datetime.datetime(2022, 1, 31)
        )
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            None,
            None
        )
        assert not sensor.poke(Context())
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            None,
            datetime.datetime(2022, 1, 31)
        )
        assert not sensor.poke(Context())
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            datetime.datetime(2022, 1, 31),
            datetime.datetime(2022, 1, 31)
        )
        assert sensor.poke(Context())
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            datetime.datetime(2022, 1, 30),
            datetime.datetime(2022, 1, 30)
        )
        assert not sensor.poke(Context())
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            datetime.datetime(2022, 1, 30),
            datetime.datetime(2022, 1, 31)
        )
        assert not sensor.poke(Context())

        sensor = TectonSensor(
            task_id='abc',
            workspace='prod',
            feature_view='fv',
            online=True,
            offline=False,
            ready_time=datetime.datetime(2022, 1, 31)
        )
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            None,
            None
        )
        assert not sensor.poke(Context())
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            datetime.datetime(2022, 1, 30),
            None
        )
        assert not sensor.poke(Context())
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            datetime.datetime(2022, 1, 31),
            None
        )
        assert sensor.poke(Context())
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            datetime.datetime(2022, 1, 31),
            datetime.datetime(2022, 1, 30)
        )
        assert sensor.poke(Context())
        sensor = TectonSensor(
            task_id='abc',
            workspace='prod',
            feature_view='fv',
            online=False,
            offline=True,
            ready_time=datetime.datetime(2022, 1, 31)
        )
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            None,
            None
        )
        assert not sensor.poke(Context())
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            None,
            datetime.datetime(2022, 1, 30),
        )
        assert not sensor.poke(Context())
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            None,
            datetime.datetime(2022, 1, 31),
        )
        assert sensor.poke(Context())
        mock_hook.get_latest_ready_time.return_value = self._make_resp(
            datetime.datetime(2022, 1, 30),
            datetime.datetime(2022, 1, 31),
        )

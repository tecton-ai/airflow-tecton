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

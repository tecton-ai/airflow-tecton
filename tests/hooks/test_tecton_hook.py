import unittest
from datetime import datetime

import requests_mock
from airflow.models import Connection
from airflow.utils.session import provide_session

from apache_airflow_providers_tecton.hooks.tecton_hook import TectonHook, JOBS_API_BASE, SUBMIT_JOB_METHOD, \
    CANCEL_JOB_METHOD, GET_JOB_METHOD, LIST_JOB_METHOD, READINESS_METHOD


class TestTectonHook(unittest.TestCase):
    EXAMPLE_URL = "https://example.tecton.ai"
    EXAMPLE_API_BASE = f"{EXAMPLE_URL}{JOBS_API_BASE}"
    @provide_session
    def setUp(self, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == "tecton_default").first()
        conn.host = self.EXAMPLE_URL
        conn.password = "abc"
        session.commit()

        self.hook = TectonHook()

    JOB_RESP_NO_ATTEMPT = {
        'job': {'created_at': '2022-07-18T21:52:06.947388Z',
                'end_time': '2022-07-11T00:00:00Z',
                'feature_view': 'fv',
                'id': '1bd77a161d034f51a2599b4da9565127',
                'offline': True,
                'online': True,
                'start_time': '2022-07-10T00:00:00Z',
                'state': 'MATERIALIZATION_TASK_STATUS_RUNNING',
                'updated_at': '2022-07-18T21:52:06.947388Z',
                'workspace': 'prod'}
    }

    LIST_JOBS_RESP = {
        'jobs': [JOB_RESP_NO_ATTEMPT]
    }

    GET_READINESS_RESP = {
        'online_latest_ready_time': '2022-07-10T00:00:00Z',
        'offline_latest_ready_time': '2022-07-11T00:00:00Z'
    }

    @requests_mock.mock()
    def test_error_handling(self, m):
        m.post(f"{self.EXAMPLE_API_BASE}/{SUBMIT_JOB_METHOD}", json={
            'error': 'myerror'
        }, status_code=500)
        with self.assertRaises(Exception) as e:
            self.hook.submit_materialization_job(
                "prod",
                "fv",
                "2022-07-01T00:00:00+00:00",
                "2022-07-02T00:00:00+00:00",
                online=True,
                offline=True,
                allow_overwrite=False,
                tecton_managed_retries=True
            )
        assert 'myerror' in str(e.exception)

    @requests_mock.mock()
    def test_submit(self, m):
        adapter = m.post(f"{self.EXAMPLE_API_BASE}/{SUBMIT_JOB_METHOD}", json=self.JOB_RESP_NO_ATTEMPT)

        # test supplying datetime
        assert self.JOB_RESP_NO_ATTEMPT == self.hook.submit_materialization_job(
            "prod",
            "fv",
            datetime(2022, 7, 1, 0, 0, 0),
            datetime(2022, 7, 2, 0, 0, 0),
            online=True,
            offline=True,
            allow_overwrite=False,
            tecton_managed_retries=True
        )
        assert adapter.call_count == 1

        assert adapter.last_request.json() == {
            'workspace': "prod",
            'feature_view': "fv",
            'start_time': "2022-07-01T00:00:00Z",
            'end_time': "2022-07-02T00:00:00Z",
            'online': True,
            'offline': True,
            'overwrite': False,
            'use_tecton_managed_retries': True,
        }

        # test supplying isoformat datetime
        assert self.JOB_RESP_NO_ATTEMPT == self.hook.submit_materialization_job(
            "prod",
            "fv",
            "2022-07-01T00:00:00+00:00",
            "2022-07-02T00:00:00+00:00",
            online=True,
            offline=True,
            allow_overwrite=False,
            tecton_managed_retries=True
        )
        assert adapter.call_count == 2

        assert adapter.last_request.json() == {
            'workspace': "prod",
            'feature_view': "fv",
            'start_time': "2022-07-01T00:00:00Z",
            'end_time': "2022-07-02T00:00:00Z",
            'online': True,
            'offline': True,
            'overwrite': False,
            'use_tecton_managed_retries': True,
        }

    @requests_mock.mock()
    def test_cancel_job(self, m):
        adapter = m.post(f"{self.EXAMPLE_API_BASE}/{CANCEL_JOB_METHOD}", json=self.JOB_RESP_NO_ATTEMPT)

        assert self.JOB_RESP_NO_ATTEMPT == self.hook.cancel_materialization_job("abc")
        assert adapter.call_count == 1
        assert adapter.last_request.json() == {
            "job_id": "abc"
        }

    @requests_mock.mock()
    def test_get_job(self, m):
        adapter = m.post(f"{self.EXAMPLE_API_BASE}/{GET_JOB_METHOD}", json=self.JOB_RESP_NO_ATTEMPT)

        assert self.JOB_RESP_NO_ATTEMPT == self.hook.get_materialization_job("abc")
        assert adapter.call_count == 1
        assert adapter.last_request.json() == {
            "job_id": "abc"
        }

    @requests_mock.mock()
    def test_list_jobs(self, m):
        adapter = m.post(f"{self.EXAMPLE_API_BASE}/{LIST_JOB_METHOD}", json=self.LIST_JOBS_RESP)

        assert self.LIST_JOBS_RESP == self.hook.list_materialization_jobs("prod", "fv")
        assert adapter.call_count == 1
        assert adapter.last_request.json() == {
            "workspace": "prod",
            "feature_view": "fv"
        }

    @requests_mock.mock()
    def test_get_readiness(self, m):
        adapter = m.post(f"{self.EXAMPLE_API_BASE}/{READINESS_METHOD}", json=self.GET_READINESS_RESP)

        unwrapped_response = {
            'online_latest_ready_time': datetime(2022, 7, 10),
            'offline_latest_ready_time': datetime(2022, 7, 11)
        }
        assert unwrapped_response == self.hook.get_latest_ready_time('prod', 'fv')
        assert adapter.call_count == 1
        assert adapter.last_request.json() == {
            "workspace": "prod",
            "feature_view": "fv"
        }

        adapter = m.post(f"{self.EXAMPLE_API_BASE}/{READINESS_METHOD}", json={})
        unwrapped_response = {
            'online_latest_ready_time': None,
            'offline_latest_ready_time': None
        }
        assert unwrapped_response == self.hook.get_latest_ready_time('prod', 'fv')
        assert adapter.call_count == 1
        assert adapter.last_request.json() == {
            "workspace": "prod",
            "feature_view": "fv"
        }

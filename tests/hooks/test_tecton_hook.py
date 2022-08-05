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
import unittest
from datetime import datetime

import pytz
import requests_mock
from airflow.models import Connection
from airflow.utils.session import provide_session

from airflow_tecton.hooks.tecton_hook import CANCEL_JOB_METHOD
from airflow_tecton.hooks.tecton_hook import GET_JOB_METHOD
from airflow_tecton.hooks.tecton_hook import JOBS_API_BASE
from airflow_tecton.hooks.tecton_hook import LIST_JOB_METHOD
from airflow_tecton.hooks.tecton_hook import READINESS_METHOD
from airflow_tecton.hooks.tecton_hook import SUBMIT_JOB_METHOD
from airflow_tecton.hooks.tecton_hook import TectonHook


class TestTectonHook(unittest.TestCase):
    EXAMPLE_URL = "https://example.tecton.ai"
    EXAMPLE_API_BASE = f"{EXAMPLE_URL}{JOBS_API_BASE}"

    @provide_session
    def setUp(self, session=None):
        conn = (
            session.query(Connection)
            .filter(Connection.conn_id == "tecton_default")
            .first()
        )
        conn.host = self.EXAMPLE_URL
        conn.password = "abc"
        session.commit()

        self.hook = TectonHook()

    JOB_RESP_NO_ATTEMPT = {
        "job": {
            "created_at": "2022-07-18T21:52:06.947388Z",
            "end_time": "2022-07-11T00:00:00Z",
            "feature_view": "fv",
            "id": "1bd77a161d034f51a2599b4da9565127",
            "offline": True,
            "online": True,
            "job_type": "batch",
            "start_time": "2022-07-10T00:00:00Z",
            "state": "MATERIALIZATION_TASK_STATUS_RUNNING",
            "updated_at": "2022-07-18T21:52:06.947388Z",
            "workspace": "prod",
        }
    }
    JOB_RESP_NO_ATTEMPT_LATER = {
        "job": {
            "created_at": "2022-07-20T22:52:06.947388Z",
            "end_time": "2022-07-11T00:00:00Z",
            "feature_view": "fv",
            "id": "abc",
            "offline": True,
            "online": True,
            "job_type": "batch",
            "start_time": "2022-07-10T00:00:00Z",
            "state": "MATERIALIZATION_TASK_STATUS_RUNNING",
            "updated_at": "2022-07-18T21:52:06.947388Z",
            "workspace": "prod",
        }
    }
    JOB_RESP_NO_ATTEMPT_LATER_NO_MATCH = {
        "job": {
            "created_at": "2022-07-21T23:52:06.947388Z",
            "end_time": "2022-07-11T00:00:00Z",
            "feature_view": "fv",
            "id": "cba",
            "offline": False,
            "online": True,
            "job_type": "batch",
            "start_time": "2022-07-10T00:00:00Z",
            "state": "MATERIALIZATION_TASK_STATUS_RUNNING",
            "updated_at": "2022-07-18T21:52:06.947388Z",
            "workspace": "prod",
        }
    }

    LIST_JOBS_RESP = {"jobs": [JOB_RESP_NO_ATTEMPT]}

    LIST_JOBS_RESP_MULTIPLE_JOBS = {
        "jobs": [
            JOB_RESP_NO_ATTEMPT,
            JOB_RESP_NO_ATTEMPT_LATER_NO_MATCH,
            JOB_RESP_NO_ATTEMPT_LATER,
        ]
    }

    GET_READINESS_RESP = {
        "online_latest_ready_time": "2022-07-10T00:00:00Z",
        "offline_latest_ready_time": "2022-07-11T00:00:00Z",
    }

    @requests_mock.mock()
    def test_error_handling(self, m):
        m.post(
            f"{self.EXAMPLE_API_BASE}/{SUBMIT_JOB_METHOD}",
            json={"error": "myerror"},
            status_code=500,
        )
        with self.assertRaises(Exception) as e:
            self.hook.submit_materialization_job(
                "prod",
                "fv",
                "2022-07-01T00:00:00+00:00",
                "2022-07-02T00:00:00+00:00",
                online=True,
                offline=True,
                allow_overwrite=False,
                tecton_managed_retries=True,
            )
        assert "myerror" in str(e.exception)

    @requests_mock.mock()
    def test_submit(self, m):
        adapter = m.post(
            f"{self.EXAMPLE_API_BASE}/{SUBMIT_JOB_METHOD}",
            json=self.JOB_RESP_NO_ATTEMPT,
        )

        # test supplying datetime
        assert self.JOB_RESP_NO_ATTEMPT == self.hook.submit_materialization_job(
            "prod",
            "fv",
            datetime(2022, 7, 1, 0, 0, 0),
            datetime(2022, 7, 2, 0, 0, 0),
            online=True,
            offline=True,
            allow_overwrite=False,
            tecton_managed_retries=True,
        )
        assert adapter.call_count == 1

        assert adapter.last_request.json() == {
            "workspace": "prod",
            "feature_view": "fv",
            "start_time": "2022-07-01T00:00:00Z",
            "end_time": "2022-07-02T00:00:00Z",
            "online": True,
            "offline": True,
            "overwrite": False,
            "use_tecton_managed_retries": True,
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
            tecton_managed_retries=True,
        )
        assert adapter.call_count == 2

        assert adapter.last_request.json() == {
            "workspace": "prod",
            "feature_view": "fv",
            "start_time": "2022-07-01T00:00:00Z",
            "end_time": "2022-07-02T00:00:00Z",
            "online": True,
            "offline": True,
            "overwrite": False,
            "use_tecton_managed_retries": True,
        }

    @requests_mock.mock()
    def test_cancel_job(self, m):
        adapter = m.post(
            f"{self.EXAMPLE_API_BASE}/{CANCEL_JOB_METHOD}",
            json=self.JOB_RESP_NO_ATTEMPT,
        )

        assert self.JOB_RESP_NO_ATTEMPT == self.hook.cancel_materialization_job(
            "prod", "fv", "abc"
        )
        assert adapter.call_count == 1
        assert adapter.last_request.json() == {
            "workspace": "prod",
            "feature_view": "fv",
            "job_id": "abc",
        }

    @requests_mock.mock()
    def test_get_job(self, m):
        adapter = m.post(
            f"{self.EXAMPLE_API_BASE}/{GET_JOB_METHOD}", json=self.JOB_RESP_NO_ATTEMPT
        )

        assert self.JOB_RESP_NO_ATTEMPT == self.hook.get_materialization_job(
            "prod", "fv", "abc"
        )
        assert adapter.call_count == 1
        assert adapter.last_request.json() == {
            "workspace": "prod",
            "feature_view": "fv",
            "job_id": "abc",
        }

    @requests_mock.mock()
    def test_list_jobs(self, m):
        adapter = m.post(
            f"{self.EXAMPLE_API_BASE}/{LIST_JOB_METHOD}", json=self.LIST_JOBS_RESP
        )

        assert self.LIST_JOBS_RESP == self.hook.list_materialization_jobs("prod", "fv")
        assert adapter.call_count == 1
        assert adapter.last_request.json() == {
            "workspace": "prod",
            "feature_view": "fv",
        }

    @requests_mock.mock()
    def test_get_readiness(self, m):
        adapter = m.post(
            f"{self.EXAMPLE_API_BASE}/{READINESS_METHOD}", json=self.GET_READINESS_RESP
        )

        unwrapped_response = {
            "online_latest_ready_time": datetime(2022, 7, 10, tzinfo=pytz.utc),
            "offline_latest_ready_time": datetime(2022, 7, 11, tzinfo=pytz.utc),
        }
        assert unwrapped_response == self.hook.get_latest_ready_time("prod", "fv")
        assert adapter.call_count == 1
        assert adapter.last_request.json() == {
            "workspace": "prod",
            "feature_view": "fv",
        }

        adapter = m.post(f"{self.EXAMPLE_API_BASE}/{READINESS_METHOD}", json={})
        unwrapped_response = {
            "online_latest_ready_time": None,
            "offline_latest_ready_time": None,
        }
        assert unwrapped_response == self.hook.get_latest_ready_time("prod", "fv")
        assert adapter.call_count == 1
        assert adapter.last_request.json() == {
            "workspace": "prod",
            "feature_view": "fv",
        }

    @requests_mock.mock()
    def test_find_materialization_job(self, m):
        adapter = m.post(
            f"{self.EXAMPLE_API_BASE}/{LIST_JOB_METHOD}",
            json={
                "jobs": [x["job"] for x in self.LIST_JOBS_RESP_MULTIPLE_JOBS["jobs"]]
            },
        )
        assert (
            "abc"
            == self.hook.find_materialization_job(
                "prod",
                "fv",
                online=True,
                offline=True,
                start_time="2022-07-10T00:00:00Z",
                end_time="2022-07-11T00:00:00Z",
            )["id"]
        )
        assert (
            self.hook.find_materialization_job(
                "prod",
                "fv",
                online=True,
                offline=True,
                start_time="2022-07-10T00:00:00Z",
                end_time="2022-07-12T00:00:00Z",
            )
            is None
        )
        assert adapter.call_count == 2

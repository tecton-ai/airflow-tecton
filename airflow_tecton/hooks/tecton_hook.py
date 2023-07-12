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
import json
import logging
import pprint
import sys
import urllib
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

import pytz
import requests
from airflow.hooks.base import BaseHook

JOBS_API_BASE = "/api/v1/jobs"
SUBMIT_JOB_METHOD = "submit-materialization-job"
GET_JOB_METHOD = "get-materialization-job"
CANCEL_JOB_METHOD = "cancel-materialization-job"
LIST_JOB_METHOD = "list-materialization-jobs"
READINESS_METHOD = "get-latest-ready-time"
INGEST_DATAFRAME = "ingest-dataframe"
GET_DATAFRAME_INFO = "get-dataframe-info"


class TectonHook(BaseHook):
    """
    Interact with Tecton's API. Currently supported are:
        get_materialization_job
        list_materialization_jobs
        submit_materialization_job
        cancel_materialization_job
        get_latest_ready_time

    To add Connection:
        Put `https://your-tecton-url.tecton.ai` as host in the Connections page
        Put your Tecton API key in the password field
        The default connection is `tecton_default`
    """

    conn_name_attr = "tecton_conn_id"
    default_conn_name = "tecton_default"
    conn_type = "tecton"
    hook_name = "Tecton"

    def __init__(self, conn_id: str = default_conn_name):
        super().__init__()
        self.conn = self.get_connection(conn_id)
        self._session = None

    def get_conn(self) -> requests.Session:
        # TODO: configure retries here
        if self._session is None:
            session = requests.Session()

            conn = self.conn

            if conn.host and conn.host.startswith("http"):
                self.base_url = conn.host
            else:
                host = "https://" + conn.host
                self.base_url = host

            session.headers.update({"Authorization": f"Tecton-key {conn.password}"})
            session.headers.update({"Content-type": "application/json"})
            self._session = session

        return self._session

    def _pformat_dict(self, d):
        if sys.version_info >= (3, 8):
            # this flag does not exist in earlier python versions
            return pprint.pformat(d, sort_dicts=False)
        else:
            return pprint.pformat(d)

    def _make_request(
        self,
        conn: requests.Session,
        url: str,
        data: Dict[str, Any],
        verbose: bool = False,
    ) -> Dict[str, Any]:
        full_path = urllib.parse.urljoin(self.base_url, url)
        if verbose:
            logging.info(
                f"Making POST request to {full_path} with body=\n{self._pformat_dict(data)}"
            )
        resp = conn.post(full_path, json.dumps(data))
        try:
            resp.raise_for_status()
        except Exception as e:
            exc = e
            if "error" in resp.json():
                raise Exception(f"Tecton error: {resp.json()['error']}")
            raise e
        else:
            exc = None
        finally:
            if verbose or exc:
                logging.info(
                    f"Response: Code={resp.status_code} Body=\n{self._pformat_dict(resp.json())}"
                )
        return resp.json()

    def _canonicalize_datetime(self, dt) -> str:
        if isinstance(dt, str):
            try:
                return datetime.datetime.fromisoformat(dt).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            except:
                return self._parse_time(dt).strftime("%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(dt, datetime.datetime):
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            raise Exception("unexpected type for datetime: " + str(type(dt)))

    def _parse_time(self, dt):
        try:
            return datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=pytz.UTC
            )
        except:
            return datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=pytz.UTC
            )

    def find_materialization_job(
        self,
        workspace: str,
        feature_view: str,
        start_time: Union[datetime.datetime, str],
        end_time: Union[datetime.datetime, str],
        online: bool,
        offline: bool,
        job_type: str ='batch',
    ):
        jobs = [
            x
            for x in self.list_materialization_jobs(
                workspace=workspace, feature_view=feature_view
            ).get("jobs", [])
        ]
        for job in sorted(
            jobs, reverse=True, key=lambda x: self._parse_time(x["created_at"])
        ):
            self.log.info('job = %s' % job)
            if job_type.lower() == job.get("job_type", "").lower():
                if (
                    job["online"] == online
                    and job["offline"] == offline
                    and (job["start_time"] == self._canonicalize_datetime(start_time))
                    and (job["end_time"] == self._canonicalize_datetime(end_time))
                ):
                    return job
        return None

    def submit_materialization_job(
        self,
        workspace: str,
        feature_view: str,
        start_time: Union[datetime.datetime, str],
        end_time: Union[datetime.datetime, str],
        online: bool,
        offline: bool,
        allow_overwrite: bool = False,
        tecton_managed_retries: bool = True,
    ) -> Dict:
        """
        Calls the /jobs/submit-materialization-job API. This submits a Tecton materialization job for a FeatureView.

        :param workspace: Workspace of FeatureView
        :param feature_view: FeatureView name
        :param start_time: Start of time range for materialization job
        :param end_time: End of time range for materialization job
        :param online: Whether job writes to online store
        :param offline: Whether job writes to offline store
        :param allow_overwrite: Whether the job should be able to overwrite an existing, successful job. Note that this can cause inconsistencies if the underlying data has changed.
        :param tecton_managed_retries: Whether the job should be retried by Tecton automatically. Set to `False` if you want to control and submit retries manually.
        :return: The response from the Tecton API. Throws an exception if request was not successful.
        """
        data = {
            "workspace": workspace,
            "feature_view": feature_view,
            "start_time": self._canonicalize_datetime(start_time),
            "end_time": self._canonicalize_datetime(end_time),
            "online": online,
            "offline": offline,
            "overwrite": allow_overwrite,
            "use_tecton_managed_retries": tecton_managed_retries,
        }
        return self._make_request(
            self.get_conn(), f"{JOBS_API_BASE}/{SUBMIT_JOB_METHOD}", data, verbose=True
        )

    def list_materialization_jobs(self, workspace: str, feature_view: str) -> Dict:
        """
        Lists all materialization jobs for a Feature View.
        :param workspace: Workspace name
        :param feature_view: Feature View name
        :return: A dict which contains a list of jobs.
        """
        data = {
            "workspace": workspace,
            "feature_view": feature_view,
        }
        return self._make_request(
            self.get_conn(), f"{JOBS_API_BASE}/{LIST_JOB_METHOD}", data
        )

    def get_materialization_job(
        self, workspace: str, feature_view: str, job_id: str
    ) -> Dict:
        """
        Gets a job
        :param workspace: Workspace name
        :param feature_view: Feature View name
        :param job_id: Job ID
        :return:
        """
        data = {"workspace": workspace, "feature_view": feature_view, "job_id": job_id}
        return self._make_request(
            self.get_conn(), f"{JOBS_API_BASE}/{GET_JOB_METHOD}", data
        )

    def cancel_materialization_job(
        self, workspace: str, feature_view: str, job_id: str
    ) -> Dict:
        """
        Cancels a running materialization job

        :param workspace: Workspace name
        :param feature_view: Feature View name
        :param job_id:  Job ID
        :return:
        """
        data = {"workspace": workspace, "feature_view": feature_view, "job_id": job_id}
        return self._make_request(
            self.get_conn(), f"{JOBS_API_BASE}/{CANCEL_JOB_METHOD}", data, verbose=True
        )

    def get_latest_ready_time(
        self,
        workspace: str,
        feature_view: Optional[str] = None,
        feature_service: Optional[str] = None,
    ) -> Dict:
        """

        :param workspace: workspace name
        :param feature_view: [optional] feature view name: mutually exclusive with feature_service
        :param feature_service: [optional] feature service name: mutually exclusive with feature_view
        :return:
        """
        assert (feature_service is None) != (
            feature_view is None
        ), "Exactly one of feature_view or feature_service should be set"
        data = {
            "workspace": workspace,
        }
        if feature_view:
            data["feature_view"] = feature_view
        if feature_service:
            data["feature_service"] = feature_service
        result = self._make_request(
            self.get_conn(), f"{JOBS_API_BASE}/{READINESS_METHOD}", data
        )
        online_key = "online_latest_ready_time"
        offline_key = "offline_latest_ready_time"
        if online_key not in result:
            result[online_key] = None
        elif result[online_key]:
            result[online_key] = self._parse_time(result[online_key])

        if offline_key not in result:
            result[offline_key] = None
        elif result[offline_key]:
            result[offline_key] = self._parse_time(result[offline_key])

        return result

    def get_dataframe_info(
        self, feature_view: str, workspace: str
    ):
        """
        Get ingest data frame information

        :param feature_view: feature view name
        :param workspace:  workspace name
        :return:
        """
        data = {"feature_view": feature_view, "workspace": workspace}
        return self._make_request(
            self.get_conn(), f"{JOBS_API_BASE}/{GET_DATAFRAME_INFO}", data, verbose=True
        )

    def ingest_dataframe(
        self, feature_view: str, df_path: str, workspace: str
    ):
        """
        Ingest data frame to FeatureTable from s3 path

        :param feature_view: feature view name
        :param df_path: s3 path containing pandas dataframe data
        :param workspace:  workspace name
        :return:
        """
        data = {"feature_view": feature_view, "df_path": df_path, "workspace": workspace}
        return self._make_request(
            self.get_conn(), f"{JOBS_API_BASE}/{INGEST_DATAFRAME}", data, verbose=True
        )

    @classmethod
    def create(cls, conn_id: str):
        return cls(conn_id)

    @classmethod
    def get_ui_field_behaviour(cls) -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["port", "schema", "login", "extra"],
            "placeholders": {
                "password": "Output of `tecton api-key create`",
                "host": "https://example.tecton.ai",
            },
            "relabeling": {
                "password": "API Key",
            },
        }

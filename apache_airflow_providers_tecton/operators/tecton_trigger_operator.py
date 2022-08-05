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
import logging
import pprint
from typing import List
from typing import Sequence
from typing import Union

from airflow.models import BaseOperator
from airflow.utils.context import Context

from apache_airflow_providers_tecton.hooks.tecton_hook import TectonHook


class TectonTriggerOperator(BaseOperator):
    """
    An Airflow operator that kicks off a Tecton job, and does not wait
    for its completion. If the latest job with the same params is in
    the running or success state, this operator will do nothing.

    Note that this will use Tecton managed retries.

    Use this if you have unpredictably arriving data but want Tecton to manage retries of jobs.
    """

    template_fields: Sequence[str] = ("start_time", "end_time")

    def __init__(
        self,
        *,
        conn_id: str = "tecton_default",
        workspace: str,
        feature_view: str,
        online: bool,
        offline: bool,
        start_time: Union[str, datetime.datetime] = "{{ data_interval_start }}",
        end_time: Union[str, datetime.datetime] = "{{ data_interval_end }}",
        **kwargs,
    ):
        """

        :param conn_id: Airflow connection ID for Tecton connection
        :param workspace: Workspace of FeatureView
        :param feature_view: FeatureView name
        :param start_time: Start of time range for materialization job
        :param end_time: End of time range for materialization job
        :param online: Whether job writes to online store
        :param offline: Whether job writes to offline store
        :param kwargs: Airflow base kwargs passed to BaseOperator
        """
        super().__init__(**kwargs)
        self.workspace = workspace
        self.feature_view = feature_view
        self.online = online
        self.offline = offline
        self.start_time = start_time
        self.end_time = end_time
        self.conn_id = conn_id

    def execute(self, context: Context) -> List[str]:
        hook = TectonHook.create(self.conn_id)

        job = hook.find_materialization_job(
            workspace=self.workspace,
            feature_view=self.feature_view,
            online=self.online,
            offline=self.offline,
            start_time=self.start_time,
            end_time=self.end_time,
        )
        if job:
            logging.info(f"Existing job found: {pprint.pformat(job)}")
            if job["state"].lower().endswith("running") or job[
                "state"
            ].lower().endswith("success"):
                logging.info(f"Job in {job['state']} state, so not triggering job")
                return [job["id"]]
            else:
                logging.info(f"Job in {job['state']} state; triggering new job")

        resp = hook.submit_materialization_job(
            workspace=self.workspace,
            feature_view=self.feature_view,
            online=self.online,
            offline=self.offline,
            start_time=self.start_time,
            end_time=self.end_time,
            allow_overwrite=False,
            tecton_managed_retries=True,
        )
        job_id = resp["job"]["id"]
        logging.info(f"Launched job with id {job_id}")
        return [job_id]

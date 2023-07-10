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
from typing import Any, Sequence, Union

from airflow.models import BaseOperator

from airflow_tecton.hooks.tecton_hook import TectonHook
from airflow_tecton.operators.job_utils import is_job_finished, check_job_status, kill_job


class TectonJobOperator(BaseOperator):
    """
    An Airflow operator that launches a Tecton job, and waits for its completion. If the latest job with the same params
    is running, it will cancel it first. If the latest job with the same params is successful, this will return without
    running a new job.

    Use this if you want to submit a Tecton job via Airflow and control retries via Airflow. Each attempt of this operator creates 1 job.
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
        allow_overwrite: bool = False,
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
        :param allow_overwrite: Whether jobs are able to run materialization for periods that previously have materialized data. Note that this can cause inconsistencies if the underlying data has changed.
        :param kwargs: Airflow base kwargs passed to BaseOperator
        """
        super().__init__(**kwargs)
        self.workspace = workspace
        self.feature_view = feature_view
        self.online = online
        self.offline = offline
        self.start_time = start_time
        self.end_time = end_time
        self.allow_overwrite = allow_overwrite
        self.conn_id = conn_id
        self.job_id = None

    def execute(self, context) -> Any:
        hook = TectonHook.create(self.conn_id)

        if is_job_finished(hook, self.workspace, self.feature_view, self.online, self.offline,
                           self.allow_overwrite, self.start_time, self.end_time, "batch"):
            return

        resp = hook.submit_materialization_job(
            workspace=self.workspace,
            feature_view=self.feature_view,
            online=self.online,
            offline=self.offline,
            start_time=self.start_time,
            end_time=self.end_time,
            allow_overwrite=self.allow_overwrite,
            tecton_managed_retries=False,
        )

        self.job_id = resp["job"]["id"]
        check_job_status(hook, self.workspace, self.feature_view, self.job_id)

    def on_kill(self) -> None:
        kill_job(TectonHook.create(self.conn_id), self.workspace, self.feature_view, self.job_id)

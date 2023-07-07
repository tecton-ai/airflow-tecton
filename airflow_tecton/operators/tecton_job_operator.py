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
import pandas
import pprint
import time
from typing import Any, Callable, Collection, Mapping, Sequence, Union

from airflow.models import BaseOperator
from airflow.utils.context import context_merge

from airflow_tecton.hooks.tecton_hook import TectonHook
from airflow_tecton.operators.df_utils import upload_df_pandas
from airflow.utils.operator_helpers import KeywordParameters


class TectonJobOperator(BaseOperator):
    """
    An Airflow operator that launches a Tecton job, and waits for its completion. If the latest job with the same params
    is running, it will cancel it first. If the latest job with the same params is successful, this will return without
    running a new job.

    Use this if you want to submit a Tecton job via Airflow and control retries via Airflow. Each attempt of this operator creates 1 job.
    """

    template_fields: Sequence[str] = ("start_time", "end_time", "templates_dict", "op_args", "op_kwargs")
    template_fields_renderers = {"templates_dict": "json", "op_args": "py", "op_kwargs": "py"}

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
        df_generator: Callable[..., pandas.DataFrame] = None,
        op_args: Union[Collection[Any], None] = None,
        op_kwargs: Union[Mapping[str, Any], None] = None,
        templates_dict: Union[dict, None] = None,
        templates_exts: Union[list[str], None] = None,
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
        :param df_generator: A reference to an object that is callable and returns pandas.DataFrame
        :param op_args: a list of positional arguments that will get unpacked when
            calling df_generator
        :param op_kwargs: a dictionary of keyword arguments that will get unpacked
            in df_generator
        :param templates_dict: a dictionary where the values are templates that
            will get templated by the Airflow engine sometime between
            ``__init__`` and ``execute`` takes place and are made available
            in df generator's context after the template has been applied. (templated)
        :param templates_exts: a list of file extensions to resolve while
            processing templated fields, for examples ``['.sql', '.hql']``
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
        if df_generator and not callable(df_generator):
            raise Exception("`df_generator` param must be callable")
        self.df_generator = df_generator
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}
        self.templates_dict = templates_dict
        if templates_exts:
            self.template_ext = templates_exts

    def execute(self, context) -> Any:
        hook = TectonHook.create(self.conn_id)

        job = hook.find_materialization_job(
            workspace=self.workspace,
            feature_view=self.feature_view,
            online=self.online,
            offline=self.offline,
            start_time=self.start_time,
            end_time=self.end_time,
            job_type="ingest" if self.df_generator else "batch",
        )
        if job:
            logging.info(f"Existing job found: {pprint.pformat(job)}")
            if job["state"].lower().endswith("running"):
                logging.info("Job in running state; cancelling")
                hook.cancel_materialization_job(
                    workspace=self.workspace,
                    feature_view=self.feature_view,
                    job_id=job["id"],
                )
                while (
                    not hook.get_materialization_job(
                        workspace=self.workspace,
                        feature_view=self.feature_view,
                        job_id=job["id"],
                    )["job"]["state"]
                    .lower()
                    .endswith("manually_cancelled")
                ):
                    logging.info(f"waiting for job to enter state manually_cancelled")
                    time.sleep(60)
            elif job["state"].lower().endswith("success"):
                if self.allow_overwrite:
                    logging.info(
                        "Overwriting existing job in success state; (allow_overwrite=True)"
                    )
                else:
                    logging.info("Existing job in success state; exiting")
                    return

        if self.df_generator:
            resp = self.ingest_feature_table_with_pandas_df(hook, context)
        else:
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
        job_result = hook.get_materialization_job(
            self.workspace, self.feature_view, self.job_id
        )["job"]
        while job_result["state"].upper().endswith("RUNNING"):
            if "attempts" in job_result:
                attempts = job_result["attempts"]
                latest_attempt = attempts[-1]
                logging.info(
                    f"Latest attempt #{len(attempts)} in state {latest_attempt['state']} with URL {latest_attempt['run_url']}"
                )
            else:
                logging.info(f"No attempt launched yet")
            time.sleep(60)
            job_result = hook.get_materialization_job(
                self.workspace, self.feature_view, self.job_id
            )["job"]
        if job_result["state"].upper().endswith("SUCCESS"):
            return
        else:
            raise Exception(
                f"Final job state was {job_result['state']}. Final response:\n {job_result}"
            )

    def ingest_feature_table_with_pandas_df(self, hook, context):
        context_merge(context, self.op_kwargs, templates_dict=self.templates_dict)
        self.op_kwargs = KeywordParameters.determine(self.df_generator, self.op_args, context).unpacking()

        df_info = hook.get_dataframe_info(self.feature_view, self.workspace)

        df_path = df_info["df_path"]
        upload_url = df_info["signed_url_for_df_upload"]

        df = self.df_generator(*self.op_args, **self.op_kwargs)
        upload_df_pandas(upload_url, df)

        return hook.ingest_dataframe(self.feature_view, df_path, self.workspace)

    def on_kill(self) -> None:
        if self.job_id:
            logging.info(f"Attempting to kill job {self.job_id}")
            hook = TectonHook.create(self.conn_id)
            hook.cancel_materialization_job(
                self.workspace, self.feature_view, self.job_id
            )
            logging.info(f"Successfully killed job {self.job_id}")
        else:
            logging.debug(f"No job started; none to kill")

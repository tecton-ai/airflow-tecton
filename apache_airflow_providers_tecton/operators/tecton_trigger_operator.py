import datetime
import logging
from typing import Sequence, Union, Any, List

from apache_airflow_providers_tecton.hooks.tecton_hook import TectonHook

from airflow.models import BaseOperator
from airflow.utils.context import Context


class TectonTriggerOperator(BaseOperator):
    """
    An Airflow operator that kicks off a Tecton job, and does not wait
    for its completion.

    Note that this will use Tecton managed retries.

    Use this if you have unpredictably arriving data but want Tecton to manage retries of jobs.
    """
    template_fields: Sequence[str] = (
        "start_time",
        "end_time"
    )

    def __init__(
            self,
            *,
            conn_id: str = "tecton_default",
            workspace: str,
            feature_view: str,
            online: bool,
            offline: bool,
            start_time: Union[str, datetime.datetime] = '{{ data_interval_start }}',
            end_time: Union[str, datetime.datetime] = '{{ data_interval_end }}',
            allow_overwrite=False,
            **kwargs):
        """

        :param conn_id: Airflow connection ID for Tecton connection
        :param workspace: Workspace of FeatureView
        :param feature_view: FeatureView name
        :param start_time: Start of time range for materialization job
        :param end_time: End of time range for materialization job
        :param online: Whether job writes to online store
        :param offline: Whether job writes to offline store
        :param allow_overwrite: Whether the job should be able to overwrite an existing, successful job. Note that this can cause inconsistencies if the underlying data has changed.
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

    def execute(self, context: Context) -> List[str]:
        hook = TectonHook.create(self.conn_id)
        resp = hook.submit_materialization_job(
            workspace=self.workspace,
            feature_view=self.feature_view,
            online=self.online,
            offline=self.offline,
            start_time=self.start_time,
            end_time=self.end_time,
            allow_overwrite=self.allow_overwrite,
            tecton_managed_retries=False
        )
        job_id = resp['job']['id']
        logging.info(f"Launched job with id {job_id}")
        return [job_id]

import datetime
import logging
from typing import Sequence, Union, Optional

from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.context import Context

from apache_airflow_providers_tecton.hooks.tecton_hook import TectonHook


class TectonSensor(BaseSensorOperator):
    """
    Sensor for Tecton FeatureViews or FeatureServices.

    A workspace and exactly one of (FeatureService name, FeatureView name) must be provided.

    This will return "success" when the given FeatureView or FeatureService
    has data materialized from its feature_start_time (for FeatureServices,
    this is the latest for all FeatureViews listed). Specifying `online`
    and `offline` controls which stores to wait for materialization in.
    """
    template_fields: Sequence[str] = (
        'ready_time',
    )

    def __init__(
            self,
            *,
            conn_id: str = "tecton_default",
            workspace: str,
            feature_view: Optional[str] = None,
            feature_service: Optional[str] = None,
            online: bool,
            offline: bool,
            ready_time: Union[str, datetime.datetime] = '{{ data_interval_end }}',
            poke_interval=300,
            **kwargs):
        """

        :param conn_id: Airflow connection ID for Tecton connection
        :param workspace: workspace name
        :param feature_view: [optional] feature view name (mutually exclusive with feature_service)
        :param feature_service: [optional] feature service name (mutually exclusive with feature_view)
        :param online: Whether to wait for data to be materialized in the online store
        :param offline: Whether to wait for data ot be materialized in the offline store.
        :param ready_time: What time to wait for feature data to be ready at. By default, this is the Airflow template variable "data_interval_end"
        :param poke_interval: How often to check Tecton
        :param kwargs: Airflow base kwargs passed to BaseOperator
        """
        super().__init__(**kwargs, mode='reschedule', poke_interval=poke_interval)
        assert (feature_view is None) != (feature_service is None), "Exactly one of feature_view or feature_service must be set"
        assert online or offline, "You must wait for either the online store, offline store, or both. Waiting for neither is a no-op."
        self.conn_id = conn_id
        self.workspace = workspace
        self.feature_view = feature_view
        self.feature_service = feature_service
        self.online = online
        self.offline = offline
        if isinstance(ready_time, str):
            self.ready_time = datetime.datetime.fromisoformat(ready_time)
        else:
            self.ready_time = ready_time

    def poke(self, context: Context) -> Union[bool, PokeReturnValue]:
        hook = TectonHook(self.conn_id)
        if self.feature_view:
            readiness_resp = hook.get_latest_ready_time(self.workspace, feature_view=self.feature_view)
        else:
            readiness_resp = hook.get_latest_ready_time(self.workspace, feature_service=self.feature_service)
        online_ready = self._maybe_check_readiness(readiness_resp, 'online', self.online)
        offline_ready = self._maybe_check_readiness(readiness_resp, 'offline', self.offline)
        return online_ready and offline_ready

    def _maybe_check_readiness(self, resp, store, should_check) -> bool:
        if should_check:
            actual_time = resp[store + "_latest_ready_time"]
            ready = actual_time and actual_time >= self.ready_time
            if ready:
                logging.info(f"{store.capitalize()} store ready!")
            else:
                logging.info(f"{store.capitalize()} store not ready. Expected: {self.ready_time} Actual: {actual_time}")
            return ready
        else:
            return True


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
from datetime import datetime
import json
import pandas as pd
import requests
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from airflow.utils.context import Context

from airflow_tecton.operators.tecton_feature_table_trigger_operator import TectonFeatureTableTriggerOperator
from airflow_tecton.operators.tecton_trigger_operator import (
    TectonTriggerOperator,
)


def mock_requests_put(*args, **kwargs):
    if args[0] == 'upload_url':
        resp = requests.Response()
        resp._content = json.dumps({'success': True}).encode('utf-8')
        resp.status_code = 200
        return resp

    resp = requests.Response()
    resp._content = json.dumps({'success': False}).encode('utf-8')
    resp.status_code = 400
    return resp


class TestTectonFeatureTableTriggerOperator(unittest.TestCase):
    JOB = {"job": {"id": "abc"}}
    SUCCESS_JOB = {"id": "cba", "state": "success"}

    @patch('requests.put', side_effect=mock_requests_put)
    @patch("airflow_tecton.operators.tecton_trigger_operator.TectonHook.create")
    def test_execute_with_df_generator(self, mock_create, mock_put):
        def df_generator(a, b, c=None, d=None):
            assert a == 1
            assert b == 2
            assert c == 3

            data = {'name': ['Tom', 'Joseph', 'Krish', 'John'], 'age': [20, 21, 19, 18],
                    'ts': [datetime.fromtimestamp(1674819600), datetime.fromtimestamp(1675211580),
                           datetime.fromtimestamp(1674347580), datetime.fromtimestamp(1674725580)]}
            return pd.DataFrame(data)

        mock_hook = MagicMock()
        mock_create.return_value = mock_hook
        mock_hook.find_materialization_job.return_value = self.SUCCESS_JOB
        mock_hook.get_dataframe_info.return_value = {"df_path": "df_path", "signed_url_for_df_upload": "upload_url"}
        mock_hook.ingest_dataframe.return_value = self.JOB

        operator = TectonFeatureTableTriggerOperator(
            task_id="abc",
            workspace="prod",
            feature_view="fv",
            online=True,
            offline=True,
            df_generator=df_generator,
            op_args=[1, 2],
            op_kwargs={'c': 3},
            templates_dict={'c': 4},
        )
        self.assertEqual(["abc"], operator.execute(Context()))
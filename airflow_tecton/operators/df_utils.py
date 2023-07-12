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
import io
from typing import Callable, Any, Collection, Mapping, Optional

import pandas as pd
import requests

from airflow.utils.context import context_merge, Context
from airflow.utils.operator_helpers import KeywordParameters

from airflow_tecton.hooks.tecton_hook import TectonHook


def upload_df_pandas(upload_url: str, df: pd.DataFrame):
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False)

    # Maximum 1GB per ingestion
    if out_buffer.__sizeof__() > 1_000_000_000:
        raise Exception(f"Pandas dataframe size, {out_buffer.__sizeof__()}, is larger than 1GB")

    r = requests.put(upload_url, data=out_buffer.getvalue())
    if r.status_code != 200:
        raise Exception("Pandas dataframe upload failed with %s" % r.status_code)
    return r.status_code


def ingest_feature_table_with_pandas_df(hook: TectonHook,
                                        workspace: str,
                                        feature_view: str,
                                        context: Context,
                                        df_generator: Callable,
                                        op_args: Optional[Collection[Any]],
                                        op_kwargs: Optional[Mapping[str, Any]],
                                        templates_dict: Optional[dict[str, Any]]):
    context_merge(context, op_kwargs, templates_dict=templates_dict)
    new_op_kwargs = KeywordParameters.determine(df_generator, op_args, context).unpacking()
    op_kwargs.update(new_op_kwargs)

    df_info = hook.get_dataframe_info(feature_view, workspace)

    df_path = df_info["df_path"]
    upload_url = df_info["signed_url_for_df_upload"]

    df = df_generator(*op_args, **op_kwargs)
    upload_df_pandas(upload_url, df)

    return hook.ingest_dataframe(feature_view, df_path, workspace)

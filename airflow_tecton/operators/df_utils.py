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
import pandas as pd
import requests

def upload_df_pandas(upload_url: str, df: pd.DataFrame):
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False)

    # Maximum 1GB per ingestion
    if out_buffer.__sizeof__() > 1_000_000_000:
        raise Exception(f"Pandas dataframe size, {out_buffer.__sizeof__()}, is too large")

    r = requests.put(upload_url, data=out_buffer.getvalue())
    if r.status_code != 200:
        raise Exception("Pandas dataframe upload fails, %s" % r.status_code)
    return r.status_code

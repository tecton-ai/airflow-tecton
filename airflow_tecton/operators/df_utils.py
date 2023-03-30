import io
import pandas as pd
import requests

def upload_df_pandas(upload_url: str, df: pd.DataFrame):
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False)

    # Maximum 1GB per ingestion
    if out_buffer.__sizeof__() > 1_000_000_000:
        raise Exception("Pandas dataframe size is too large")

    r = requests.put(upload_url, data=out_buffer.getvalue())
    if r.status_code != 200:
        raise Exception("Pandas dataframe upload fails, %s" % r.status_code)

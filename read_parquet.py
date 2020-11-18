import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import glob
from s3fs.core import S3FileSystem
import pandas as pd

import s3fs

bucket_prod = 'h24-forecasting-service-prod-workspace'
path_country = 'last-successful-executions/item-rates/intermediate/item-price-history'

def load_parquet_from_local_to_pd_df(_path):
    files = glob.glob(_path)
    print(files)
    status = pd.concat([load_and_filter_file(f) for f in files], ignore_index=True)
    return status

def load_and_filter_file(_path):
    print(f'Executing file {_path}')
    df = pd.read_parquet(_path)
    return df[(df['shop_code'] == 'DE') & (df['item_code'].str.startswith('0'))]

def load_file(_path):
    print(f'Loading file {_path}')
    return pd.read_parquet(_path)

def write_df_to_pq(_df, _path):
    _df.to_parquet(_path)

def load_parquet_from_s3_to_pd_df(_bucket, _path):
    bucket_uri = f's3://{_bucket}/{_path}'
    df = pq.ParquetDataset(bucket_uri, filesystem=s3fs.S3FileSystem(anon=False,
                                                                    default_fill_cache=False)).read().to_pandas()
    return df


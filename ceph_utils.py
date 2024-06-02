import boto3
import pandas as pd
import io

import pyarrow as pa
from pyarrow.parquet import ParquetFile

import warnings
warnings.filterwarnings("ignore")

from config import Config


def get_resource():
    resource = boto3.resource(service_name=Config.service_name, 
                        aws_access_key_id=Config.aws_access_key_id,
                        aws_secret_access_key=Config.aws_secret_access_key,
                        endpoint_url=Config.endpoint_url, 
                        verify=Config.verify)
    return resource


def get_client():
    client = boto3.client(service_name=Config.service_name, 
                        aws_access_key_id=Config.aws_access_key_id,
                        aws_secret_access_key=Config.aws_secret_access_key,
                        endpoint_url=Config.endpoint_url, 
                        verify=Config.verify)
    return client


def print_buckets(resource):
    for bucket in resource.buckets.all():
        print(bucket.name, bucket.creation_date)


def get_bucket(resource, bucket_name):
    return resource.Bucket(bucket_name)


def download_file(bucket, filename, key):
    print(f'Downloading {filename}...')
    bucket.download_file(Filename=filename, Key=key)


def upload_file(bucket, filename, key):
    print(f'Uploading {filename} ...')
    bucket.upload_file(Filename=filename, Key=key)


def delete_file(resource, bucket_name, key):
    print(f'Deleting {key} ...')
    resource.Object(bucket_name, key).delete()


def print_filenames(bucket, prefix):
    for obj in bucket.objects.filter(Prefix=prefix):
        print(obj)


def get_file_size(bucket, prefix):
    total_size = 0

    for obj in bucket.objects.filter(Prefix=prefix):
        total_size = total_size + obj.size

    return total_size


def read_parquet_batch(client, batch_size, bucket, ceph_file_path):
    data = client.get_object(Bucket=bucket, Key=ceph_file_path)
    file_like_obj = io.BytesIO(data['Body'].read())
    pf = ParquetFile(file_like_obj)
    record_batch = pf.iter_batches(batch_size = batch_size)


    for batch in record_batch:
        df = pf.Table.from_batches([batch]).to_pandas()
        yield df


def read_parquet(client, bucket, key, columns=None):
    data = client.get_object(Bucket=bucket, Key=key)
    if columns:
        return pd.read_parquet(io.BytesIO(data['Body'].read()), columns=columns)
    else:
        return pd.read_parquet(io.BytesIO(data['Body'].read()))


def read_csv(client, bucket, key):
    obj = client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'])
    return df


def main():
    print(Config.endpoint_url)
    print(Config.aws_access_key_id)
    print(Config.aws_secret_access_key)

    
if __name__ == "__main__":
    main() 
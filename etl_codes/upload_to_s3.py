from configparser import ConfigParser
import pandas as pd
import os

def upload_nonpartitioned_data(file_path,file_prefix):
    df = pd.read_csv(file_path)
    df.to_parquet(
        file_prefix,
        engine="pyarrow",
        compression="snappy",
        index=False,
        storage_options={"anon": False}
    )

def upload_partitioned_data(file_path, partition_column, file_prefix):
    # Implementation for uploading partitioned data
    print('file_prefix',file_prefix)
    df = pd.read_csv(file_path)
    df[partition_column] = pd.to_datetime(df[partition_column], errors='coerce', infer_datetime_format=True)

    df["year"] = df[partition_column].dt.year
    df["month"] = df[partition_column].dt.month
    df.to_parquet(
        file_prefix,
        engine="pyarrow",
        compression="snappy",
        partition_cols=["year", "month"],
        index=False,
        storage_options={"anon": False}
        )

def upload_data_to_s3():
    config = ConfigParser()
    config.read('config/configs.conf')
    source_data_path = '/opt/airflow/data'
    bucket_name = config.get('aws_upload_bucket','bucket_name')
    prefix = config.get('aws_upload_bucket','prefix')
    
    # List of files to not upload
    files_to_not_upload = config.get('files_to_not_upload','files')
    
    #partition files
    partition_files = config.get('files_to_partition','files')
    # Upload files to S3
    for file in os.listdir(source_data_path):
        if file not in files_to_not_upload:
            dataset_prefix = config.get('dataset_prefix', file)
            
            if file in partition_files:
                file_prefix = f"s3://{bucket_name}/{prefix}{dataset_prefix}/"
                partition_column = config.get('partition_column', dataset_prefix)
                upload_partitioned_data(os.path.join(source_data_path, file), partition_column, file_prefix)
            else:
                file_prefix = f"s3://{bucket_name}/{prefix}{dataset_prefix}/{dataset_prefix}.parquet"
                upload_nonpartitioned_data(os.path.join(source_data_path, file), file_prefix)
        else:
            print(f'Skipped {file} as it is in the exclusion list.')



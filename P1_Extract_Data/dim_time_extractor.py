import os
import boto3
from pathlib import Path

access_key_id = os.environ['ACCESS_KEY_ID']
secret_access_key =  os.environ['SECRET_ACCESS_KEY'] 
s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
bucket_name = os.environ['S3_DATA_LAKEHOUSE_BUCKET_NAME'] 

dim_times_file_path = os.path.join(Path(__file__).parents[1], 'data', 'dimtime.csv')
s3_path_to_upload = 'raw/dimension-time/dimension_time.csv'
print(dim_times_file_path)
with open(dim_times_file_path, "rb") as f:
    print('Uploading dimension time')
    s3.upload_fileobj(f, bucket_name, s3_path_to_upload)
    print('Uploaded dimension time successfully')
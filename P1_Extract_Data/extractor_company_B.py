import boto3
from utils.generate_customer_data import generate_batch_customer_data
import os
import json
access_key_id = os.environ['ACCESS_KEY_ID']
secret_access_key =  os.environ['SECRET_ACCESS_KEY'] 
s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
bucket_name = os.environ['S3_DATA_LAKEHOUSE_BUCKET_NAME'] 

if __name__ == "__main__":
    for data in generate_batch_customer_data(4):
        filename = data.get('date')
        json_data = json.dumps(data)
        key_name = f'raw/companyB/customer/{filename}.json'
        print(f'Writing customer data {key_name}')
        s3.put_object(Body=json_data, Bucket=bucket_name, Key=key_name)
        print(f'Customer data {key_name} written completed')

from kafka import KafkaConsumer
import boto3
import os
from datetime import datetime, timedelta
import json

access_key_id = os.environ['ACCESS_KEY_ID']
secret_access_key =  os.environ['SECRET_ACCESS_KEY'] 
s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
bucket_name = os.environ['S3_DATA_LAKEHOUSE_BUCKET_NAME'] 


def consume_customer_message(message_str):
    now = datetime.utcnow() + timedelta(hours=7)
    now_str = now.isoformat()
    now_str = now_str.replace(':','')
    key_name = f'raw/companyA/customer/{now_str}.json'
    print(f'Uploading file: {key_name}')
    s3.put_object(Body=message_str, Bucket=bucket_name, Key=key_name)
    print(f'Uploaded file {key_name} successfully')

def consume_transaction_message(message_str):
    now = datetime.utcnow() + timedelta(hours=7)
    now_str = now.isoformat()
    now_str = now_str.replace(':','')
    key_name = f'raw/companyA/transactions/{now_str}.json'
    print(f'Uploading file: {key_name}')
    s3.put_object(Body=message_str, Bucket=bucket_name, Key=key_name)
    print(f'Uploaded file {key_name} successfully')

def consume_message(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
    )
    for message_kafka in consumer:
        message_str = message_kafka.value.decode("utf-8")
        message_dict = json.loads(message_str)
        print(message_dict)
        print(type(message_dict))
        data_type = message_dict.get('table')
        if data_type == 'customer':
            consume_customer_message(message_str)
        elif data_type == 'transaction':
            consume_transaction_message(message_str)
        

if __name__ == "__main__":
    topic = "CompanyA-Raw-Data"
    consume_message(topic)
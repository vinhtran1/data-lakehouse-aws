import boto3
from utils.generate_customer_data import generate_batch_customer_data, genrate_transaction_data, get_random_string
import os
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "--num-customers",
    type=int,
    default=1,
    help='number of customers to generate')
parser.add_argument(
    "--num-transactions",
    type=int,
    default=10,
    help='number of transactions to generate')
args = parser.parse_args()

access_key_id = os.environ['ACCESS_KEY_ID']
secret_access_key =  os.environ['SECRET_ACCESS_KEY'] 
s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
bucket_name = os.environ['S3_DATA_LAKEHOUSE_BUCKET_NAME'] 

if __name__ == "__main__":
    lst_customer_id = []
    all_customer_data = generate_batch_customer_data(args.num_customers)
    for data in all_customer_data:
        lst_customer_id_tmp = [t.get('payload').get('id') for t in data.get('value')]
        lst_customer_id.append(lst_customer_id_tmp)
        filename = data.get('date')
        json_data = json.dumps(data)
        key_name = f'raw/companyB/customer/{filename}.json'
        print(f'Writing customer data {key_name}')
        s3.put_object(Body=json_data, Bucket=bucket_name, Key=key_name)
        print(f'Customer data {key_name} written completed')
    
    lst_transaction_data = genrate_transaction_data(lst_customer_id, args.num_transactions, 'companyB')
    chunk_size = 100
    chunks = [lst_transaction_data[x:x+100] for x in range(0, len(lst_transaction_data), 100)]
    for chunk in chunks:
        transaction_dict = {'transactions': chunk}
        json_data = json.dumps(transaction_dict)
        filename = get_random_string(10)
        key_name = f'raw/companyB/transactions/{filename}.json'
        print(f'Writing transaction data {key_name}')
        s3.put_object(Body=json_data, Bucket=bucket_name, Key=key_name)
        print(f'Transaction data {key_name} written completed')


import json
from kafka import KafkaProducer
from utils.generate_customer_data import generate_stream_customer_data, genrate_transaction_data
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

def produce_message(topic, message):
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    message_bytes = json.dumps(message).encode("utf-8")
    future = producer.send(topic, message_bytes)
    try:
        future.get(timeout=10)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    topic = "CompanyA-Raw-Data"
    lst_customer_id = []
    print(args.num_customers)
    print(args.num_transactions)
    raise
    all_customer_data = generate_stream_customer_data(args.num_customers)
    for data in all_customer_data:
        lst_customer_id_tmp = data.get('payload').get('id')
        lst_customer_id.append(lst_customer_id_tmp)
        produce_message(topic, data)
    lst_transaction_data = genrate_transaction_data(lst_customer_id, args.num_transactions, 'companyA')
    for data in lst_transaction_data:
        transaction_dict = {
            'table':'transaction',
            'type':'Insert',
            'payload': data
        }
        produce_message(topic, transaction_dict)
    
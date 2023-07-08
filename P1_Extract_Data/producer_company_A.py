import json
from kafka import KafkaProducer
from utils.generate_customer_data import generate_stream_customer_data
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "--num-records",
    type=int,
    help='number of records to generate')
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
    num_records = args.num_records or 1
    topic = "CompanyA-Raw-Data"
    for data in generate_stream_customer_data(num_records):
        produce_message(topic, data)
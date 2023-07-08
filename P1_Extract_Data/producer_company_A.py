import json
from kafka import KafkaProducer
from utils.generate_customer_data import generate_stream_customer_data

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
    for data in generate_stream_customer_data():
        produce_message(topic, data)
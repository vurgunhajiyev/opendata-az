import json
import time
import requests
from kafka import KafkaProducer

def get_api_data():
    API_URL = "https://dummyjson.com/products/1"
    response = requests.get(API_URL)
    return response.json()

def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    while True:
        data = get_api_data()
        producer.send("products", value=data)
        producer.flush()
        print("Sent:", data)
        time.sleep(2)

if __name__ == "__main__":
    main()

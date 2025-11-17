from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "products",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="payment",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Consumer started... listening for messages...\n")

for msg in consumer:
    print("Received:", msg.value)
    

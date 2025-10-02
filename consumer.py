from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "cellphone-topic",
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    value_deserializer=lambda m: m.decode('utf-8') if m else None
)

print("Listening messages...")
for msg in consumer:
    try:
        data = json.loads(msg.value) if msg.value else None
        print(f"Received: {data}")
    except Exception as e:
        print(f"Skip invalid message: {msg.value}, error: {e}")

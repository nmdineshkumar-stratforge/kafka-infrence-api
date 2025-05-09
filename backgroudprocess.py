from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def send_message(topic: str, msg: dict):
    producer.send(topic, msg)
    producer.flush()
    print(f"[Backend] Sent task {msg.get('task_id')}")

import os
import whisper
import torch
from kafka import KafkaConsumer, KafkaProducer
import json
import threading

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

model = whisper.load_model("base")
device = "cuda" if torch.cuda.is_available() else "cpu"
model = model.to(device)
print(f"[Worker] Using {device.upper()} for transcription")

def process_audio(task):
    task_id = task.get("task_id")
    audio_path = task.get("audio_path")
    print(f"[Worker] Processing task {task_id}: {audio_path}")

    if not os.path.exists(audio_path):
        print(f"[Worker] File not found: {audio_path}")
        result = "[ERROR] File not found"
    else:
        try:
            result = model.transcribe(audio_path, language="en")["text"]
        except Exception as e:
            result = f"[ERROR] Failed: {str(e)}"

    producer.send("result-topic", {
        "task_id": task_id,
        "text": result
    })
    producer.flush()
    print(f"[Worker] Finished task {task_id}")

# Kafka consumer loop
consumer = KafkaConsumer(
    'input-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='worker-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("[Worker] Ready to receive tasks...")

for msg in consumer:
    task = msg.value
    # Spawn a thread for each file
    thread = threading.Thread(target=process_audio, args=(task,))
    thread.start()

from fastapi import FastAPI, UploadFile, File
import os, uuid, shutil, asyncio, threading
from kafka import KafkaConsumer
import json
from backgroudprocess import send_message

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI()
responses = {}  # shared memory dict

def start_kafka_consumer():
    consumer = KafkaConsumer(
        'result-topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='backend-group',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    print("[Kafka] Consumer started for result-topic")
    for msg in consumer:
        result = msg.value
        task_id = result.get("task_id")
        print(f"[Kafka] Got result for task {task_id}: {result}")
        responses[task_id] = result

# Start Kafka consumer in background
threading.Thread(target=start_kafka_consumer, daemon=True).start()

@app.post("/upload-audio")
async def upload_audio(file: UploadFile = File(...)):
    req_id = str(uuid.uuid4())
    filepath = os.path.join(UPLOAD_DIR, f"{req_id}_{file.filename}")
    with open(filepath, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    message = {"task_id": req_id, "audio_path": filepath}
    send_message('input-topic', message)

    # Wait for result (max 10 seconds)
    for _ in range(20):
        if req_id in responses:
            return {"result": responses.pop(req_id)}
        await asyncio.sleep(0.5)

    return {"error": "Timeout waiting for processing"}

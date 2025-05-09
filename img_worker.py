import os
import whisper
import torch
import json
from kafka import KafkaProducer

task_id = os.getenv("TASK_ID")
audio_path = os.getenv("AUDIO_PATH")

model = whisper.load_model("base")
device = "cuda" if torch.cuda.is_available() else "cpu"
model = model.to(device)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

if not os.path.exists(audio_path):
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

from kafka import KafkaConsumer,KafkaProducer
from kubernetes import client, config
import json
import uuid

# Load kubeconfig (Windows with Docker Desktop)
config.load_kube_config()
batch_v1 = client.BatchV1Api()

# 
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# Kafka consumer setup
consumer = KafkaConsumer(
    'input-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='controller-group',
    enable_auto_commit=True
)

print("[Controller] Listening for tasks...")

for msg in consumer:
    task = msg.value
    task_id = task.get("task_id")
    audio_path = task.get("audio_path")
    filename = audio_path.split("/")[-1]

    print(f"[Controller] Received task {task_id}, creating Job for: {filename}")

    # Define unique job name
    job_name = f"whisper-worker-{task_id[:8]}"

    # Define job spec
    job = client.V1Job(
        metadata=client.V1ObjectMeta(name=job_name),
        spec=client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    containers=[
                        client.V1Container(
                            name="worker",
                            image="nmdineshkumar/wishper-worker:0.0.1",
                            env=[
                                client.V1EnvVar(name="TASK_ID", value=task_id),
                                client.V1EnvVar(name="AUDIO_PATH", value=f"/app/uploads/{filename}")
                            ],
                            volume_mounts=[
                                client.V1VolumeMount(
                                    name="audio-storage",
                                    mount_path="/app/uploads"
                                )
                            ]
                        )
                    ],
                    restart_policy="Never",
                    volumes=[
                        client.V1Volume(
                            name="audio-storage",
                            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                claim_name="audio-pvc"
                            )
                        )
                    ]
                )
            ),
            backoff_limit=1
        )
    )

    # Submit the job to Kubernetes
    batch_v1.create_namespaced_job(namespace="default", body=job)
    print(f"[Controller] Job {job_name} created.")
    producer.send("result-topic", {
        "task_id": task_id,
        "audio_path": audio_path
    })

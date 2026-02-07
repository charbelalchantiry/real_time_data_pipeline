import json
import time
from io import BytesIO
from kafka import KafkaConsumer
from minio import Minio
from datetime import datetime, timezone

# Kafka config
KAFKA_TOPIC = "weather_raw"
KAFKA_SERVER = "localhost:9092"

# MinIO config
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "weather-raw"

# Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True
)

# MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Ensure bucket exists
if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

print("Consumer started. Listening to Kafka...")

for message in consumer:
    data = message.value

    # Fix 1: Use timezone-aware UTC datetime (no deprecation warning)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"weather_{timestamp}.json"

    # Fix 2: Wrap bytes in BytesIO (file-like object)
    content = json.dumps(data).encode("utf-8")
    content_stream = BytesIO(content)

    # Upload to MinIO
    minio_client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=filename,
        data=content_stream,       # <-- file-like object, not bytes
        length=len(content),
        content_type="application/json"
    )

    print(f"Saved {filename} to MinIO")
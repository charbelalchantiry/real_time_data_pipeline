import json
from io import BytesIO
from kafka import KafkaConsumer
from minio import Minio
from datetime import datetime, timezone

# Kafka config
KAFKA_TOPIC = "weather_raw"
KAFKA_SERVER = "kafka:9092"

# MinIO config
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
PROCESSED_BUCKET = "weather-processed"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True
)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Ensure processed bucket exists
if not minio_client.bucket_exists(PROCESSED_BUCKET):
    minio_client.make_bucket(PROCESSED_BUCKET)

print("Processor started. Transforming weather data...")

for message in consumer:
    raw = message.value

    # Transform logic
    processed = {
        "city": raw.get("name"),
        "country": raw.get("sys", {}).get("country"),
        "temperature_c": raw.get("main", {}).get("temp"),
        "humidity": raw.get("main", {}).get("humidity"),
        "wind_speed": raw.get("wind", {}).get("speed"),
        "weather_description": raw.get("weather", [{}])[0].get("description"),
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    # Save processed file
    now = datetime.now(timezone.utc)

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    time_part = now.strftime("%H%M%S_%f")

    filename = f"year={year}/month={month}/day={day}/processed_weather_{time_part}.json"

    
    content = json.dumps(processed).encode("utf-8")
    content_stream = BytesIO(content)

    minio_client.put_object(
        bucket_name=PROCESSED_BUCKET,
        object_name=filename,
        data=content_stream,
        length=len(content),
        content_type="application/json"
    )

    print(f"Processed & saved {filename}")

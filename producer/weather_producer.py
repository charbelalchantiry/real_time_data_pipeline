import json
import time
import requests
from kafka import KafkaProducer

API_KEY = "PUT_YOUR_API_KEY_HERE"
CITY = "Beirut"
KAFKA_TOPIC = "weather_raw"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    response = requests.get(
        f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    )
    data = response.json()

    producer.send(KAFKA_TOPIC, data)
    print("Sent:", data["name"], data["main"]["temp"])

    time.sleep(30)

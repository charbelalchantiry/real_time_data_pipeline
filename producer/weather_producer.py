import json
import time
import requests
from kafka import KafkaProducer

API_KEY = "81cfcaf709b50c2093bd784c4bfee544" 
CITY = "Beirut"
KAFKA_TOPIC = "weather_raw"
KAFKA_SERVER = "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"Producer started. Fetching weather for {CITY}...")

while True:
    try:
        response = requests.get(
            f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
        )
        data = response.json()

        # LOGIC: Only send if the request was successful (HTTP 200)
        if response.status_code == 200:
            producer.send(KAFKA_TOPIC, data)
            print(f"Sent: {data['name']} | Temp: {data['main']['temp']}°C")
        else:
            # Print the error so you know about it, but DON'T send to Kafka
            print(f"API Error: {data.get('message')}")

    except Exception as e:
        print(f"Script Error: {e}")

    # Wait 30 seconds before next fetch
    time.sleep(30)
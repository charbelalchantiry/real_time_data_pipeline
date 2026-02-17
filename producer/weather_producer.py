import json
import time
import requests
from kafka import KafkaProducer

API_KEY = "81cfcaf709b50c2093bd784c4bfee544"

CITIES = ["Beirut", "Zahle", "Baalbek"]

KAFKA_TOPIC = "weather_raw"
KAFKA_SERVER = "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"Producer started. Fetching weather for: {', '.join(CITIES)}")

while True:
    for city in CITIES:
        try:
            response = requests.get(
                f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
            )
            data = response.json()

            if response.status_code == 200:
                producer.send(KAFKA_TOPIC, data)
                print(f"Sent: {data['name']} | Temp: {data['main']['temp']}°C")
            else:
                print(f"{city} API Error: {data.get('message')}")

        except Exception as e:
            print(f"{city} Script Error: {e}")

        time.sleep(2)  # small delay between cities

    # Wait 30 seconds before next full cycle
    time.sleep(30)

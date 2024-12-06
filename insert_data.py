import requests
from datetime import datetime, timedelta
import random

base_time = datetime.now()

for i in range(100):
    timestamp = (base_time + timedelta(minutes=i)).strftime('%Y-%m-%d %H:%M:%S')
    station_id = f"Station_{random.randint(1, 5)}"
    metric_name = random.choice(["temperature", "humidity"])
    value = round(random.uniform(15.0, 35.0), 2)
    location = "New York"
    tags = "urban"

    payload = {
        "timestamp": timestamp,
        "station_id": station_id,
        "metric_name": metric_name,
        "value": value,
        "location": location,
        "tags": tags
    }

    try:
        response = requests.post("http://127.0.0.1:5000/insert", json=payload)
        response.raise_for_status()  # Raise an error for bad status codes
        try:
            print(response.json())  # Try to parse the response as JSON
        except requests.exceptions.JSONDecodeError:
            print(f"Response content is not JSON: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
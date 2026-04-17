import argparse
import random
import requests
from datetime import datetime

# Configuration
FASTAPI_URL = "http://localhost:8000/ingest"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer operator"  # Use one of the valid tokens: "operator", "supervisor", or "admin"
}


def generate_random_reading(device_id: str = "line1"):
    return {
        "device_id": device_id,
        "timestamp": datetime.now().isoformat(),
        "temperature": round(random.uniform(18.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "pressure": round(random.uniform(990.0, 1035.0), 2),
        "electronic_signature": f"operator{random.randint(1, 5)}",
    }


def ingest_sensor_data(
    device_id: str = "line1",
    timestamp: str = None,
    temperature: float = 22.5,
    humidity: float = 50.0,
    pressure: float = 1013.0,
    electronic_signature: str = "operator1",
):
    if timestamp is None:
        timestamp = datetime.now().isoformat()

    data = {
        "device_id": device_id,
        "timestamp": timestamp,
        "temperature": temperature,
        "humidity": humidity,
        "pressure": pressure,
        "electronic_signature": electronic_signature,
    }

    try:
        response = requests.post(FASTAPI_URL, json=data, headers=HEADERS)
        print(f"✅ Success! Status code: {response.status_code}")
        try:
            print(f"Response: {response.json()}")
        except ValueError:
            print("Response contained no JSON body.")
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to send data: {e}")


def main():
    parser = argparse.ArgumentParser(description="Ingest sensor data into the FastAPI app.")
    parser.add_argument("--random", action="store_true", help="Send randomized sensor readings.")
    parser.add_argument("--count", type=int, default=1, help="Number of readings to send when using --random.")
    parser.add_argument("--device-id", default="line1", help="Device ID to send.")
    parser.add_argument("--signature", default="operator1", help="Electronic signature value.")
    args = parser.parse_args()

    if args.random:
        for _ in range(args.count):
            reading = generate_random_reading(device_id=args.device_id)
            ingest_sensor_data(**reading)
    else:
        ingest_sensor_data(
            device_id=args.device_id,
            electronic_signature=args.signature,
        )


if __name__ == "__main__":
    main()
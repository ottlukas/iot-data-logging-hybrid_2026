import argparse
import random
import requests
from datetime import datetime

FASTAPI_URL = "http://localhost:8000"


def fetch_access_token(username: str, password: str) -> str:
    response = requests.post(
        f"{FASTAPI_URL}/token",
        data={"username": username, "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    response.raise_for_status()
    return response.json()["access_token"]


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
    token: str,
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

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    try:
        response = requests.post(f"{FASTAPI_URL}/ingest", json=data, headers=headers)
        print(f"✅ Status code: {response.status_code}")
        print(response.json())
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to send data: {e}")


def main():
    parser = argparse.ArgumentParser(description="Ingest sensor data into the FastAPI app.")
    parser.add_argument("--random", action="store_true", help="Send randomized sensor readings.")
    parser.add_argument("--count", type=int, default=1, help="Number of readings to send when using --random.")
    parser.add_argument("--device-id", default="line1", help="Device ID to send.")
    parser.add_argument("--signature", default="operator1", help="Electronic signature value.")
    parser.add_argument("--username", default="operator", help="Login username for API authentication.")
    parser.add_argument("--password", default="operator", help="Login password for API authentication.")
    args = parser.parse_args()

    token = fetch_access_token(args.username, args.password)

    if args.random:
        for _ in range(args.count):
            reading = generate_random_reading(device_id=args.device_id)
            ingest_sensor_data(token, **reading)
    else:
        ingest_sensor_data(
            token,
            device_id=args.device_id,
            electronic_signature=args.signature,
        )


if __name__ == "__main__":
    main()
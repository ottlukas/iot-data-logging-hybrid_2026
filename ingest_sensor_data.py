import argparse
import json
import random
import requests
from datetime import datetime
import sys
from pathlib import Path

try:
    from tsfile import TSFileWriter, TSFileReader, Tablet, TSDataType
except ImportError:
    TSFileWriter = TSFileReader = Tablet = TSDataType = None

TSFILE_INSTALL_URL = "https://tsfile.apache.org/UserGuide/latest/QuickStart/QuickStart-PYTHON.html"
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


def write_direct_to_tsfile(output_path: str, readings: list, device_id: str):
    """Writes readings directly to a local Apache TSFile."""
    if TSFileWriter is None:
        print(f"⚠️ Warning: 'tsfile' library not installed. Falling back to JSON.")
        print(f"👉 Follow instructions at: {TSFILE_INSTALL_URL}")
        
        print(f"📦 Writing {len(readings)} points as JSON to {output_path}...")
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "a", encoding="utf-8") as f:
            for r in readings:
                f.write(json.dumps(r) + "\n")
        return

    print(f"📦 Writing {len(readings)} points to {output_path}...")
    
    # Prepare Tablet data
    measurements = ["temperature", "humidity", "pressure"]
    data_types = [TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT]
    
    timestamps = []
    values = [[], [], []]
    
    for r in readings:
        ts = int(datetime.fromisoformat(r["timestamp"]).timestamp() * 1000)
        timestamps.append(ts)
        values[0].append(float(r["temperature"]))
        values[1].append(float(r["humidity"]))
        values[2].append(float(r["pressure"]))

    device_path = f"root.factory.{device_id}"
    tablet = Tablet(device_path, measurements, data_types, values, timestamps)
    
    writer = TSFileWriter(output_path)
    try:
        writer.write_tablet(tablet)
        print("✅ Successfully wrote local TSFile.")
    finally:
        writer.close()


def verify_tsfile(path: str):
    """Verified the readability of a TSFile and catches errors."""
    if TSFileReader is None:
        print(f"❌ Error: 'tsfile' library not installed. Cannot verify binary files.")
        print(f"👉 Follow instructions at: {TSFILE_INSTALL_URL}")
        return

    if not Path(path).exists():
        print(f"❌ Error: File not found at {path}")
        return

    print(f"🔍 Verifying TSFile: {path}")
    try:
        reader = TSFileReader(path)
        try:
            # In a real scenario, you might discover devices first. 
            # Here we check the standard path.
            device = "root.factory.line1"
            print(f"   Reading device: {device}")
            
            for m in ["temperature", "humidity", "pressure"]:
                query_res = reader.query(device, m, 0, sys.maxsize)
                count = 0
                while query_res.has_next():
                    query_res.next()
                    count += 1
                print(f"   - Measurement '{m}': {count} points found.")
                query_res.close()
            
            print("✅ TSFile is valid and readable.")
        finally:
            reader.close()
    except Exception as e:
        print(f"❌ TSFile verification failed (File may be corrupted): {e}")


def main():
    parser = argparse.ArgumentParser(description="Ingest sensor data into the FastAPI app.")
    parser.add_argument("--random", action="store_true", help="Send randomized sensor readings.")
    parser.add_argument("--count", type=int, default=1, help="Number of readings to send when using --random.")
    parser.add_argument("--device-id", default="line1", help="Device ID to send.")
    parser.add_argument("--local", action="store_true", help="Write directly to a local TSFile instead of the API.")
    parser.add_argument("--output", default="data/tsfiles/manual_ingest.tsfile", help="Local output path for --local.")
    parser.add_argument("--verify", help="Verify the readability of a specific TSFile path.")
    parser.add_argument("--signature", default="operator1", help="Electronic signature value.")
    parser.add_argument("--username", default="operator", help="Login username for API authentication.")
    parser.add_argument("--password", default="operator", help="Login password for API authentication.")
    args = parser.parse_args()

    if args.verify:
        verify_tsfile(args.verify)
        return

    if args.local:
        if TSFileWriter is None:
            print("⚠️ Warning: 'tsfile' package missing. Local mode will use JSON fallback.")
            
        readings = [generate_random_reading(args.device_id) for _ in range(args.count)]
        write_direct_to_tsfile(args.output, readings, args.device_id)
        return

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
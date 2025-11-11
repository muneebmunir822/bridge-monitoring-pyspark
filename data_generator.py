import os
import time
import json
import random
from datetime import datetime, timedelta
import argparse

# --- CLI Arguments ---
parser = argparse.ArgumentParser(description="Bridge Sensor Data Generator")
parser.add_argument("--duration", type=int, default=300, help="Duration to run in seconds")
parser.add_argument("--rate", type=int, default=5, help="Number of events per second")
args = parser.parse_args()

# --- Configuration ---
BRIDGE_IDS = ["B1", "B2", "B3", "B4", "B5"]
SENSOR_TYPES = ["temperature", "vibration", "tilt"]
STREAMS_BASE = "streams"

# Create folders if not exist
for s in SENSOR_TYPES:
    os.makedirs(os.path.join(STREAMS_BASE, f"bridge_{s}"), exist_ok=True)

# --- Helper function ---
def generate_event(bridge_id, sensor_type):
    event_time = datetime.utcnow() - timedelta(seconds=random.randint(0, 60))
    value = {
        "temperature": round(random.uniform(-10, 60), 2),
        "vibration": round(random.uniform(0, 1.5), 3),
        "tilt": round(random.uniform(0, 90), 2)
    }[sensor_type]

    return {
        "event_time": event_time.isoformat(),
        "bridge_id": bridge_id,
        "sensor_type": sensor_type,
        "value": value,
        "ingest_time": datetime.utcnow().isoformat()
    }

# --- Generator Loop ---
print("✅ Starting data generation...")
start_time = time.time()

while time.time() - start_time < args.duration:
    for _ in range(args.rate):
        for bridge_id in BRIDGE_IDS:
            for sensor_type in SENSOR_TYPES:
                event = generate_event(bridge_id, sensor_type)
                folder = os.path.join(STREAMS_BASE, f"bridge_{sensor_type}")
                filename = os.path.join(folder, f"data_{int(time.time())}.json")

                with open(filename, "a") as f:
                    f.write(json.dumps(event) + "\n")

    print(f"🟢 Wrote new events batch at {datetime.utcnow().isoformat()}")
    time.sleep(5)

print("✅ Data generation completed.")

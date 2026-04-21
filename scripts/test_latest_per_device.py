import json
import os
import time
from datetime import datetime, UTC
from confluent_kafka import Consumer, TopicPartition
from dotenv import load_dotenv
from pathlib import Path

# ==============================
# LOAD ENV
# ==============================
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

TOPIC = os.getenv("TOPIC")

# ==============================
# CONFIG
# ==============================
consumer = Consumer({
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
    'group.id': f'test-latest-{int(time.time())}',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("SASL_USERNAME"),
    'sasl.password': os.getenv("SASL_PASSWORD"),
})

# ==============================
# DESCOBRIR PARTIÇÕES
# ==============================
metadata = consumer.list_topics(TOPIC)
partitions = list(metadata.topics[TOPIC].partitions.keys())

print(f"🔎 Partições: {partitions}")

# ==============================
# OFFSET RECENTE
# ==============================
topic_partitions = []

for p in partitions:
    tp = TopicPartition(TOPIC, p)

    low, high = consumer.get_watermark_offsets(tp)

    offset = max(high - 20, low)

    tp.offset = offset
    topic_partitions.append(tp)

consumer.assign(topic_partitions)

print("🚀 Lendo últimos eventos...")

# ==============================
# CAPTURA
# ==============================
latest_per_device = {}

start_time = time.time()

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        if time.time() - start_time > 5:
            break
        continue

    if msg.error():
        continue

    data = json.loads(msg.value().decode())

    device = data["device_id"]
    event_ts = datetime.fromtimestamp(data["timestamp"], UTC)

    # 🔥 agora guardando partição e offset
    record = {
        "event_ts": event_ts,
        "data": data,
        "partition": msg.partition(),
        "offset": msg.offset()
    }

    if device not in latest_per_device:
        latest_per_device[device] = record
    else:
        if event_ts > latest_per_device[device]["event_ts"]:
            latest_per_device[device] = record

consumer.close()

# ==============================
# OUTPUT FINAL
# ==============================
print("\n📊 ESTADO ATUAL DAS GELADEIRAS\n")

for device in sorted(latest_per_device.keys()):
    rec = latest_per_device[device]

    print("=" * 70)
    print(f"Device: {device}")
    print(f"Temp: {rec['data']['temperature']:.2f}")
    print(f"Lat/Lon: {rec['data']['latitude']:.4f}, {rec['data']['longitude']:.4f}")
    print(f"Time: {rec['event_ts']}")
    print(f"Partition: {rec['partition']}")
    print(f"Offset: {rec['offset']}")
    print("=" * 70)
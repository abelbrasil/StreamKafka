import json
import time
import os
import signal
import random
from confluent_kafka import Producer
from dotenv import load_dotenv
from pathlib import Path

# ==============================
# LOAD ENV
# ==============================
if os.getenv("ENV") != "docker":
    BASE_DIR = Path(__file__).resolve().parents[1]
    load_dotenv(BASE_DIR / ".env")

TOPIC = os.getenv("TOPIC")

# ==============================
# KAFKA CONFIG
# ==============================
producer = Producer({
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("SASL_USERNAME"),
    'sasl.password': os.getenv("SASL_PASSWORD"),
})

# ==============================
# CONTROLE
# ==============================
running = True

def shutdown(sig, frame):
    global running
    print("🛑 Encerrando producer...")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ==============================
# CONFIG CEARÁ
# ==============================
LAT_MIN = -7.85
LAT_MAX = -2.75
LON_MIN = -41.45
LON_MAX = -37.25

# ==============================
# GELADEIRAS FIXAS
# ==============================
devices = [f"geladeira_{i:02d}" for i in range(20)]

# 5 com problema (fixas)
faulty_devices = set(devices[:5])

def generate_temp(device):
    if device in faulty_devices:
        return random.uniform(20, 40)
    return random.uniform(-4, 0)

print("🚀 Producer Ceará iniciado...")

# ==============================
# LOOP
# ==============================
while running:
    for device in devices:
        event = {
            "device_id": device,
            "timestamp": time.time(),
            "latitude": random.uniform(LAT_MIN, LAT_MAX),
            "longitude": random.uniform(LON_MIN, LON_MAX),
            "temperature": generate_temp(device),
        }

        producer.produce(
            TOPIC,
            key=device,
            value=json.dumps(event)
        )

    producer.flush()
    print("📤 Lote enviado")

    time.sleep(2)

print("🔚 Finalizando producer...")
producer.flush()
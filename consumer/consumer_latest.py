import json
import os
import time
import signal
from datetime import datetime, UTC
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import create_engine, text

# ==============================
# LOAD ENV
# ==============================
if os.getenv("ENV") != "docker":
    BASE_DIR = Path(__file__).resolve().parents[1]
    load_dotenv(BASE_DIR / ".env")

TOPIC = os.getenv("TOPIC")
DB_URI = os.getenv("DB_URI")

# ==============================
# KAFKA CONFIG
# ==============================
consumer = Consumer({
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
    'group.id': f'consumer-latest-{int(time.time())}',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("SASL_USERNAME"),
    'sasl.password': os.getenv("SASL_PASSWORD"),
})

# ==============================
# DATABASE
# ==============================
engine = create_engine(DB_URI, pool_pre_ping=True)

# ==============================
# RECRIAR TABELA
# ==============================
with engine.begin() as conn:
    conn.execute(text("""
    DROP TABLE IF EXISTS tab_temp_geladeira.geladeira_temp_atual_ce;
    """))

    conn.execute(text("""
    CREATE TABLE tab_temp_geladeira.geladeira_temp_atual_ce (
        device_id TEXT PRIMARY KEY,
        event_ts TIMESTAMP,
        kafka_partition INTEGER,
        kafka_offset BIGINT,
        kafka_key TEXT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        temperature DOUBLE PRECISION,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """))

# ==============================
# UPSERT QUERY
# ==============================
upsert_query = text("""
INSERT INTO tab_temp_geladeira.geladeira_temp_atual_ce (
    device_id,
    event_ts,
    kafka_partition,
    kafka_offset,
    kafka_key,
    latitude,
    longitude,
    temperature,
    updated_at
)
VALUES (
    :device_id,
    :event_ts,
    :kafka_partition,
    :kafka_offset,
    :kafka_key,
    :latitude,
    :longitude,
    :temperature,
    CURRENT_TIMESTAMP
)
ON CONFLICT (device_id)
DO UPDATE SET
    event_ts = EXCLUDED.event_ts,
    kafka_partition = EXCLUDED.kafka_partition,
    kafka_offset = EXCLUDED.kafka_offset,
    kafka_key = EXCLUDED.kafka_key,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    temperature = EXCLUDED.temperature,
    updated_at = CURRENT_TIMESTAMP
""")

# ==============================
# CONTROLE
# ==============================
running = True

def shutdown(sig, frame):
    global running
    print("🛑 Encerrando consumer_latest...")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ==============================
# DESCOBRIR PARTIÇÕES
# ==============================
metadata = consumer.list_topics(TOPIC)
partitions = list(metadata.topics[TOPIC].partitions.keys())

print(f"🔎 Partições: {partitions}")

# ==============================
# ASSIGN OFFSET RECENTE
# ==============================
topic_partitions = []

for p in partitions:
    tp = TopicPartition(TOPIC, p)

    low, high = consumer.get_watermark_offsets(tp)

    # pega últimos eventos por partição
    offset = max(high - 50, low)

    tp.offset = offset
    topic_partitions.append(tp)

consumer.assign(topic_partitions)

print("🚀 Consumer Latest iniciado...")

# ==============================
# LOOP PRINCIPAL
# ==============================
while running:

    latest_per_device = {}
    start_time = time.time()

    # janela curta de leitura
    while time.time() - start_time < 3:

        msg = consumer.poll(0.5)

        if msg is None:
            continue

        if msg.error():
            print("Erro Kafka:", msg.error())
            continue

        try:
            data = json.loads(msg.value().decode())

            device = data["device_id"]
            event_ts = datetime.fromtimestamp(data["timestamp"], UTC)

            record = {
                "device_id": device,
                "event_ts": event_ts,
                "kafka_partition": msg.partition(),
                "kafka_offset": msg.offset(),
                "kafka_key": msg.key().decode() if msg.key() else None,
                "latitude": data["latitude"],
                "longitude": data["longitude"],
                "temperature": data["temperature"]
            }

            # mantém apenas o mais recente por device
            if device not in latest_per_device:
                latest_per_device[device] = record
            else:
                if event_ts > latest_per_device[device]["event_ts"]:
                    latest_per_device[device] = record

        except Exception as e:
            print("❌ Erro ao processar mensagem:", e)

    # ==============================
    # UPSERT NO BANCO
    # ==============================
    if latest_per_device:
        try:
            with engine.begin() as conn:
                conn.execute(upsert_query, list(latest_per_device.values()))

            print(f"✅ Estado atualizado: {len(latest_per_device)} geladeiras")

        except Exception as e:
            print("❌ Erro no UPSERT:", e)

    time.sleep(1)

# ==============================
# FINALIZAÇÃO
# ==============================
consumer.close()
print("🔚 Consumer Latest encerrado")
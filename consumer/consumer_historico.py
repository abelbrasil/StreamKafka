import json
import os
import time
import signal
from datetime import datetime, UTC
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Consumer
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
    'group.id': 'consumer-historico',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,

    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("SASL_USERNAME"),
    'sasl.password': os.getenv("SASL_PASSWORD"),
})

consumer.subscribe([TOPIC])

# ==============================
# DATABASE
# ==============================
engine = create_engine(DB_URI, pool_pre_ping=True)

# ==============================
# CREATE TABLE + CONSTRAINT
# ==============================
with engine.begin() as conn:

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS tab_temp_geladeira.geladeira_temp_ce (
        id BIGSERIAL PRIMARY KEY,
        device_id TEXT,
        event_ts TIMESTAMP,
        kafka_partition INTEGER,
        kafka_offset BIGINT,
        kafka_key TEXT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        temperature DOUBLE PRECISION,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """))

    # 🔥 Constraint de idempotência
    conn.execute(text("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'uniq_kafka_event'
        ) THEN
            ALTER TABLE tab_temp_geladeira.geladeira_temp_ce
            ADD CONSTRAINT uniq_kafka_event
            UNIQUE (kafka_partition, kafka_offset);
        END IF;
    END
    $$;
    """))

# ==============================
# INSERT QUERY (IDEMPOTENTE)
# ==============================
insert_query = text("""
INSERT INTO tab_temp_geladeira.geladeira_temp_ce (
    device_id,
    event_ts,
    kafka_partition,
    kafka_offset,
    kafka_key,
    latitude,
    longitude,
    temperature
)
VALUES (
    :device_id,
    :event_ts,
    :kafka_partition,
    :kafka_offset,
    :kafka_key,
    :latitude,
    :longitude,
    :temperature
)
ON CONFLICT DO NOTHING
""")

# ==============================
# CONTROLE
# ==============================
running = True

def shutdown(sig, frame):
    global running
    print("🛑 Encerrando consumer_historico...")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ==============================
# BUFFER
# ==============================
buffer = []
BATCH_SIZE = 100

print("🚀 Consumer Histórico iniciado...")

# ==============================
# LOOP PRINCIPAL
# ==============================
while running:

    msgs = consumer.consume(num_messages=50, timeout=1.0)

    if not msgs:
        continue

    for msg in msgs:

        if msg is None:
            continue

        if msg.error():
            print("Erro Kafka:", msg.error())
            continue

        try:
            data = json.loads(msg.value().decode())

            event_ts = datetime.fromtimestamp(data["timestamp"], UTC)

            record = {
                "device_id": data["device_id"],
                "event_ts": event_ts,
                "kafka_partition": msg.partition(),
                "kafka_offset": msg.offset(),
                "kafka_key": msg.key().decode() if msg.key() else None,
                "latitude": data["latitude"],
                "longitude": data["longitude"],
                "temperature": data["temperature"]
            }

            buffer.append(record)

        except Exception as e:
            print("❌ Erro ao processar mensagem:", e)

    # ==============================
    # INSERT EM LOTE
    # ==============================
    if len(buffer) >= BATCH_SIZE:
        try:
            with engine.begin() as conn:
                conn.execute(insert_query, buffer)

            consumer.commit()

            print(f"📦 Histórico inserido: {len(buffer)} registros (idempotente)")

            buffer.clear()

        except Exception as e:
            print("❌ Erro ao inserir histórico:", e)

# ==============================
# FLUSH FINAL
# ==============================
if buffer:
    try:
        with engine.begin() as conn:
            conn.execute(insert_query, buffer)

        print(f"📦 Flush final: {len(buffer)} registros")

    except Exception as e:
        print("❌ Erro no flush final:", e)

consumer.close()

print("🔚 Consumer Histórico encerrado")
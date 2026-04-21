import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

# ==============================
# LOAD ENV
# ==============================
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

DB_URI = os.getenv("DB_URI")

engine = create_engine(DB_URI)

print("\n🔎 INICIANDO VERIFICAÇÃO DE DUPLICIDADE\n")

# ==============================
# TOTAL DE REGISTROS
# ==============================
with engine.connect() as conn:
    total = conn.execute(text("""
        SELECT COUNT(*) FROM tab_temp_geladeira.geladeira_temp_ce
    """)).scalar()

print(f"📊 Total de registros: {total}")

# ==============================
# DUPLICADOS (KAFKA - CRÍTICO)
# ==============================
with engine.connect() as conn:
    dup_kafka = conn.execute(text("""
        SELECT kafka_partition, kafka_offset, COUNT(*) as qtd
        FROM tab_temp_geladeira.geladeira_temp_ce
        GROUP BY kafka_partition, kafka_offset
        HAVING COUNT(*) > 1
        ORDER BY qtd DESC
        LIMIT 10
    """)).fetchall()

if dup_kafka:
    print("\n❌ DUPLICIDADE POR KAFKA (partition + offset):")
    for row in dup_kafka:
        print(row)
else:
    print("\n✅ Sem duplicidade por Kafka (idempotência OK)")

# ==============================
# DUPLICADOS (REGRA DE NEGÓCIO)
# ==============================
with engine.connect() as conn:
    dup_business = conn.execute(text("""
        SELECT device_id, event_ts, COUNT(*) as qtd
        FROM tab_temp_geladeira.geladeira_temp_ce
        GROUP BY device_id, event_ts
        HAVING COUNT(*) > 1
        ORDER BY qtd DESC
        LIMIT 10
    """)).fetchall()

if dup_business:
    print("\n⚠️ DUPLICIDADE POR DEVICE/TIMESTAMP:")
    for row in dup_business:
        print(row)
else:
    print("\n✅ Sem duplicidade por device/timestamp")

# ==============================
# CONTAGEM DE EVENTOS ÚNICOS
# ==============================
with engine.connect() as conn:
    unique_kafka = conn.execute(text("""
        SELECT COUNT(DISTINCT (kafka_partition, kafka_offset))
        FROM tab_temp_geladeira.geladeira_temp_ce
    """)).scalar()

print(f"\n📊 Eventos únicos Kafka: {unique_kafka}")

# ==============================
# TAXA DE DUPLICIDADE
# ==============================
if total > 0:
    duplicidade = total - unique_kafka
    perc = (duplicidade / total) * 100

    print(f"\n📉 Duplicados: {duplicidade}")
    print(f"📉 Taxa de duplicidade: {perc:.2f}%")

print("\n🏁 FIM DA VERIFICAÇÃO\n")
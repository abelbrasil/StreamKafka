import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from pathlib import Path

import plotly.express as px

# 🔥 auto refresh
from streamlit_autorefresh import st_autorefresh

# ==============================
# CONFIG
# ==============================
st.set_page_config(layout="wide")
st.title("📊 Monitoramento de Geladeiras - Ceará (Tempo Real)")

# 🔥 refresh a cada 5 segundos
st_autorefresh(interval=5 * 1000, key="refresh")

# ==============================
# LOAD ENV
# ==============================
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

DB_URI = os.getenv("DB_URI")

# ==============================
# CONEXÃO (CACHEADA)
# ==============================
@st.cache_resource
def get_engine():
    return create_engine(DB_URI)

engine = get_engine()

# ==============================
# QUERY (CACHE CONTROLADO)
# ==============================
@st.cache_data(ttl=5)  # 🔥 atualiza a cada 5 segundos
def load_data():
    return pd.read_sql("""
        SELECT *
        FROM tab_temp_geladeira.geladeira_temp_atual_ce
    """, engine)

df = load_data()

# ==============================
# VALIDAÇÃO
# ==============================
if df.empty:
    st.warning("Nenhum dado disponível")
    st.stop()

# ==============================
# STATUS
# ==============================
df["status"] = df["temperature"].apply(
    lambda x: "ALERTA" if x > 10 else "NORMAL"
)

# ==============================
# KPIs
# ==============================
total = len(df)
normal = df[df["status"] == "NORMAL"]
alerta = df[df["status"] == "ALERTA"]

media_normal = normal["temperature"].mean() if not normal.empty else 0
media_alerta = alerta["temperature"].mean() if not alerta.empty else 0

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total Geladeiras", total)
col2.metric("Normais", len(normal))
col3.metric("Alerta", len(alerta))
col4.metric("Média Normal", f"{media_normal:.2f} °C")
col5.metric("Média Alerta", f"{media_alerta:.2f} °C")

# ==============================
# GRÁFICO
# ==============================
st.subheader("📊 Temperatura Atual por Geladeira")

df_plot = df.sort_values("device_id")

st.bar_chart(
    df_plot.set_index("device_id")["temperature"]
)

# ==============================
# MAPA (COM COR)
# ==============================
st.subheader("📍 Localização das Geladeiras")

fig = px.scatter_mapbox(
    df,
    lat="latitude",
    lon="longitude",
    color="status",
    color_discrete_map={
        "NORMAL": "blue",
        "ALERTA": "red"
    },
    zoom=5,
    height=500
)

fig.update_layout(mapbox_style="open-street-map")

st.plotly_chart(fig, use_container_width=True)

# ==============================
# TABELA
# ==============================
st.subheader("📋 Estado Atual")

st.dataframe(
    df[[
        "device_id",
        "temperature",
        "status",
        "kafka_partition",
        "kafka_offset",
        "updated_at"
    ]].sort_values("device_id"),
    use_container_width=True
)
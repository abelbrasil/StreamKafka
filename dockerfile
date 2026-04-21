FROM python:3.12-slim

# Evita .pyc e melhora logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependências de runtime (mínimas)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
 && rm -rf /var/lib/apt/lists/*

# Instala deps primeiro (cache eficiente)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código
COPY . .

# Default (sobrescrito no compose)
CMD ["python", "producer/producer.py"]
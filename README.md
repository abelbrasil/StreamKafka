# 📊 Monitoramento de Geladeiras em Tempo Real (Ceará)

Projeto de arquitetura streaming em tempo real (<1s) para monitoramento de temperatura de geladeiras distribuídas geograficamente no estado do Ceará.

---

# 🧠 Objetivo

Construir um pipeline completo de dados em tempo real com:

- Arquitetura orientada a eventos (Event-driven)
- Baixa latência (<1s)
- Processamento contínuo (streaming)
- Idempotência e confiabilidade
- Visualização em tempo real

---

# 🏗️ Arquitetura

Producer → Kafka → Consumers → Postgres → Dashboard

- consumer_latest → estado atual
- consumer_historico → histórico

---

# ⚙️ Componentes

## Producer
- Simula 20 geladeiras
- Envio a cada 2 segundos
- 5 com temperatura fora do padrão

## Kafka
- Tópico: temp_geladeira_ceara
- Particionado automaticamente
- Chave: device_id

## Consumer Histórico
- Append-only
- Buffer de 100 mensagens
- Idempotência com:
  UNIQUE (kafka_partition, kafka_offset)

## Consumer Latest
- Mantém estado atual por device
- UPSERT no banco

## PostgreSQL
- geladeira_temp_ce → histórico
- geladeira_temp_atual_ce → estado atual

## Dashboard
- KPIs
- Gráfico de temperatura
- Mapa
- Atualização automática

---

# 🚀 Execução

## 1. Clonar repositório

git clone <repo>
cd <repo>

## 2. Configurar ambiente

cp .env.example .env

## 3. Build

docker-compose build

## 4. Subir ambiente

docker-compose up

## 5. Rodar em background

docker-compose up -d

## 6. Acessar dashboard

http://localhost:8501

## 7. Parar

docker-compose down

## 8. Limpeza completa

docker-compose down --rmi all --volumes --remove-orphans

---

# 🔐 Configuração (.env)

BOOTSTRAP_SERVERS=

SASL_USERNAME=

SASL_PASSWORD=

DB_URI=

TOPIC=temp_geladeira_ceara

---

# ⚡ Decisões Arquiteturais

- Kafka para alta vazão e baixa latência
- Kappa Architecture (streaming-first)
- Idempotência via banco
- Stateful processing no consumer

---

# 📊 Métricas

- Latência: ~1–2s
- Consistência: eventual
- Escalável por partição

---

# 🔎 Observabilidade

Evoluções possíveis:
- Prometheus
- Grafana
- Alertas

---

# 🚀 Próximos passos

- WebSocket
- Flink/Kafka Streams
- Kubernetes
- Alertas automáticos

---

# 🎯 Conclusão

Pipeline completo de dados em tempo real com arquitetura moderna e pronto para evolução.

---

# 👨‍💻 Autor

Abel Brasil Ramos da Silva

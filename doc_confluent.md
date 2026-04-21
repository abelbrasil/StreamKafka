## Configuração Inicial do Confluent Cloud Kakfa

```bash

brew install confluentinc/tap/cli
confluent version

confluent login

confluent kafka cluster create <nome-cluster> --cloud aws --region us-east-2 --type basic

confluent kafka cluster list
confluent kafka cluster use <id_cluster>
confluent api-key create --resource <id_cluster>

+------------+------------------------------------------------------------------+
| API Key    | XXXXXXXXXXXX
| API Secret | XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX |
+------------+------------------------------------------------------------------+

confluent kafka topic list

confluent kafka topic create <topic_name> --partitions 6

confluent kafka topic describe <topic_name>

+--------------------+-------------------+
| Name               | <topic_name>      |
| Internal           | false             |
| Replication Factor | 3                 |
| Partition Count    | 6                 |
+--------------------+-------------------+

confluent kafka cluster describe

+----------------------+---------------------------------------------------------+
| Endpoint             | SASL_SSL://XXXXXXXXXXXXXXXXXXXXXXX.confluent.cloud:YYYY |
+----------------------+---------------------------------------------------------+

```

docker-compose down
docker-compose build --no-cache
docker-compose up
docker-compose up --build

docker rm -f kafka-producer
docker rmi 03kafka-producer
docker rmi 03kafka-consumer
docker rmi 03kafka-dashboard
docker-compose down # para e remove, imagens ficam
docker-compose stop  # apenas para o container

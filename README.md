# Web Log Analysis and Monitoring with Apache Kafka, Elasticsearch and Docker

**COMP 6231 — Distributed Systems Design, Concordia University, Montreal**

A real-time distributed log analytics pipeline. Apache web server logs are parsed, streamed through a three-broker Kafka cluster, indexed in Elasticsearch, and explored in Kibana. Producer and consumer instances expose Prometheus metrics, visualised in Grafana.

---

## Authors

| Name | Student ID |
|---|---|
| Saranraj Sivakumar | 40306771 |
| Vighnesh Kumar | 40312394 |
| Navachethan Murugeppa | 40306253 |
| Aditya Sawant | 40311540 |
| Sanjay Upadhyaya | 40306152 |

---

## Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech stack](#tech-stack)
- [Distributed systems features](#distributed-systems-features)
- [Project structure](#project-structure)
- [Setup](#setup)
- [Monitoring](#monitoring)
- [Results](#results)
- [Known limitations](#known-limitations)
- [References](#references)

---

## Overview

Logs are a primary signal for system health and reliability, but at scale the volume makes manual inspection impractical. This project builds a streaming pipeline over a 3.3 GB web server log corpus from the Iranian e-commerce site `zanbil.ir`, processing entries in real time rather than in batches.

Each log line carries a client IP, timestamp, HTTP method, URL, response status, byte count, referrer, and user agent. The pipeline parses these into structured JSON, streams them through Kafka, and makes them searchable in Elasticsearch within moments of ingestion.

---

## Architecture

```
          data/access.log  (Apache Combined Log Format)
                    |
                    v
          preprocess_logs.py        regex parse -> newline-delimited JSON
                    |
                    v
          kafka_producer.py         tags each entry with produce timestamp
                    |
                    v
   +--------------------------------------+
   |  Apache Kafka                        |
   |  3 brokers | 3 partitions | RF = 3   |
   |  min.insync.replicas = 2             |
   +--------------------------------------+
                    |
                    v
          kafka_consumer.py         computes end-to-end latency
                    |
                    v
          Elasticsearch             index: web_logs, 3 shards
                    |
                    v
          Kibana :5601              dashboards and log exploration


   producers :8000 :8004 :8005  --\
                                   >--  Prometheus :9090  -->  Grafana :3000
   consumers :8001 :8002 :8003  --/
```

---

## Tech stack

| Component | Technology | Version |
|---|---|---|
| Containerisation | Docker / Docker Compose | — |
| Message broker | Apache Kafka (Confluent) | 7.4.0 |
| Cluster coordination | Apache ZooKeeper | 7.4.0 |
| Search and analytics | Elasticsearch | 8.9.0 |
| Log visualisation | Kibana | 8.9.0 |
| Metrics collection | Prometheus | latest |
| Metrics dashboards | Grafana | latest |
| Scripting | Python | 3.9 |

Python dependencies are listed in `requirements.txt`.

---

## Distributed systems features

### Data streaming

Producers publish to a Kafka topic; consumers read from it independently. The topic acts as the interface between the two, so neither side needs to know about the other — producers can be added or removed without touching consumer code.

### Fault tolerance

- Three-broker cluster with `replication-factor=3` on the topic
- `min.insync.replicas=2`, so a write is only acknowledged once at least two replicas hold it
- If the partition leader fails, an in-sync replica takes over without data loss
- The consumer retries broker connections five times with backoff before giving up

### Real-time indexing

Elasticsearch indexes each entry as it is consumed. Explicit mappings set `ip` and `method` as keywords, `timestamp` and `ingest_timestamp` as dates, `status` and `bytes` as integers, and `url`, `referrer`, and `user_agent` as text. A separate `ingest_timestamp` is written at index time so ingestion lag can be measured against the original event time.

### Scalability

Three partitions are distributed across the three brokers, allowing concurrent reads and writes. Consumers share a group ID, so partitions are divided between them automatically. The system was tested with three concurrent producers and three concurrent consumers.

### Monitoring

Prometheus scrapes each producer and consumer instance every 15 seconds. Grafana renders throughput, per-consumer processing latency, and CPU consumption.

---

## Project structure

```
.
├── Dockerfile
├── docker-compose.yml
├── prometheus.yml
├── requirements.txt
├── README.md
├── .gitignore
├── data/                     # not committed — see Setup
│   ├── access.log
│   └── processed_logs.json
└── scripts/
    ├── preprocess_logs.py
    ├── kafka_producer.py
    ├── kafka_consumer.py
    └── create_index.py
```

---

## Setup

### 1. Get the dataset

Download from [Kaggle](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs/data), extract it, and place `access.log` in a `data/` folder at the project root. The dataset is not committed to this repository.

### 2. Start the stack

```bash
docker compose build
docker compose up -d
```

### 3. Pre-process the logs

```bash
docker exec -it script-runner python /app/scripts/preprocess_logs.py
```

Reads `data/access.log` and writes `data/processed_logs.json`.

### 4. Create the Kafka topic

```bash
docker exec -it kafka-broker-1 kafka-topics \
  --create \
  --bootstrap-server kafka-broker-1:9092,kafka-broker-2:9093,kafka-broker-3:9094 \
  --replication-factor 3 \
  --partitions 3 \
  --topic web_topic
```

Verify:

```bash
docker exec -it kafka-broker-1 kafka-topics \
  --list \
  --bootstrap-server kafka-broker-1:9092,kafka-broker-2:9093,kafka-broker-3:9094
```

### 5. Create the index and run the pipeline

```bash
docker exec -it script-runner python /app/scripts/create_index.py
docker exec -it script-runner python /app/scripts/kafka_producer.py producer1 8000
docker exec -it script-runner python /app/scripts/kafka_consumer.py consumer1 8001
```

### 6. Scale out

Each additional instance needs its own terminal and its own metrics port:

```bash
# producers
docker exec -it script-runner python /app/scripts/kafka_producer.py producer2 8004
docker exec -it script-runner python /app/scripts/kafka_producer.py producer3 8005

# consumers
docker exec -it script-runner python /app/scripts/kafka_consumer.py consumer2 8002
docker exec -it script-runner python /app/scripts/kafka_consumer.py consumer3 8003
```

Ports must match the targets in `prometheus.yml`.

---

## Monitoring

| Tool | URL | Credentials |
|---|---|---|
| Kibana | http://localhost:5601 | none |
| Prometheus | http://localhost:9090 | none |
| Grafana | http://localhost:3000 | admin / admin |
| Elasticsearch | http://localhost:9201 | none |

Metrics exposed per instance:

| Metric | Type | Source |
|---|---|---|
| `produced_messages` | Counter | producer |
| `consumed_messages` | Counter | consumer |
| `log_processing_latency` | Gauge | consumer |

Latency is measured as the difference between the consumer's wall clock at processing time and the `timestamp_produced` field written by the producer.

---

## Results

**Consumer latency.** All three consumers held between 3 ms and 4 ms under normal load. Two spikes were observed — one at roughly 11 ms and one at 12 ms — consistent with transient resource contention rather than a systematic bottleneck.

**CPU usage.** Producer instances reached up to 198 CPU-seconds under sustained load. Consumers stayed between 2 and 15 CPU-seconds, reflecting lighter per-message work.

**Traffic patterns (top 10 IPs, first 15,000 records).** Two addresses dominated: `66.249.66.194` with over 2,400 requests and `66.249.66.91` with over 1,500. The remaining addresses ranged between 500 and 1,000 requests each, a distribution consistent with crawler traffic against a long tail of ordinary clients.

---

## Known limitations

These are deliberate trade-offs for a single-machine coursework deployment, not oversights.

- **The producer throttles at 0.1 s per message** to simulate a live stream. Throughput figures therefore reflect the throttle, not Kafka's capacity, and only a subset of the 3.3 GB corpus is streamed in a typical run.
- **Grafana dashboards are created manually** rather than provisioned as code, so they do not persist with the repository.

---

## References

1. A. Kumar et al. *Real-Time Monitoring of Servers with Prometheus and Grafana for High Availability.* Jain University Research Symposium, 2023.
2. H. Zhou, W. Qian, X. Zhou et al. *Scalable and Adaptive Log Manager in Distributed Systems.* Frontiers of Computer Science, 17(172205), 2023. DOI: 10.1007/s11704-022-1357-5
3. J. V. and K. B. Nath. *IoT Data Analytics Pipeline Using Elastic Stack and Kafka.* IJCSE, 8(5):144–148, 2023.
4. P. Atri. *Design and Implementation of High-Throughput Data Streams using Apache Kafka for Real-Time Data Pipelines.* IJSR, 7(11):1988–1991, 2018.
5. Y. Wei, M. Li, B. Xu. *Research on Establishing an Efficient Log Analysis System with Kafka and Elasticsearch.* ICSDA, 2023.
6. [Elasticsearch Documentation](https://www.elastic.co/guide/index.html)
7. [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
8. [Docker Documentation](https://docs.docker.com/)

# Web Log Analysis and Monitoring Using Apache Kafka, Elasticsearch and Docker

COMP 6231 - Distributed Systems Design | Concordia University, Montreal, Canada

A real-time distributed log analytics pipeline that streams Apache web server logs through
a Kafka cluster, indexes them in Elasticsearch, and visualises them through Kibana.
System metrics are collected by Prometheus and displayed in Grafana.

---

## Authors

| Name | Student ID | Email |
|---|---|---|
| Saranraj Sivakumar | 40306771 | saran260raj@gmail.com |
| Vighnesh Kumar | 40312394 | vighnesh28kumar@gmail.com |
| Navachethan Murugeppa | 40306253 | navachethan.murugeppa@gmail.com |
| Aditya Sawant | 40311540 | adi.v.sawant@gmail.com |
| Sanjay Upadhyaya | 40306152 | sanjayup21@gmail.com |

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Distributed Systems Features](#distributed-systems-features)
- [Project Structure](#project-structure)
- [Setup and Running](#setup-and-running)
- [Monitoring and Visualization](#monitoring-and-visualization)
- [Results](#results)
- [References](#references)

---

## Overview

Logs play a critical role in monitoring system health and reliability. In large-scale systems,
the volume of logs can reach billions of entries, making manual analysis impractical. This project
builds a streaming pipeline over a 3.3 GB web server log corpus from the Iranian e-commerce site
zanbil.ir. It processes logs in real time using modern distributed systems tools.

Key data fields: timestamps, HTTP methods, IP addresses, response statuses, URLs, user agents.

---

## Architecture

`
Raw access.log (Kaggle)
        |
        v
 preprocess_logs.py      -- Parses Apache Combined Log Format to JSON
        |
        v
 kafka_producer.py       -- Streams JSON entries to Kafka topic
        |
        v
 Apache Kafka (3 brokers) -- Fault-tolerant, partitioned topic
        |
        v
 kafka_consumer.py       -- Reads from Kafka, measures latency
        |
        v
 Elasticsearch           -- Real-time indexing, web_logs index, 3 shards
        |
        v
 Kibana :5601            -- Dashboards and log exploration

 Prometheus :9090        -- Scrapes producer and consumer metrics
        |
        v
 Grafana :3000           -- Latency, CPU, and throughput dashboards
`

---

## Tech Stack

| Component | Technology | Version |
|---|---|---|
| Containerization | Docker / Docker Compose | - |
| Message Broker | Apache Kafka (Confluent) | 7.4.0 |
| Cluster Coordination | Apache Zookeeper | 7.4.0 |
| Search and Analytics | Elasticsearch | 8.9.0 |
| Log Visualization | Kibana | 8.9.0 |
| Metrics Collection | Prometheus | latest |
| Metrics Visualization | Grafana | latest |
| Scripting | Python | 3.9 |

Python libraries: kafka-python==2.0.2, elasticsearch==8.9.0, prometheus_client==0.17.1

---

## Distributed Systems Features

### Data Streaming

Log data flows from Kafka Producers to a Kafka Topic and then to Kafka Consumers, enabling
low-latency, event-driven processing without tight coupling between components.

### Fault Tolerance

- 3-broker Kafka cluster with replication-factor=3
- min.insync.replicas=2 ensures data is not lost if one broker goes down
- Consumer has retry logic with 5 attempts and backoff before failing

### Real-Time Indexing

Elasticsearch indexes each log entry as soon as it is consumed. Field mappings define types
for ip (keyword), timestamp (date), method (keyword), status (integer), and others.
The index uses 3 shards for parallel read and write operations.

### Scalability

- Topic partitions are distributed across 3 brokers for concurrent access
- Multiple producer and consumer instances can be started on separate ports
- The system was tested with 3 simultaneous producer and 3 consumer instances

### Monitoring

Prometheus scrapes metrics from each producer and consumer instance every 15 seconds.
Grafana is used to visualise message throughput, processing latency, and CPU usage.

---

## Project Structure

`
Group_Learn_DSD_Project/
|-- Dockerfile
|-- docker-compose.yml
|-- prometheus.yml
|-- requirements.txt
|-- README.md
|-- data/                    (not committed - download dataset manually, see Setup)
|   |-- access.log
|   -- processed_logs.json
-- scripts/
    |-- preprocess_logs.py
    |-- kafka_producer.py
    |-- kafka_consumer.py
    -- create_index.py
`

---

## Setup and Running

### 1. Get the Dataset

Download from Kaggle:
https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs/data

Extract the zip file and place ccess.log inside a data/ folder at the project root.

### 2. Start All Containers

`ash
docker compose build
docker compose up -d
`

### 3. Pre-process the Logs

`ash
docker exec -it script-runner bash
python preprocess_logs.py
exit
`

This reads data/access.log and writes data/processed_logs.json.

### 4. Create the Kafka Topic

`ash
docker exec -it kafka-broker-1 kafka-topics \
  --create \
  --bootstrap-server kafka-broker-1:9092,kafka-broker-2:9093,kafka-broker-3:9094 \
  --replication-factor 3 \
  --partitions 3 \
  --topic web_topic
`

Verify the topic exists:

`ash
docker exec -it kafka-broker-1 kafka-topics \
  --list \
  --bootstrap-server kafka-broker-1:9092,kafka-broker-2:9093,kafka-broker-3:9094
`

### 5. Run the Pipeline

`ash
docker exec -it script-runner python /app/scripts/create_index.py

docker exec -it script-runner python /app/scripts/kafka_producer.py producer1 8000

docker exec -it script-runner python /app/scripts/kafka_consumer.py consumer1 8001
`

### 6. Run Multiple Instances

Open a separate terminal for each additional instance:

`ash
docker exec -it script-runner python /app/scripts/kafka_producer.py producer2 8004
docker exec -it script-runner python /app/scripts/kafka_producer.py producer3 8005

docker exec -it script-runner python /app/scripts/kafka_consumer.py consumer2 8002
docker exec -it script-runner python /app/scripts/kafka_consumer.py consumer3 8003
`

---

## Monitoring and Visualization

| Tool | URL | Credentials |
|---|---|---|
| Kibana | http://localhost:5601 | none |
| Prometheus | http://localhost:9090 | none |
| Grafana | http://localhost:3000 | admin / admin |
| Elasticsearch | http://localhost:9201 | none |

Prometheus metrics exposed per instance:
- produced_messages - total messages sent (producer)
- consumed_messages - total messages processed (consumer)
- log_processing_latency - end-to-end latency in seconds (consumer)
- consumer_group_lag - total unprocessed messages across all partitions

---

## Results

### Consumer Latency

All three consumer instances maintained a stable latency between 3 ms and 4 ms under normal load.
Two spikes were observed: one reaching 11 ms and another reaching 12 ms, likely due to temporary
resource contention. Overall the system operated within acceptable bounds for real-time processing.

### CPU Usage

Producer instances used up to 198 CPU-seconds under heavy load. Consumer instances used between
2 and 15 CPU-seconds, consistent with lighter per-message processing work.

### IP Address Trends (Top 10, first 15,000 records)

- 66.249.66.194 was the most active with over 2,400 requests
- 66.249.66.91 was second with over 1,500 requests
- Remaining addresses ranged from 500 to 1,000 requests each

---

## References

1. A. Kumar et al. Real-Time Monitoring of Servers with Prometheus and Grafana. 2023.
2. H. Zhou et al. Scalable and Adaptive Log Manager in Distributed Systems. Frontiers of Computer Science, 2023. DOI: 10.1007/s11704-022-1357-5
3. J. V. and K. B. Nath. IoT Data Analytics Pipeline Using Elastic Stack and Kafka. IJCSE, 2023.
4. P. Atri. Design and Implementation of High-Throughput Data Streams using Apache Kafka. IJSR, 2018.
5. Y. Wei et al. Research on Establishing an Efficient Log Analysis System with Kafka and Elasticsearch. ICSDA, 2023.
6. Elasticsearch Documentation: https://www.elastic.co/guide/index.html
7. Apache Kafka Documentation: https://kafka.apache.org/documentation/
8. Docker Documentation: https://docs.docker.com/

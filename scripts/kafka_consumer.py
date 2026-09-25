
import json
import time
import logging
import threading
import sys
from datetime import datetime, timezone
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable
from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch
from prometheus_client import start_http_server, Gauge, Counter
from create_index import INDEX_NAME  # reuse the index name constant


KAFKA_BROKERS = ['kafka-broker-1:9092', 'kafka-broker-2:9093', 'kafka-broker-3:9094']
KAFKA_TOPIC = 'web_topic'
GROUP_ID = 'log_consumers'
ES_HOST = "http://elasticsearch:9200"

# Number of messages to accumulate before a bulk Elasticsearch flush.
# Batching 500 docs per HTTP round-trip is orders of magnitude faster
# than one es.index() call per message.
BULK_BATCH_SIZE = 500

logging.basicConfig(level=logging.INFO)
es = Elasticsearch(ES_HOST)

# Prometheus metrics
LATENCY = Gauge('log_processing_latency', 'End-to-end processing latency in seconds', ['instance'])
CONSUMED_MESSAGES = Counter('consumed_messages', 'Number of messages consumed', ['instance'])
CONSUMER_LAG = Gauge('consumer_group_lag', 'Total consumer group lag across all partitions')


def create_consumer():
    for attempt in range(5):
        try:
            return KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                metadata_max_age_ms=10000,
                reconnect_backoff_max_ms=5000,
                enable_auto_commit=True,
            )
        except NoBrokersAvailable:
            logging.warning(f"[Attempt {attempt + 1}] No brokers available. Retrying in 5 seconds...")
            time.sleep(5)
    raise Exception("Failed to connect to any Kafka brokers after multiple attempts.")


def get_consumer_lag():
    # Computes lag as sum of (end_offset - committed_offset) per partition
    # and exposes it as a Prometheus gauge.
    while True:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)

            committed = admin_client.list_consumer_group_offsets(GROUP_ID)
            admin_client.close()

            if not committed:
                logging.warning("No committed offsets found for group - consumer may not have started yet.")
                time.sleep(10)
                continue

            # fetch end (high-water mark) offsets using a temporary consumer
            temp_consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKERS)
            partitions = list(committed.keys())
            end_offsets = temp_consumer.end_offsets(partitions)
            temp_consumer.close()

            total_lag = 0
            for tp, meta in committed.items():
                end = end_offsets.get(tp, meta.offset)
                lag = max(0, end - meta.offset)
                total_lag += lag
                logging.debug(f"  {tp}: committed={meta.offset}, end={end}, lag={lag}")

            CONSUMER_LAG.set(total_lag)
            logging.info(f"Consumer group '{GROUP_ID}' total lag: {total_lag} messages")

        except Exception as e:
            logging.error(f"Error measuring consumer lag: {e}")

        time.sleep(10)


def bulk_index_logs(buffer):
    # Sends a batch of documents to Elasticsearch in a single request.
    # Much faster than one es.index() call per message at high volume.
    if not buffer:
        return

    now = datetime.now(timezone.utc).isoformat()
    actions = [
        {
            "_index": INDEX_NAME,
            "_source": {**entry, "ingest_timestamp": now},
        }
        for entry in buffer
    ]

    try:
        success, errors = bulk(es, actions)
        logging.info(f"Bulk indexed {success} documents.")
        if errors:
            logging.error(f"Bulk indexing errors: {errors}")
    except Exception as e:
        logging.error(f"Bulk indexing failed: {e}")


def consume_messages(instance_name, port):
    start_http_server(port)
    logging.info(f"Prometheus metrics available at http://localhost:{port}/metrics")

    consumer = create_consumer()
    logging.info(f"Consumer '{instance_name}' started. Listening on topic: {KAFKA_TOPIC}")

    buffer = []

    while True:
        try:
            for message in consumer:
                try:
                    log_entry = message.value
                    if not log_entry:
                        logging.debug("Skipping empty message.")
                        continue

                    CONSUMED_MESSAGES.labels(instance=instance_name).inc()

                    # calculate end-to-end latency using the timestamp the producer attached
                    timestamp_produced = log_entry.get('timestamp_produced')
                    if timestamp_produced:
                        latency = time.time() - timestamp_produced
                        LATENCY.labels(instance=instance_name).set(latency)

                    buffer.append(log_entry)

                    # flush to Elasticsearch once the buffer is full
                    if len(buffer) >= BULK_BATCH_SIZE:
                        bulk_index_logs(buffer)
                        buffer.clear()

                except json.JSONDecodeError as e:
                    logging.error(f"Invalid JSON message: {message.value} | Error: {e}")
                except Exception as e:
                    logging.error(f"Error processing message: {e}")

        except KafkaError as e:
            logging.error(f"Kafka error encountered: {e}. Flushing buffer and restarting consumer...")
            bulk_index_logs(buffer)  # flush before reconnecting
            buffer.clear()
            consumer.close()
            time.sleep(5)
            consumer = create_consumer()


if __name__ == "__main__":
    instance_name = sys.argv[1] if len(sys.argv) > 1 else 'default_consumer'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8001

    lag_thread = threading.Thread(target=get_consumer_lag, daemon=True)
    lag_thread.start()

    consume_messages(instance_name, port)


import time
import json
import sys
from kafka import KafkaProducer
from prometheus_client import start_http_server, Counter


KAFKA_BROKER = 'kafka-broker-1:9092,kafka-broker-2:9093,kafka-broker-3:9094'
KAFKA_TOPIC = 'web_topic'


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER.split(','),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


PRODUCED_MESSAGES = Counter('produced_messages', 'Number of messages produced', ['instance'])

def stream_logs_to_kafka(input_file, instance_name):

    with open(input_file, 'r') as infile:
        for line in infile:
            try:
                log_entry = json.loads(line.strip())


                log_entry['timestamp_produced'] = time.time()


                producer.send(KAFKA_TOPIC, log_entry)
                PRODUCED_MESSAGES.labels(instance=instance_name).inc()  # Increment Prometheus counter

                print(f"Sent: {log_entry}")
                time.sleep(0.1)
            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON line: {line.strip()} Error: {e}")

    producer.flush()
    producer.close()

if __name__ == "__main__":

    instance_name = sys.argv[1] if len(sys.argv) > 1 else 'default_instance'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8000  # Default port: 8000


    start_http_server(port)
    print(f"Prometheus metrics available at http://localhost:{port}/metrics")


    stream_logs_to_kafka('/app/data/processed_logs.json', instance_name)

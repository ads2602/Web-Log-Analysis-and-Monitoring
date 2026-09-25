
from elasticsearch import Elasticsearch
from datetime import datetime, timezone


ES_HOST = "http://elasticsearch:9200"
INDEX_NAME = "web_logs"


es = Elasticsearch(ES_HOST)

def create_index():

    mappings = {
        "properties": {
            "ip": {"type": "keyword"},
            "timestamp": {"type": "date"},
            "ingest_timestamp": {"type": "date"},
            "method": {"type": "keyword"},
            "url": {"type": "text"},
            "status": {"type": "integer"},
            "bytes": {"type": "integer"},
            "referrer": {"type": "text"},
            "user_agent": {"type": "text"}
        }
    }

    settings = {
        "index": {
            "number_of_shards": 3,
            # set to 0 for single-node; increase when running a multi-node cluster
            "number_of_replicas": 0
        }
    }

    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, mappings=mappings, settings=settings)
        print(f"Index '{INDEX_NAME}' created successfully.")
    else:
        print(f"Index '{INDEX_NAME}' already exists.")

def index_log(log_entry):
    try:
        log_entry['ingest_timestamp'] = datetime.now(timezone.utc).isoformat()
        response = es.index(index=INDEX_NAME, document=log_entry)
        print(f"Successfully indexed log entry: {response['_id']}")
    except Exception as e:
        print(f"Failed to index log entry: {e}")


if __name__ == "__main__":
    create_index()

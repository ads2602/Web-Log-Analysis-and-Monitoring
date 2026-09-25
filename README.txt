For Dataset : 
- Download from : https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs/data

- Click the Download button (a downward arrow icon) located at the top-right of the page.
- This will download a .zip file containing the dataset.
- Extract the Dataset
- After downloading, unzip the .zip file.

The dataset is now ready for use.

For Running the code:
- Unzip this folder 
- Create a folder named 'data'
- Add the dataset into the folder

- Run the program using commands in order:
- docker compose build
- docker compose up -d
- docker exec -it script-runner bash 
- cd app
- python preprocess_logs.py (this will transform the data to JSON format)
- exit
- Create a topic: Example - docker exec -it kafka-broker-1 kafka-topics --create --bootstrap-server kafka-broker-1:9092,kafka-   broker-2:9093,kafka-broker-3:9094 --replication-factor 3 --partitions 3 --topic web_topic

- Check if the topic exists:  docker exec -it kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:9092,kafka-broker-2:9093,kafka-broker-3:9094  

- Run the producer file: docker exec -it script-runner python /app/scripts/kafka_producer.py producer1 8000
- Run the create index file: docker exec -it script-runner python /app/scripts/create_index.py  
- Run the Consumer file: docker exec -it script-runner python /app/scripts/kafka_consumer.py consumer1 8001  

For Adding multiple consumers and producers open in different terminals: 
 docker exec -it script-runner python /app/scripts/kafka_producer.py producer2 8004
 docker exec -it script-runner python /app/scripts/kafka_producer.py producer3 8005

 docker exec -it script-runner python /app/scripts/kafka_consumer.py consumer2 8002
 docker exec -it script-runner python /app/scripts/kafka_consumer.py consumer3 8003

Go to web browser to view the indexing:
Open http://localhost:5601 - for Kibana

For Monitoring and Visualisation of Metrics:
Prometheus: localhost:9090 (Target Health for monitoring the instances)

Grafana: http://localhost:3000 (need to login with id and password being admin, create a new dashboard to visualise as needed)



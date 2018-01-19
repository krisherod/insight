# insight

Project Idea:

Detect methane leaks using a large array of gas sensors and identify faulty sensors in real time.


Use Cases:

1. To detect if there is a high methane event by checking if two collocated sensors are reading high values within 10 seconds
2. Throw out values if there is no agreement between collocated sensors
3. Identify if sensors are consistently reading faulty values compared to surrounding sensors


Technologies

- Kafka: for sensor data ingestion
- Flink: for processing a stream of data
- PostgreSQL: for storage of data
- Flask: for visualization of alerts, and faulty sensors


What are the primary engineering challenges

- It has to be low latency, stations need to be compared to each other within seconds


Proposed architecture

S3 (to store sensor values) -> Kafka (to ingest sensor data) -> Flink (perform calculations on streaming data) -> PostgreSQL (store status for all sensors) -> Flask (visualize data)

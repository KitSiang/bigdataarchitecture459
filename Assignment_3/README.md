# Assignment 3

## Instructions

1. Clone the content of Assignment 3 and navigate into the `spiders` folder where `spider.py` is located.
2. Run the command, `scrapy runspider spider.py`.
3. Navigate into Kafka folder, `cd kafka_2.12-2.8.0`.
3. Start Zookeeper and Kafka environment with (a) `bin/zookeeper-server-start.sh config/zookeeper.properties` and (b) `bin/kafka-server-start.sh config/server.properties
4. Run the command, `bin/kafka-console-consumer.sh --topic scrapy-output --from-beginning --bootstrap-server localhost:9092`.
5. Run the command, `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 spark-kafka-streaming.py`.
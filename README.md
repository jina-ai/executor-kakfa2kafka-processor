# Kafka Executor
[Kafka](https://kafka.apache.org) is very powerful message bus platform used for various applications. This demo repository has simple executors integrating with 
Kafka Consumer and Producer for processing generic json messages into DocArray and from json to Document json messages.

# Install and initialize Kafka locally

Install requirements using:
```shell
pip install -r requirements.txt
```

A sample Kafka app is provided in the [tests/docker-compose.yml](tests/docker-compose.yml). Local Kafka application can be started by running `docker-compose -f tests/docker-compose up -d`.

For creating the topics and sample data for the demo, run:
```shell
python init_kafka_topics.py
```

The script creates one topic `input_raw_docs` and adds a simple json message with a key and text field containing random characters. The second topic `enriched_docs` is created and the data will be populated by the producer executor.

To remove all data in Kafka, run:
```shell
python delete_topics.py
```

## KafkaToKafka Executor

Simpler consumer and producer executor that consumes raw messages/documents from the CONSUMER_TOPIC and write the processed Docarray to the PRODUCER_TOPIC. A BATCH_SIZE environment is also provided to configure the publish batch size. On a `/` post request, the batch raw documents are converted to a Document and a random embedding is added to the document. A Docarray is created from the batch which is then published as json to the PRODUCER_TOPIC.

# Flow

A sample flow can be triggered by running

```shell
python run_kafka2kafka_flow.py
```
import json
import time

import pytest
from docarray import Document
from jina import Flow
from kafka import KafkaConsumer
from kafka2kafka import KafkaToKafka


@pytest.mark.flaky(reruns=3)
def test_flow(
    docker_compose,
    bootstrap_servers,
    consumer_topic,
    producer_topic,
    num_messages,
    batch_size,
):
    f = Flow().add(
        uses=KafkaToKafka,
        env={
            'BOOTSTRAP_SERVERS': bootstrap_servers,
            'CONSUMER_TOPIC': consumer_topic,
            'PUBLISH_TOPIC': producer_topic,
            'BATCH_SIZE': batch_size,
        },
    )

    with f:
        f.post(on='/')
        for _ in range(12):
            f.post(on='/')

        time.sleep(2)

    consumer = KafkaConsumer(
        producer_topic,
        group_id='console-consumer',
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=3000,
    )

    topic_partitions = consumer.poll(
        timeout_ms=2000, max_records=num_messages, update_offsets=True
    )
    consumer_records = []  # flatten all consumer records
    for crs in topic_partitions.values():
        consumer_records.extend(crs)

    assert len(consumer_records) == num_messages

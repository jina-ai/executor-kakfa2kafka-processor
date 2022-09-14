import time

from jina import Flow

from init_kafka_topics import (
    BOOTSTRAP_SERVERS,
    CONSUME_TOPIC,
    PUBLISH_TOPIC,
    print_messages_in_topic,
)
from kafka2kafka import Kafka2Kafka

f = Flow().add(
    uses=Kafka2Kafka,
    env={
        'BOOTSTRAP_SERVERS': BOOTSTRAP_SERVERS,
        'CONSUMER_TOPIC': CONSUME_TOPIC,
        'PUBLISH_TOPIC': PUBLISH_TOPIC,
        'BATCH_SIZE': 4,
    },
)

with f:
    f.post(on='/')
    for index in range(12):
        response = f.post(on='/')

    time.sleep(2)
    print_messages_in_topic(PUBLISH_TOPIC)

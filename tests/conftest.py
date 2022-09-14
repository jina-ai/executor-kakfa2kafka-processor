import json
import os
import random
import string
import time

import pytest
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

cur_dir = os.path.dirname(os.path.abspath(__file__))
compose_yml = os.path.abspath(os.path.join(cur_dir, 'docker-compose.yml'))


@pytest.fixture(scope='module')
def bootstrap_servers():
    return 'localhost:9092'


@pytest.fixture(scope='module')
def consumer_topic():
    return 'test_input_raw_docs'


@pytest.fixture(scope='module')
def producer_topic():
    return 'test_enriched_docs'


@pytest.fixture(scope='module')
def num_messages():
    return 50


@pytest.fixture(scope='module')
def batch_size():
    return 5


@pytest.fixture(scope='module')
def docker_compose(bootstrap_servers, consumer_topic, producer_topic, num_messages):
    os.system(
        f"docker-compose -f {compose_yml} --project-directory . up  --build -d --remove-orphans"
    )
    is_ready = False
    required_topics = [consumer_topic, producer_topic]

    while not is_ready:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            existing_topics = admin_client.list_topics()
            for topic in required_topics:
                if topic not in existing_topics:
                    new_topics = [
                        NewTopic(name=topic, num_partitions=1, replication_factor=1)
                    ]
                    admin_client.create_topics(new_topics=new_topics)
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda m: json.dumps(m).encode('utf-8'),
            )

            for index in range(num_messages):
                json_doc = {
                    'key': index,
                    'text': ''.join(
                        random.choices(string.ascii_uppercase + string.digits, k=20)
                    ),
                }
                producer.send(topic=consumer_topic, key=bytes(index), value=json_doc)

            producer.flush(timeout=2)
            is_ready = True
        except:
            time.sleep(0.5)

    yield
    os.system(
        f"docker-compose -f {compose_yml} --project-directory . down --remove-orphans"
    )

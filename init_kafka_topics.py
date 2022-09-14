import json
import random
import string

from docarray import Document
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

BOOTSTRAP_SERVERS = 'localhost:9092'

CONSUME_TOPIC = 'input_raw_docs'
PUBLISH_TOPIC = 'enriched_docs'
TOPICS = [CONSUME_TOPIC, PUBLISH_TOPIC]
NUM_MESSAGES = 50


def print_messages_in_topic(topic: str):
    print(f'Created following messages with text field in "{topic}" kafka topic')
    consumer = KafkaConsumer(
        topic,
        group_id='console-consumer',
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        max_poll_records=NUM_MESSAGES,
        consumer_timeout_ms=3000,
    )
    for message in consumer:
        try:
            d = Document.from_json(message.value)
            print(f'{d.text}')
        except:
            print(f'{message.value["text"]}')


def main():

    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    existing_topics = admin_client.list_topics()

    missing_topics = [t for t in TOPICS if t not in existing_topics]
    if len(missing_topics) > 0:
        new_topics = [
            NewTopic(name=t, num_partitions=1, replication_factor=1)
            for t in missing_topics
        ]
        admin_client.create_topics(new_topics=new_topics)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda m: json.dumps(m).encode('utf-8'),
    )

    for index in range(NUM_MESSAGES):
        json_doc = {
            'key': index,
            'text': ''.join(
                random.choices(string.ascii_uppercase + string.digits, k=20)
            ),
        }
        producer.send(topic=CONSUME_TOPIC, key=bytes(index), value=json_doc)

    producer.flush(timeout=1)
    print_messages_in_topic(CONSUME_TOPIC)

    admin_client.close()


if __name__ == '__main__':
    main()

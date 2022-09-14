from kafka import KafkaAdminClient

from init_kafka_topics import BOOTSTRAP_SERVERS, TOPICS


def main():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    admin_client.delete_topics(topics=TOPICS)
    admin_client.close()


if __name__ == '__main__':
    main()

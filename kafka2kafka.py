import json
import os
from typing import Dict, Optional

import numpy as np
from docarray import Document, DocumentArray
from jina import Executor, requests
from jina.logging.logger import JinaLogger
from kafka import KafkaConsumer, KafkaProducer


class KafkaToKafka(Executor):
    '''Publishes the input docarray into the specified topic. Each Document is published as a single Kafka message.'''

    def __init__(
        self,
        metas: Optional[Dict] = None,
        requests: Optional[Dict] = None,
        runtime_args: Optional[Dict] = None,
        workspace: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(metas, requests, runtime_args, workspace, **kwargs)
        self._consumer_topic = os.environ['CONSUMER_TOPIC']
        self._producer_topic = os.environ['PUBLISH_TOPIC']
        self._batch_size = int(os.environ['BATCH_SIZE'])
        self.logger = JinaLogger(self.metas.name)
        self._consumer = None
        self._producer = None
        self.__init_consumer_producer = True

    def __lazy_init(self):
        self._consumer = KafkaConsumer(
            self._consumer_topic,
            group_id='kafka2docarray',
            bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )
        self._producer = KafkaProducer(
            bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
            value_serializer=lambda m: json.dumps(m).encode('utf-8'),
        )
        self.__init_consumer_producer = False

    @requests
    def enrich(self, **kwargs):
        if self.__init_consumer_producer:
            self.__lazy_init()

        topic_partitions = self._consumer.poll(
            timeout_ms=0, max_records=self._batch_size, update_offsets=True
        )
        consumer_records = []  # flatten all consumer records
        for crs in topic_partitions.values():
            consumer_records.extend(crs)

        values = [record.value for record in consumer_records]

        if len(values) > 0:
            for batch in [
                values[x : x + self._batch_size]
                for x in range(0, len(values), self._batch_size)
            ]:
                for raw_doc in batch:
                    d = Document.from_dict(raw_doc)
                    d.embedding = np.random.randn(128)
                    d.tags = {'key': raw_doc['key']}
                    key = d.tags['key']
                    self._producer.send(
                        topic=self._producer_topic,
                        key=bytes(int(key)),
                        value=d.to_json(),
                    )  # async buffering
                # block until batch is published
                self._producer.flush(timeout=1)

            self._producer.flush(timeout=1)

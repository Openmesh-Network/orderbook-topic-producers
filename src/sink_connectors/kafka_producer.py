import json

from confluent_kafka import Producer, KafkaError, KafkaException
from helpers.read_config import get_producer_config

class KafkaProducer():
    def __init__(self, topic):
        self.topic = topic
        self.producer = Producer(get_producer_config())

    def _ack(self, err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (msg.topic(), msg.partition()))

    def produce(self, key, msg):
        if isinstance(msg, dict) or isinstance(msg, list):
            msg = json.dumps(msg).encode('utf-8')
        self.producer.produce(self.topic, key=key, value=msg, on_delivery=self._ack)
        self.producer.poll(0)
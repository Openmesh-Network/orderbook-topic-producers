from confluent_kafka import Consumer, KafkaError, KafkaException

from helpers.read_config import get_consumer_config

class KafkaConsumer():
    def __init__(self, topic):
        self.topic = topic
        self.consumer = Consumer(get_consumer_config())
        self.consumer.subscribe([topic])

    def consume(self):
        msg = self.consumer.poll(0)
        if msg is None:
            return

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            return msg.value()
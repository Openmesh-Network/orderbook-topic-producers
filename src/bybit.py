import json
import time

from orderbooks.l2 import L2Lob
from orderbooks.orderbook import LobUpdateError
from source_connectors.kafka_consumer import KafkaConsumer 
from sink_connectors.kafka_producer import KafkaProducer

def enrich_quote(quote, lob_event):
    quote['quote_no'] = lob_event['quote_no']
    quote['event_timestamp'] = lob_event['send_timestamp']
    quote['receive_timestamp'] = lob_event['receive_timestamp']
    quote['send_timestamp'] = str(int(time.time() * 10**3))
    return quote

def run_lob():
    consumer = KafkaConsumer('bybit-normalised')
    producer = KafkaProducer('bybit-L1')
    orderbook = L2Lob()
    while True:
        message = consumer.consume()
        if not message is None:
            lob_event = json.loads(message)
            if 'quote_no' in lob_event.keys():
                try:
                    orderbook.handle_event(lob_event)
                except LobUpdateError:
                    # No snapshot, so need to build orderbook asymptotically.
                    pass 
                quote = orderbook.book_top()
                quote = enrich_quote(quote, lob_event)
                producer.produce(str(lob_event['quote_no']), quote)

if __name__ == "__main__":
    run_lob()
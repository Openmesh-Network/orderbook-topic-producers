import json
import time

from orderbooks.l2 import L2Lob
from orderbooks.orderbook import LobUpdateError
from source_connectors.kafka_consumer import KafkaConsumer 
from sink_connectors.kafka_producer import KafkaProducer

def enrich_quote(quote, lob_event):
    quote['event_no'] = lob_event['event_no']
    quote['quote_no'] = lob_event['quote_no']
    quote['event_timestamp'] = lob_event['send_timestamp']
    quote['receive_timestamp'] = lob_event['receive_timestamp']
    quote['send_timestamp'] = str(int(time.time() * 10**3))
    return quote

def run_lob():
    consumer = KafkaConsumer('bybit-normalised')
    producer = KafkaProducer('bybit-L1')
    orderbook = L2Lob()
    current_event_no = -1
    while True:
        message = consumer.consume()
        if not message is None:
            lob_event = json.loads(message)
            if 'quote_no' in lob_event.keys():
                # Only produce if event has been completely processed
                if lob_event['event_no'] > current_event_no:
                    current_event_no = lob_event['event_no']
                    quote = orderbook.book_top()
                    quote = enrich_quote(quote, lob_event)
                    producer.produce(str(lob_event['event_no']), quote)

                try:
                    orderbook.handle_event(lob_event)
                except LobUpdateError:
                    # No snapshot, so need to build orderbook asymptotically.
                    pass 

if __name__ == "__main__":
    run_lob()
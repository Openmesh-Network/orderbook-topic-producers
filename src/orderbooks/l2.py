import json

from .orderbook import Orderbook, LobUpdateError
from .lob_enums import *


class L2Lob(Orderbook):
    def __init__(self):
        self.bids = {}
        self.asks = {}
        self.highest_bid = {'price': -1, 'size': -1}
        self.lowest_ask = {'price': 10e10, 'size': -1}

    def handle_event(self, event: dict):
        self._check_fields(event)
        lob_action, side, size, price = self._unpack_event(event)
        book = self.bids if side == BID else self.asks
        if lob_action == INSERT:
            self._insert(book, size, price)
        elif lob_action == REMOVE:
            self._remove(book, price)
        elif lob_action == UPDATE:
            self._update(book, size, price)
        else:
            return
        self._update_best(lob_action, side, size, price)

    def book_top(self) -> dict:
        return {
            'bid_price': self.highest_bid['price'],
            'bid_size': self.highest_bid['size'],
            'ask_price': self.lowest_ask['price'],
            'ask_size': self.lowest_ask['size']
        }
    
    def _insert(self, book, size, price):
        if price in book.keys():
            raise LobUpdateError("Inserting when price level already exists")
        book[price] = size

    def _remove(self, book, price):
        if price not in book.keys():
            raise LobUpdateError("Removing when price level doesn't exist")
        del book[price]

    def _update(self, book, size, price):
        if price not in book.keys():
            self._insert(book, size, price) # Building LOB with no snapshot
            raise LobUpdateError("Updating when price level doesn't exist")
        book[price] = size
    
    def _update_best(self, lob_action, side, size, price):
        if len(self.bids) == 0 or len(self.asks) == 0:
            return
        if lob_action in (INSERT, UPDATE):
            if side == BID:
                if price >= self.highest_bid['price']:
                    self.highest_bid = {'price': price, 'size': size}
                    self._correct_quotes(side)
            elif side == ASK:
                if price <= self.lowest_ask['price']:
                    self.lowest_ask = {'price': price, 'size': size}
                    self._correct_quotes(side)
        elif lob_action == REMOVE:
            if side == BID:
                if price == self.highest_bid['price']:
                    new_highest_bid = max(self.bids.keys())
                    self.higest_bid = {'price': new_highest_bid, 'size': self.bids[new_highest_bid]}
                    self._correct_quotes(side)
            elif side == ASK:
                if price == self.lowest_ask['price']:
                    new_lowest_ask = min(self.asks.keys())
                    self.lowest_ask = {'price': new_lowest_ask, 'size': self.asks[new_lowest_ask]}
                    self._correct_quotes(side)
    
    def _correct_quotes(self, side):
        if side == BID:
            # This can be made much faster with an implementation which directly filters 
            # the book prices into a new dict, then setting the new dict as the new book side.
            to_delete = [ask_price for ask_price in self.asks.keys() if ask_price <= self.highest_bid['price']]
            for ask_price in to_delete:
                del self.asks[ask_price]
            if len(self.asks) > 0:
                new_lowest_ask = min(self.asks.keys())
                self.lowest_ask = {'price': new_lowest_ask, 'size': self.asks[new_lowest_ask]}
            else:
                self.lowest_ask = {'price': 10e10, 'size': -1}
        elif side == ASK:
            to_delete = [bid_price for bid_price in self.bids.keys() if bid_price >= self.lowest_ask['price']]
            for bid_price in to_delete:
                del self.bids[bid_price]
            if len(self.bids) > 0:
                new_highest_bid = max(self.bids.keys())
                self.highest_bid = {'price': new_highest_bid, 'size': self.bids[new_highest_bid]}
            else:
                self.highest_bid = {'price': -1, 'size': -1}

    def snapshot(self):
        lob = {'bids': self.bids, 'asks': self.asks}
        return json.dumps(lob)
    
    def _check_fields(self, event):
        keys = event.keys()
        if 'price' not in keys or \
                'lob_action' not in keys or \
                'side' not in keys or \
                'size' not in keys:
            raise KeyError("Key is not present in LOB event.")
    
    def _unpack_event(self, event: dict):
        return event['lob_action'], event['side'], event['size'], event['price']
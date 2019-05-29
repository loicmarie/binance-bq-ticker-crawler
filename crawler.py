from binance.client import Client
from binance.websockets import BinanceSocketManager
from google.cloud import bigquery
import json
import time

with open('config.json', 'r') as f:
    config = json.load(f)

BINANCE_API_KEY    = config['DEFAULT']['BINANCE']['API_KEY']
BINANCE_SECRET_KEY = config['DEFAULT']['BINANCE']['SECRET_KEY']

bq_client = bigquery.Client()
client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
dataset_id = 'binance_exchange'

class Stream:
    def __init__(self, name, table_id):
        self.name = name
        self.table_id = table_id
        self.table = bq_client.dataset(dataset_id).table(self.table_id)
    def write_to_bq(self, data):
        errors = bq_client.insert_rows(self.table, data, selected_fields=self.schema)
    def get_fields(self):
        return []
class AggregateTradeStream(Stream):
    def __init__(self, symbol):
        Stream.__init__(self, '%s@aggTrade' % symbol, '%s_agg_trade' % symbol)
        self.schema = [
            bigquery.SchemaField('time', 'TIMESTAMP'),
            bigquery.SchemaField('agg_trade_id', 'INTEGER'),
            bigquery.SchemaField('price', 'FLOAT'),
            bigquery.SchemaField('quantity', 'FLOAT'),
            bigquery.SchemaField('first_trade_id', 'INTEGER'),
            bigquery.SchemaField('last_trade_id', 'INTEGER'),
            bigquery.SchemaField('trade_time', 'TIMESTAMP'),
            bigquery.SchemaField('is_buyer_market_maker', 'BOOLEAN')
        ]
    def process(self, msg):
        return [(
            int(msg['E']) / 1000., # Event time
            msg['a'],              # Aggregate trade ID
            msg['p'],              # Price
            msg['q'],              # Quantity
            msg['f'],              # First trade ID
            msg['l'],              # Last trade ID
            msg['T'] / 1000.,      # Trade time
            msg['m'] == 'true'     # Is the buyer the market maker ?
        )]

class TradeStream(Stream):
    def __init__(self, symbol):
        Stream.__init__(self, '%s@trade' % symbol, '%s_trade' % symbol)
        self.schema = [
            bigquery.SchemaField('time', 'TIMESTAMP'),
            bigquery.SchemaField('trade_id', 'INTEGER'),
            bigquery.SchemaField('price', 'FLOAT'),
            bigquery.SchemaField('quantity', 'FLOAT'),
            bigquery.SchemaField('buyer_order_id', 'INTEGER'),
            bigquery.SchemaField('seller_order_id', 'INTEGER'),
            bigquery.SchemaField('trade_time', 'TIMESTAMP'),
            bigquery.SchemaField('is_buyer_market_maker', 'BOOLEAN')
        ]
    def process(self, msg):
        return [(
            int(msg['E']) / 1000., # Event time
            msg['t'],              # Trade ID
            msg['p'],              # Price
            msg['q'],              # Quantity
            msg['b'],              # Buyer order ID
            msg['a'],              # Seller order ID
            int(msg['T']) / 1000., # Trade time
            msg['m'] == 'true'     # Is the buyer the market maker ?
        )]

class IndividualSymbolMiniTickerStream(Stream):
    def __init__(self, symbol):
        Stream.__init__(self, '%s@miniTicker' % symbol, '%s_mini_ticker' % symbol)
        self.schema = [
            bigquery.SchemaField('time', 'TIMESTAMP'),
            bigquery.SchemaField('close_price', 'FLOAT'),
            bigquery.SchemaField('open_price', 'FLOAT'),
            bigquery.SchemaField('high_price', 'FLOAT'),
            bigquery.SchemaField('low_price', 'FLOAT'),
            bigquery.SchemaField('base_volume', 'FLOAT'),
            bigquery.SchemaField('quote_volume', 'FLOAT')
        ]
    def process(self, msg):
        return [(
            int(msg['E']) / 1000.,   # Event time
            msg['c'],                # Close price
            msg['o'],                # Open price
            msg['h'],                # High price
            msg['l'],                # Low price
            msg['v'],                # Total traded base asset volume
            msg['q'],                # Total traded quote asset volume
        )]

class IndividualSymbolTickerStream(Stream):
    def __init__(self, symbol):
        Stream.__init__(self, '%s@ticker' % symbol, '%s_ticker' % symbol)
        self.schema = [
            bigquery.SchemaField('time', 'TIMESTAMP'),
            bigquery.SchemaField('price_change', 'FLOAT'),
            bigquery.SchemaField('price_change_percent', 'FLOAT'),
            bigquery.SchemaField('weighted_average_price', 'FLOAT'),
            bigquery.SchemaField('first_trade', 'FLOAT'),
            bigquery.SchemaField('last_trade', 'FLOAT'),
            bigquery.SchemaField('last_quantity', 'FLOAT'),
            bigquery.SchemaField('best_bid_price', 'FLOAT'),
            bigquery.SchemaField('best_bid_quantity', 'FLOAT'),
            bigquery.SchemaField('best_ask_price', 'FLOAT'),
            bigquery.SchemaField('best_ask_quantity', 'FLOAT'),
            bigquery.SchemaField('open_price', 'FLOAT'),
            bigquery.SchemaField('high_price', 'FLOAT'),
            bigquery.SchemaField('low_price', 'FLOAT'),
            bigquery.SchemaField('traded_base_volume', 'FLOAT'),
            bigquery.SchemaField('traded_quote_volume', 'FLOAT'),
            bigquery.SchemaField('stats_open_time', 'TIMESTAMP'),
            bigquery.SchemaField('stats_close_time', 'TIMESTAMP'),
            bigquery.SchemaField('first_trade_id', 'INTEGER'),
            bigquery.SchemaField('last_trade_id', 'INTEGER'),
            bigquery.SchemaField('total_num_trades', 'INTEGER')
        ]
    def process(self, msg):
        return [(
            int(msg['E']) / 1000.,   # Event time
            msg['p'],                # Price change
            msg['P'],                # Price change percent
            msg['w'],                # Weighted average price
            msg['x'],                # First trade(F)-1 price (first trade before the 24hr rolling window)
            msg['c'],                # Last price
            msg['Q'],                # Last quantity
            msg['b'],                # Best bid price
            msg['B'],                # Best bid quantity
            msg['a'],                # Best ask price
            msg['A'],                # Best ask quantity
            msg['o'],                # Open price
            msg['h'],                # High price
            msg['l'],                # Low price
            msg['v'],                # Total traded base asset volume
            msg['q'],                # Total traded quote asset volume
            msg['O'] / 1000.,        # Statistics open time
            msg['C'] / 1000.,        # Statistics close time
            msg['F'],                # First trade ID
            msg['L'],                # Last trade Id
            msg['n']                 # Total number of trades
            # msg['s']               # Symbol
        )]

class PartialBookDepthStream(Stream):
    def __init__(self, symbol, levels):
        Stream.__init__(self, '%s@depth%d' % (symbol, levels), '%s_depth' % symbol)
        self.schema = [
            bigquery.SchemaField('time', 'TIMESTAMP'),
            bigquery.SchemaField('price_level', 'FLOAT'),
            bigquery.SchemaField('quantity', 'FLOAT'),
            bigquery.SchemaField('is_bid', 'BOOLEAN')
        ]
    def process(self, msg):
        data = []
        timestamp = time.time()
        for i in range(len(msg['bids'])): # Bids to be updated
            data += [(
                timestamp,                # Event time
                msg['bids'][i][0],        # Price level to be updated
                msg['bids'][i][1],        # Quantity
                True                      # Is bid
            )]
        for i in range(len(msg['asks'])): # Asks to be updated
            data += [(
                timestamp,                # Event time
                msg['asks'][i][0],        # Price level to be updated
                msg['asks'][i][1],        # Quantity
                False                     # Is bid
            )]
        return data

class StreamsManager:
    def __init__(self, streams):
        self.streams = {stream.name: stream for stream in streams}
        self.bm = BinanceSocketManager(client)
        self.conn_key = self.bm.start_multiplex_socket(self.get_stream_names(), self.process)
    def process(self, msg):
        data = self.streams[msg['stream']].process(msg['data'])
        self.streams[msg['stream']].write_to_bq(data)
    def get_stream_names(self):
        return list(self.streams.keys())
    def start(self):
        self.bm.start()

if __name__ == '__main__':
    symbol = 'ltcbtc'
    manager = StreamsManager([
        AggregateTradeStream(symbol),
        TradeStream(symbol),
        IndividualSymbolTickerStream(symbol),
        IndividualSymbolMiniTickerStream(symbol),
        PartialBookDepthStream(symbol, 20)
    ])
    manager.start()

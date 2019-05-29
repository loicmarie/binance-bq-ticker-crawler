from binance.client import Client
from binance.websockets import BinanceSocketManager
from google.cloud import bigquery
import json

with open('config.json', 'r') as f:
    config = json.load(f)

BINANCE_API_KEY    = config['DEFAULT']['BINANCE']['API_KEY']
BINANCE_SECRET_KEY = config['DEFAULT']['BINANCE']['SECRET_KEY']

bq_client = bigquery.Client()
client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)

dataset_id = 'binance_exchange'

table_id = 'ltc_btc_tickers'
table_ref = bq_client.dataset(dataset_id).table(table_id)
table = bq_client.get_table(table_ref)

def process_message(msg):
    global bq_client, table
    for ticker in msg:
        if ticker['s'] == 'LTCBTC':
            data = [(
                float(ticker['E']) // 10**9,
                float(ticker['c']),
                float(ticker['o']),
                float(ticker['h']),
                float(ticker['l']),
                float(ticker['v']),
                ticker['q']
            )]
            errors = bq_client.insert_rows(table, data)

bm = BinanceSocketManager(client)
conn_key = bm.start_miniticker_socket(process_message, update_time=1000)
bm.start()

import json
import sys
import os
import csv
import pytz
import pandas as pd
import multiprocessing
from datetime import datetime
from catalyst.api import record, symbol, symbols
from catalyst.utils.run_algo import run_algorithm

def handle_data(context, data):
    # Variables to record for a given asset: price and volume
    # Other options include 'open', 'high', 'open', 'close'
    # Please note that 'price' equals 'close'
    date   = context.blotter.current_dt     # current time in each iteration
    opened = data.current(context.asset, 'open')
    close = data.current(context.asset, 'close')
    high = data.current(context.asset, 'high')
    low = data.current(context.asset, 'low')
    volume = data.current(context.asset, 'volume')

    df = pd.DataFrame({'dt':[date], 'open':[opened], 'high':[high], 'low':[low], 'close':[close], 'volume':[volume]})
    context.ohlcv_df.append(df)

def parse_json(path, exchange):
    file_name = os.path.join(path, exchange, 'symbols.json')
    with open(file_name) as f:
        data = json.load(f)

    df = pd.DataFrame(data).T
    df = df[['symbol', 'start_date', 'end_minute']].set_index('symbol').reset_index()
    df.start_date = pd.to_datetime(df.start_date).apply(lambda x: pytz.utc.localize(x.to_pydatetime()))
    df.end_minute = pd.to_datetime(df.end_minute).apply(lambda x: pytz.utc.localize(x.to_pydatetime()))
    df['exchange'] = exchange
    return(df)

def run_algorithm_helper(args):
    sym, start_date, end_date, exchange = args
    def initialize(context):
        context.asset = symbol(sym)
        context.ohlcv_df = []

    def analyze(context=None, results=None):
        csv_file_name = os.path.join(os.getcwd(), 'data', sym + '.csv')
        df = pd.concat(context.ohlcv_df)
        df.to_csv(csv_file_name)
    
    print('Processing sym={} from {} to {} on {}'.format(sym, start_date, end_date, exchange))

    try:
        results = run_algorithm(initialize=initialize,
                            handle_data=handle_data,
                            analyze=analyze,
                            start=start_date,
                            end=end_date,
                            exchange_name=exchange,
                            data_frequency='minute',
                            base_currency='usdt',
                            capital_base=10000)
        return(results)
    except Exception as e:
        print(e)
        return(e)

def main():
    path = r'/Users/yowtzu/.catalyst/data/exchanges/'
    exchange = 'poloniex' # 'bitfinex'
    df = parse_json(path, exchange)
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() // 2)
    params = df.itertuples(index=False, name=None)
    pool.map(run_algorithm_helper, params)
    pool.close()

if __name__ == '__main__':
   main()

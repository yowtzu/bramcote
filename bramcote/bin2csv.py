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

    date   = pd.to_datetime(context.blotter.current_dt)     # current time in each iteration
    df_list = [ pd.DataFrame({'sym': [ticker], 
                              'dt': [date], 
                              'open': [data.current(sym, 'open')],
                              'high': [data.current(sym, 'high')],
                              'low': [data.current(sym, 'low')],
                              'close': [data.current(sym, 'close')],
                              'volume': [data.current(sym, 'volume')]
                             })
                for ticker, sym in context.assets ]
    df = pd.concat(df_list)
    context.ohlcv_df.append(df)

def parse_json(path, exchange):
    file_name = os.path.join(path, exchange, 'symbols.json')
    with open(file_name) as f:
        data = json.load(f)

    df = pd.DataFrame(data).T
    df = df[['symbol', 'start_date', 'end_minute']].reset_index(drop=True)
    
    expanded_df_list = []
    for _, (sym, start_date, end_minute) in df.iterrows():
        start_dates= pd.date_range(start_date, end_minute)
        end_dates = start_dates

        expanded_df = pd.DataFrame({'start_date': start_dates, 'end_date': end_dates})
        expanded_df.start_date = pd.to_datetime(expanded_df.start_date).apply(lambda x: pytz.utc.localize(x.to_pydatetime()))
        expanded_df.end_date = pd.to_datetime(expanded_df.end_date).apply(lambda x: pytz.utc.localize(x.to_pydatetime()))
        expanded_df['sym'] = sym
        expanded_df['exchange'] = exchange
        expanded_df_list.append(expanded_df)

    res = pd.concat(expanded_df_list)
    res = res[['sym', 'exchange', 'start_date', 'end_date']].sort_values(['start_date', 'sym'], ascending=False)
    return res

def run_algorithm_helper(args):
    syms, exchange, start_date, end_date = args
    def initialize(context):
        context.assets = [ (sym, symbol(sym)) for sym in syms ]
        context.ohlcv_df = []

    def analyze(context=None, results=None):
        path = os.path.join(os.getcwd(), 'data', exchange)
        print(path)
        os.makedirs(path, exist_ok=True)
        csv_file_name = os.path.join(path, start_date.strftime('%Y%M%d') + '.csv')
        df = pd.concat(context.ohlcv_df)
        df = df[['sym', 'dt', 'open', 'high', 'low', 'close', 'volume']].sort_values(['sym', 'dt'])
        df.to_csv(csv_file_name, index=False)
    
    print('Processing sym={} from {} to {} on {}'.format(syms, start_date, end_date, exchange))

    #try:
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
    #except Exception as e:
    #    print('Error: {}'.format(e))
    #    return(e)

def main():
    path = r'/Users/yowtzu/.catalyst/data/exchanges/'
    exchanges = ['poloniex'] # , 'bitfinex']

    df = pd.concat((parse_json(path, exchange) for exchange in exchanges))
    gb = df.groupby(['exchange', 'start_date'])
    
    params = []

    for ((exchange, date), df_sub) in gb:
        syms = df_sub.sym.values.tolist()
        params.append((syms, exchange, date, date))
    params = params[::-1]
    # run_algorithm_helper(params[0])
    pool = multiprocessing.Pool(multiprocessing.cpu_count() // 2)
    # params = df.itertuples(index=False, name=None)
    pool.map(run_algorithm_helper, params)
    pool.close()

if __name__ == '__main__':
   main()

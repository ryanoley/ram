
import os
import pandas as pd
import datetime as dt
from dateutil import parser

from ram import config
from ram.data.data_handler_sql import DataHandlerSQL
from ramex.accounting.daily_files import get_ramex_processed_data

BASE_DIR = os.path.join(config.IMPLEMENTATION_DATA_DIR, 'StatArbStrategy')
PRICE_DIR = os.path.join(BASE_DIR, 'archive', 'live_pricing')
RECON_DIR = os.path.join(BASE_DIR, 'archive', 'reconciliation')


def get_live_prices(px_dt, price_dir=PRICE_DIR):

    if not isinstance(px_dt, dt.date):
        px_dt = parser.parse(str(px_dt)).date()

    file_name = px_dt.strftime('%Y%m%d') + '_live_pricing.csv'
    file_path = os.path.join(price_dir, file_name)

    if not os.path.exists(file_path):
        raise IOError("No live_pricing file for: " + str(px_dt))

    live_prices = pd.read_csv(file_path)

    return live_prices


def ramex_merge_live(ramex_data, live_prices):

    trades = ramex_data.rename(columns={
                               'symbol':'Ticker',
                               'avg_px':'exec_price'})
    trades = trades[['Ticker', 'strategy_id', 'quantity', 'exec_shares',
                     'exec_price']]

    prices = live_prices.rename(columns={
        'AdjClose':'rt_close',
        'AdjVolume':'rt_volume',
        'AdjVwap':'rt_vwap'
    })
    prices = prices[['SecCode', 'Ticker', 'rt_close', 'rt_volume', 'rt_vwap']]

    merged_data = pd.merge(trades, prices, how='left')

    return merged_data


def get_qad_fields(data, inp_date=None):

    assert('SecCode' in data.columns)
    seccodes = data.SecCode.values

    dh = DataHandlerSQL()

    if inp_date is None:
        data_dt = dh.prior_trading_date()
    else:
        data_dt = inp_date

    features = ['RClose', 'RVolume', 'MarketCap', 'AdjClose']

    qad_data = dh.get_seccode_data(seccodes, features, data_dt, data_dt)

    qad_data.rename(columns={
    'RClose':'qad_close',
    'RVolume':'qad_volume',
    'AdjClose':'qad_adj_close',
    'MarketCap':'qad_market_cap'}, inplace=True)

    return qad_data[['SecCode', 'Date', 'qad_close', 'qad_adj_close',
                     'qad_volume', 'qad_market_cap']]


def _write_output(data, recon_dt, output_dir=RECON_DIR):

    if not isinstance(recon_dt, dt.date):
        recon_dt = parser.parse(str(recon_dt))

    timestamp = recon_dt.strftime('%Y%m%d')
    path = os.path.join(output_dir, '{}_pricing_recon.csv'.format(timestamp))

    output_columns = ['Ticker', 'SecCode', 'Date', 'strategy_id',
                      'quantity', 'exec_shares', 'exec_price',
                      'rt_close', 'rt_volume', 'rt_vwap', 'qad_close',
                      'qad_adj_close', 'qad_volume', 'qad_market_cap']
    data =data[output_columns]
    data.to_csv(path, index=False)
    return

def main():


    #####################################################################

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-rd', '--recon_date', default='',
        help='date to run reconciliation for')
    args = parser.parse_args()

    #####################################################################

    import ipdb; ipdb.set_trace()

    # Get Executed prices
    ramex_data = get_ramex_processed_data(dt.date(2018,4,27))[0]

    # Get live prices
    live_prices = get_live_prices(dt.date(2018,4,27))

    # Merge executed and signal prices
    trade_data = ramex_merge_live(ramex_data, live_prices)

    # QAD data for trade date
    qad_data = get_qad_fields(trade_data, dt.date(2018,4,27))

    # Combine trade data and qad data
    recon = trade_data.merge(qad_data, how='left')

    # Write to file
    _write_output(recon, dt.date(2018,4,27), 'C:/temp')



if __name__ == '__main__':
    main()
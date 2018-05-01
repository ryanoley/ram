
import os
import argparse
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
                               'symbol': 'Ticker',
                               'avg_px': 'exec_price'})
    trades = trades[['Ticker', 'strategy_id', 'quantity', 'exec_shares',
                     'exec_price']]

    prices = live_prices.rename(columns={
        'AdjClose': 'signal_close',
        'AdjVolume': 'signal_volume'
    })
    prices = prices[['SecCode', 'Ticker', 'signal_close', 'signal_volume']]

    merged_data = pd.merge(trades, prices, how='left')

    return merged_data


def get_qad_data(data, inp_date):

    assert('SecCode' in data.columns)
    seccodes = data.SecCode.values
    features = ['RClose', 'RVolume', 'MarketCap', 'AdjClose']

    dh = DataHandlerSQL()
    qad_data = dh.get_seccode_data(seccodes, features, inp_date, inp_date)

    qad_data.rename(columns={
                    'RClose': 'qad_close',
                    'RVolume': 'qad_volume',
                    'AdjClose': 'qad_adj_close',
                    'MarketCap': 'qad_market_cap'}, inplace=True)

    return qad_data[['SecCode', 'Date', 'qad_close', 'qad_adj_close',
                     'qad_volume', 'qad_market_cap']]


def _write_output(data, recon_dt, output_dir=RECON_DIR):

    if not isinstance(recon_dt, dt.date):
        recon_dt = parser.parse(str(recon_dt))

    timestamp = recon_dt.strftime('%Y%m%d')
    path = os.path.join(output_dir, '{}_pricing_recon.csv'.format(timestamp))

    output_columns = ['Ticker', 'SecCode', 'Date', 'strategy_id',
                      'quantity', 'exec_shares', 'exec_price',
                      'signal_close', 'signal_volume','qad_close',
                      'qad_volume', 'qad_market_cap', 'qad_adj_close']
    data = data[output_columns]
    data.to_csv(path, index=False)
    return


def main():

    #####################################################################
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        '-rd', '--recon_date', default='',
        help='Date to run reconciliation for')
    args = arg_parser.parse_args()

    #####################################################################
    # If no date is passed then get the last trading date
    dh = DataHandlerSQL()

    if args.recon_date == '':
        recon_dt = dh.prior_trading_date()
    else:
        recon_dt = parser.parse(args.recon_date)

    # Get Executed prices
    ramex_data = get_ramex_processed_data(recon_dt)[0]

    # Get live prices
    live_prices = get_live_prices(recon_dt)

    # Merge executed and signal prices
    trade_data = ramex_merge_live(ramex_data, live_prices)

    # QAD data for trade date
    qad_data = get_qad_data(trade_data, recon_dt)

    # Combine trade data and qad data
    recon = trade_data.merge(qad_data, how='left')

    # Write to file
    _write_output(recon, recon_dt, RECON_DIR)


if __name__ == '__main__':
    main()

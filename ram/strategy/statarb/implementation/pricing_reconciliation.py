
import os
import argparse
import pandas as pd
import datetime as dt
from dateutil import parser

from ram import config
from ram.data.data_handler_sql import DataHandlerSQL
from ramex.accounting.daily_files import get_ramex_processed_data
import ram.strategy.statarb.implementation.get_live_allocations as gla

BASE_DIR = os.path.join(config.IMPLEMENTATION_DATA_DIR, 'StatArbStrategy')
ARCHIVE_DIR = os.path.join(BASE_DIR, 'archive')
PRICE_DIR = os.path.join(ARCHIVE_DIR, 'live_pricing')
RECON_DIR = os.path.join(ARCHIVE_DIR, 'reconciliation')

############################################################################
# Pricing reconciliation
############################################################################


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

    trades = trades[['Ticker', 'strategy_id', 'quantity',
                     'exec_shares', 'exec_price']]

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

    data[output_columns].to_csv(path, index=False)


def run_pricing_reconciliation(recon_dt):
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


############################################################################
# Order level reconciliation
############################################################################

def get_qad_live_prices(data, inp_date):

    assert('SecCode' in data.columns)
    seccodes = data.SecCode.values
    features = ['TICKER', 'AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose',
                'AdjVolume', 'AdjVwap', 'RClose']

    dh = DataHandlerSQL()
    qad_data = dh.get_seccode_data(seccodes, features, inp_date, inp_date)

    qad_data.rename(columns={'TICKER':'Ticker'}, inplace=True)

    return qad_data[['SecCode', 'Ticker', 'AdjOpen', 'AdjHigh', 'AdjLow',
                     'AdjClose', 'AdjVolume', 'AdjVwap', 'RClose']]



def run_order_reconciliation(recon_dt):
    gla.LIVE_DIR = os.path.join(ARCHIVE_DIR, 'live_directories', '20180501_live')
    gla.STRATEGY_ID = 'StatArb1~papertrade'
    gla.WRITE_FLAG = False

    ###########################################################################
    # 0. Checks meta and import position size
    position_size = gla.get_position_size()

    # 1. Import raw data
    raw_data = gla.import_raw_data()

    # 2. Import raw data
    run_map = gla.import_run_map()

    # 3. Import sklearn models and model parameters
    models_params = gla.import_models_params()

    # 5. Get SizeContainers
    size_containers = gla.get_size_containers()

    # 6. Scaling data for live data
    scaling = gla.import_scaling_data()

    # 7. Prep data
    strategy = gla.StatArbImplementation()
    strategy.add_daily_data(raw_data)
    strategy.add_run_map_models(run_map, models_params)
    strategy.add_size_containers(size_containers)
    strategy.prep()

    # 8. Live pricing
    live_data = gla.get_live_pricing_data(scaling)

    qad_live_data = get_qad_live_prices(scaling, dt.date(2018, 5, 1))

    import ipdb; ipdb.set_trace()

    orders = strategy.run_live(live_data)



    out_df = orders.merge(live_data[['SecCode', 'Ticker', 'RClose']],
                          how='left')
    out_df['Dollars'] = \
        out_df.PercAlloc * position_size['gross_position_size']

    out_df['NewShares'] = (out_df.Dollars / out_df.RClose).astype(int)





def main():
    #####################################################################
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        '-rd', '--recon_date', default='',
        help='Date to run reconciliation for')

    arg_parser.add_argument(
        '-p', '--pricing', action='store_true',
        help='Reconcile daily pricing')

    arg_parser.add_argument(
        '-o', '--orders', action='store_true',
        help='Reconcile daily pricing')

    args = arg_parser.parse_args()

    #####################################################################
    dh = DataHandlerSQL()

    # Default is last trading date because close prices must exist in QAD
    if args.recon_date == '':
        recon_dt = dh.prior_trading_date()
    else:
        recon_dt = parser.parse(args.recon_date).date()

    if args.pricing:
        run_pricing_reconciliation(recon_dt)
    elif args.orders:
        run_order_reconciliation(recon_dt)





if __name__ == '__main__':
    main()

import os
import json
import argparse
import pandas as pd
import datetime as dt
from dateutil import parser

from ram import config
from ram.data.data_handler_sql import DataHandlerSQL
import ram.strategy.statarb.implementation.get_live_allocations as gla

import ramex.accounting.daily_files as dly_fls

IMP_DIR = config.IMPLEMENTATION_DATA_DIR
ARCHIVE_DIR = os.path.join(IMP_DIR, 'StatArbStrategy', 'archive')

STRATEGY_ID = 'StatArb0001'

############################################################################
# Pricing reconciliation
############################################################################


def run_pricing_reconciliation(recon_dt, strategy_id=STRATEGY_ID,
                               imp_dir=IMP_DIR):
    # Get Executed prices
    ramex_data = dly_fls.get_ramex_processed_data(recon_dt)[0]
    statarb_trades = ramex_data[ramex_data.strategy_id == STRATEGY_ID]

    # Get live prices
    live_prices = get_eze_signal_prices(recon_dt)

    # Replace Realtick Tickers with QAD
    path = os.path.join(imp_dir, 'qad_to_eze_ticker_map.json')
    qad_to_eze = json.load(open(path, 'r'))
    eze_to_qad = {v: k for k, v in qad_to_eze.iteritems()}
    live_prices.Ticker = live_prices.Ticker.replace(to_replace=eze_to_qad)

    # Merge executed and signal prices
    trade_data = merge_trades_signal_prices(statarb_trades, live_prices)

    # QAD data for trade date
    qad_data = get_qad_close_data(trade_data, recon_dt)

    # Combine trade data and qad data and append null SecCodes
    # This can occur when stocks leave the universe at the end of the month
    no_seccodes = trade_data[trade_data.SecCode.isnull()].copy()
    trade_data = trade_data[trade_data.SecCode.notnull()].copy()
    recon_data = trade_data.merge(qad_data, how='left')
    recon_data = recon_data.append(no_seccodes)

    # Write to file
    _write_pricing_output(recon_data, recon_dt)


def get_eze_signal_prices(inp_date, arch_dir=ARCHIVE_DIR):
    # Get live prices from archive
    if not isinstance(inp_date, dt.date):
        inp_date = parser.parse(str(inp_date)).date()

    pricing_dir = os.path.join(arch_dir, 'live_pricing')
    file_name = inp_date.strftime('%Y%m%d') + '_live_pricing.csv'
    file_path = os.path.join(pricing_dir, file_name)

    if not os.path.exists(file_path):
        raise IOError("No live_pricing file for: " + str(inp_date))

    live_prices = pd.read_csv(file_path)
    live_prices.SecCode = live_prices.SecCode.astype(int).astype(str)
    live_prices['captured_time'] = [parser.parse(x).time() for x
                                    in live_prices.captured_time]

    return live_prices.rename(columns={'ROpen': 'signal_open',
                                       'RHigh': 'signal_high',
                                       'RLow': 'signal_low',
                                       'RClose': 'signal_close',
                                       'RVolume': 'signal_volume',
                                       'RVwap': 'signal_vwap',
                                       'captured_time': 'signal_time'})


def merge_trades_signal_prices(trade_data, live_prices):
    trade_data.rename(columns={'symbol': 'Ticker',
                               'avg_px': 'exec_price'},
                      inplace=True)
    trade_data = trade_data[['Ticker', 'strategy_id', 'quantity',
                             'exec_shares', 'exec_price']]

    live_prices = live_prices[['SecCode', 'Ticker', 'signal_close',
                               'signal_volume', 'signal_time']]

    data = pd.merge(trade_data, live_prices, how='left')

    return data


def get_qad_close_data(data, inp_date):
    # Use SecCode to get QAD Pricing
    assert('SecCode' in data.columns)
    seccodes = data.SecCode.dropna().values
    features = ['RClose', 'RVolume', 'MarketCap', 'AdjClose']

    dh = DataHandlerSQL()
    qad_data = dh.get_seccode_data(seccodes, features, inp_date, inp_date)

    qad_data.rename(columns={'RClose': 'qad_close',
                             'RVolume': 'qad_volume',
                             'AdjClose': 'qad_adj_close',
                             'MarketCap': 'qad_market_cap'}, inplace=True)

    qad_data.SecCode = qad_data.SecCode.astype(int).astype(str)
    return qad_data[['SecCode', 'Date', 'qad_close', 'qad_adj_close',
                     'qad_volume', 'qad_market_cap']]


def _write_pricing_output(data, recon_dt, arch_dir=ARCHIVE_DIR):
    output_dir = os.path.join(arch_dir, 'reconciliation')
    datestamp = recon_dt.strftime('%Y%m%d')
    path = os.path.join(output_dir,
                        '{}_pricing_recon.csv'.format(datestamp))

    if(os.path.isfile(path)):
        timestamp = dt.datetime.utcnow().strftime('%H%M%S')
        path = os.path.join(output_dir,
                            '{}_pricing_recon_{}.csv'.format(datestamp,
                                                             timestamp))

    output_columns = ['Ticker', 'SecCode', 'Date', 'signal_time',
                      'strategy_id', 'quantity', 'exec_shares', 'exec_price',
                      'signal_close', 'signal_volume', 'qad_close',
                      'qad_volume', 'qad_market_cap', 'qad_adj_close']

    data[output_columns].to_csv(path, index=False)


############################################################################
# Order level reconciliation
############################################################################

def run_order_reconciliation(recon_dt, strategy_id=STRATEGY_ID):
    # Get orders with close data
    recon_orders = get_recon_orders(recon_dt)

    # Read sent orders from signal data
    exec_orders = get_executed_orders(recon_dt)
    exec_orders['strategy_id'] = strategy_id

    # Merge and write
    _write_order_output(recon_orders, exec_orders, recon_dt)


def get_recon_orders(recon_dt, arch_dir=ARCHIVE_DIR):
    # 0. Checks meta and import position size
    position_size = gla.get_position_size()['gross_position_size']

    # 1. Import raw data
    raw_data = gla.import_raw_data_archive(recon_dt)

    # 2. Get model dir name
    dir_name = '{}_live'.format(recon_dt.strftime('%Y%m%d'))
    path = os.path.join(arch_dir, 'live_directories', dir_name, 'meta.json')
    meta = json.load(open(path, 'r'))
    model_dir_name = meta['trained_models_dir_name']

    # 3. Import run map
    run_map = gla.import_run_map(model_dir_name)

    # 4. Import sklearn models and model parameters
    models_params = gla.import_models_params(model_dir_name)

    # 5. Get SizeContainers
    size_containers = gla.get_size_containers_archive(recon_dt)

    # 6. Scaling data for live data
    scaling = gla.import_scaling_data_archive(recon_dt)

    # 7. Prep data
    strategy = gla.StatArbImplementation()
    strategy.add_daily_data(raw_data)
    strategy.add_run_map_models(run_map, models_params)
    strategy.add_size_containers(size_containers)
    strategy.add_drop_short_seccodes([])
    strategy.prep()

    # 8. Orders using QAD closing px
    qad_live_data = get_qad_signal_prices(scaling, recon_dt)
    qad_orders = strategy.run_live(qad_live_data)
    qad_orders = qad_orders.merge(qad_live_data[['SecCode',
                                                 'Ticker',
                                                 'RClose']], how='left')
    qad_orders['Dollars'] = qad_orders.PercAlloc * position_size
    qad_orders['NewShares'] = (qad_orders.Dollars / qad_orders.RClose)
    qad_orders.NewShares = qad_orders.NewShares.astype(int)

    return rollup_orders(qad_orders, 'qad')


def get_qad_signal_prices(data, inp_date):
    assert('SecCode' in data.columns)

    # Get equity data
    seccodes = data.SecCode.dropna().values
    features = ['TICKER', 'AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose',
                'AdjVolume', 'AdjVwap', 'RClose']
    dh = DataHandlerSQL()
    qad_data = dh.get_seccode_data(seccodes, features, inp_date, inp_date)

    # Get index data
    ix_features = ['AdjClose']
    ix_seccodes = [50311, 11113]
    qad_ix_data = dh.get_index_data(ix_seccodes, ix_features, inp_date,
                                    inp_date)
    qad_ix_data.loc[qad_ix_data.SecCode == 50311, 'TICKER'] = '$SPX.X'
    qad_ix_data.loc[qad_ix_data.SecCode == 11113, 'TICKER'] = '$VIX.X'

    # Append and format
    qad_data = qad_data.append(qad_ix_data).reset_index(drop=True)
    qad_data.rename(columns={'TICKER': 'Ticker'}, inplace=True)
    qad_data.SecCode = qad_data.SecCode.astype(str)
    return qad_data[['SecCode', 'Ticker', 'AdjOpen', 'AdjHigh', 'AdjLow',
                     'AdjClose', 'AdjVolume', 'AdjVwap', 'RClose']]


def get_executed_orders(recon_dt, arch_dir=ARCHIVE_DIR):
    '''
    Return DataFrame with the sent allocations for a particular date
    '''
    alloc_dir = os.path.join(arch_dir, 'allocations')
    alloc_files = os.listdir(alloc_dir)
    alloc_files = [x for x in alloc_files if
                   os.path.isfile(os.path.join(alloc_dir, x))]
    file_name = dly_fls.get_filename_from_date(recon_dt, alloc_files)

    exec_orders = pd.read_csv(os.path.join(alloc_dir, file_name))
    exec_orders.SecCode = exec_orders.SecCode.astype(str)

    exec_orders.rename(columns={'Ticker':'exec_ticker',
                                'PercAlloc':'exec_perc_alloc',
                                'Dollars':'exec_dollars',
                                'RClose':'exec_close',
                                'NewShares':'exec_shares'}, inplace=True)

    exec_orders.drop(labels=['Shares', 'TradeShares'], axis=1, inplace=True)

    return exec_orders


def rollup_orders(order_df, col_prfx=None):
    assert(set(['SecCode', 'Ticker', 'PercAlloc', 'RClose', 'Dollars',
               'NewShares']).issubset(set(order_df.columns)))

    grp = order_df.groupby('SecCode')
    rollup = pd.DataFrame(index=order_df.SecCode.unique())

    col_prfx = col_prfx + '_' if col_prfx is not None else ''
    rollup['{}ticker'.format(col_prfx)] = grp.Ticker.min()
    rollup['{}perc_alloc'.format(col_prfx)] = grp.PercAlloc.sum()
    rollup['{}dollars'.format(col_prfx)] = grp.Dollars.sum()
    rollup['{}close'.format(col_prfx)] = grp.RClose.mean()
    rollup['{}shares'.format(col_prfx)] = grp.NewShares.sum()

    rollup.reset_index(inplace=True)
    rollup.rename(columns={'index': 'SecCode'}, inplace=True)
    return rollup


def _write_order_output(recon_orders, exec_orders, recon_dt,
                        arch_dir=ARCHIVE_DIR):
    output_dir = os.path.join(arch_dir, 'reconciliation')
    datestamp = recon_dt.strftime('%Y%m%d')
    path = os.path.join(output_dir, '{}_order_recon.csv'.format(datestamp))

    if(os.path.isfile(path)):
        timestamp = dt.datetime.utcnow().strftime('%H%M%S')
        path = os.path.join(output_dir,
                            '{}_order_recon_{}.csv'.format(datestamp,
                                                           timestamp))

    output_columns = ['SecCode', 'Date', 'strategy_id', 'exec_ticker',
                      'exec_perc_alloc', 'exec_dollars', 'exec_shares',
                      'exec_close', 'qad_ticker', 'qad_perc_alloc',
                      'qad_dollars', 'qad_shares', 'qad_close']

    data = pd.merge(recon_orders, exec_orders, how='outer')
    data['Date'] = recon_dt
    data[output_columns].to_csv(path, index=False)


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

    # Default is last trading date not today for QAD updates to process
    if args.recon_date == '':
        recon_dt = dh.prior_trading_date()
    else:
        recon_dt = parser.parse(args.recon_date).date()

    if args.pricing:
        run_pricing_reconciliation(recon_dt, strategy_id=STRATEGY_ID)
    if args.orders:
        run_order_reconciliation(recon_dt, strategy_id=STRATEGY_ID)


if __name__ == '__main__':
    main()

import os
import shutil
import unittest
import pandas as pd
import datetime as dt
from pandas.util.testing import assert_frame_equal

from ram.strategy.statarb.implementation.reconciliation_raw import *

class TestPricingReconciliation(unittest.TestCase):

    def setUp(self):

        self.test_dir = os.path.join(os.getenv('GITHUB'), 'ram', 'test_data')

        if os.path.isdir(self.test_dir):
            shutil.rmtree(self.test_dir)

        os.mkdir(self.test_dir)
        os.mkdir(os.path.join(self.test_dir, 'live_pricing'))
        # LIVE PRICES EXPORT FILE
        self.live_pricing = pd.DataFrame(data={
                            'SecCode': ['36799', '11027692', '1001', '30655'],
                            'Ticker': ['IBM', 'FB', 'BAC', 'GS'],
                            'ROpen': [100., 200., 50., 20.],
                            'RHigh': [105., 200., 60., 20.5],
                            'RLow': [95.5, 190., 40, 19.],
                            'RClose': [102.5, 195., 50., 20.],
                            'RVolume': [100, 200, 50, 20],
                            'RVwap': [102.5, 195.5, 48.5, 19.3],
                            'captured_time': [
                                '2018-05-07 15:44:37.01',
                                '2018-05-07 15:44:37.01',
                                '2018-05-07 15:44:37.01',
                                '2018-05-07 15:44:37.01']
                            })

        file_path = os.path.join(self.test_dir, 'live_pricing',
                                 '20180101_live_pricing.csv')
        self.live_pricing.to_csv(file_path, index=False)
        # RAMEX PROCESSED AGGREGATE FILE
        data = pd.DataFrame(data={
                            'Date':[dt.date(2018, 1, 1)] * 4,
                            'RAMID':['IBM_Q_tstStrat', 'FB_Q_tstStrat',
                                     'IBM_Q_tstStrat', 'GS_Q_tstStrat'],
                            'symbol': ['IBM', 'FB', 'BAC', 'GS'],
                            'strategy_id': ['tstStrat'] * 4,\
                            'trade_id': [0, 1, 2, 3],
                            'quantity': [10, -20, 30, -40],
                            'exec_shares': [10, -10, 30, -40],
                            'avg_px': [100., 101.5, 102, 103],
                            'basket': ['TestBasket'] *4,
                            'order_id': range(1000, 1004),
                            'order_type': ['vwap'] * 4})
        self.processed_detail = data



    def test_get_signal_prices(self):
        self.assertRaises(IOError, get_signal_prices,
                          dt.date.today(), self.test_dir)

        result = get_signal_prices('1/1/2018', self.test_dir)

        benchmark = self.live_pricing
        benchmark.rename(columns={'RClose': 'signal_close',
                                  'RVolume': 'signal_volume',
                                  'captured_time': 'signal_time'},
                        inplace=True)
        benchmark['signal_time'] = [dt.time(15, 44, 37, 10000)] * 4

        assert_frame_equal(result, self.live_pricing)

    def test_merge_trades_signal_prices(self):
        pricing_inp = get_signal_prices('1/1/2018', self.test_dir)
        result = merge_trades_signal_prices(self.processed_detail,
                                            pricing_inp)
        benchmark = pd.DataFrame(data={
                            'Ticker': ['IBM', 'FB', 'BAC', 'GS'],
                            'strategy_id': ['tstStrat'] * 4,
                            'quantity': [10, -20, 30, -40],
                            'exec_shares': [10, -10, 30, -40],
                            'exec_price': [100., 101.5, 102, 103],
                            'SecCode': ['36799', '11027692', '1001', '30655'],
                            'signal_close': [102.5, 195., 50., 20.],
                            'signal_volume': [100, 200, 50, 20],
                            'signal_time': [dt.time(15, 44, 37, 10000)] * 4})

        assert_frame_equal(result, benchmark[result.columns])

    def test_get_qad_live_prices(self):
        data = pd.DataFrame(data={'SecCode': [36799, 6027]})
        result = get_qad_live_prices(data, '1/4/2018')
        benchmark = pd.DataFrame(
                            data={
                             'SecCode': ['6027', '36799', '11113', '50311'],
                             'Ticker': ['AAPL', 'IBM', '$VIX.X', '$SPX.X'],
                             'AdjOpen': [203.49048, 263.55002, None, None],
                             'AdjHigh': [204.5873, 267.9638, None, None],
                             'AdjLow': [202.94798, 263.0878, None, None],
                             'AdjClose': [204.0684, 266.9342, 9.22, 2723.99],
                             'AdjVolume': [22434600, 7556249, None, None],
                             'AdjVwap': [204.0094, 266.5215, None, None],
                             'RClose': [173.03, 161.7, None, None]
                            })
        benchmark = benchmark[result.columns]
        assert_frame_equal(result, benchmark)

    def test_rollup_orders(self):
        order_df = pd.DataFrame(
                    data={
                     'SecCode': ['10', '20', '10', '20'],
                     'Ticker': ['A', 'B', 'A', 'B'],
                     'PercAlloc': [.1, -.2, .5, .1],
                     'RClose': [10, 20, 10, 20],
                     'Dollars': [100, -200, 500, 100],
                     'NewShares': [10, -10, 50, 5]
                    })

        result = rollup_orders(order_df)
        benchmark = pd.DataFrame(
                    data={
                     'SecCode': ['10', '20'],
                     'ticker': ['A', 'B'],
                     'perc_alloc': [.3, -.05],
                     'close': [10, 20],
                     'dollars': [600, -100],
                     'shares': [60, -5]
                    })
        benchmark = benchmark[result.columns]
        assert_frame_equal(result, benchmark)

        result = rollup_orders(order_df, 'XX')
        benchmark.columns = ['SecCode'] + ['XX_{}'.format(x) for x in
                                           benchmark.columns[1:]]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        if os.path.isdir(self.test_dir):
            shutil.rmtree(self.test_dir)


if __name__ == '__main__':
    unittest.main()





'''

def run_pricing_reconciliation(recon_dt, strategy_id=STRATEGY_ID):
    # Get Executed prices  ramex_data = dly_fls.get_ramex_processed_data(recon_dt)[0]

    statarb_trades = statarb_trades[statarb_trades.strategy_id == STRATEGY_ID]

    # Get live prices
    live_prices = get_signal_prices(recon_dt)

    # Merge executed and signal prices
    trade_data = merge_trades_signal_prices(statarb_trades, live_prices)

    # QAD data for trade date
    qad_data = get_qad_close_data(trade_data, recon_dt)

    # Combine trade data and qad data and append null SecCodes
    # This can occur when stocks leave the universe at the end of the month
    no_seccodes = trade_data[trade_data.SecCode.isnull()].copy()
    trade_data = trade_data[trade_data.SecCode.notnull()].copy()
    recon = trade_data.merge(qad_data, how='left')
    trade_data = trade_data.append(no_seccodes)

    # Write to file
    _write_pricing_output(recon, recon_dt, RECON_DIR)



def get_qad_close_data(data, inp_date):
    # Use SecCode to get QAD Pricing
    assert('SecCode' in data.columns)
    seccodes = data.SecCode.dropna().values
    features = ['RClose', 'RVolume', 'MarketCap', 'AdjClose']

    dh = DataHandlerSQL()
    qad_data = dh.get_seccode_data(seccodes, features, inp_date, inp_date)

    qad_data.rename(columns={
                    'RClose': 'qad_close',
                    'RVolume': 'qad_volume',
                    'AdjClose': 'qad_adj_close',
                    'MarketCap': 'qad_market_cap'}, inplace=True)

    qad_data.SecCode = qad_data.SecCode.astype(int).astype(str)
    return qad_data[['SecCode', 'Date', 'qad_close', 'qad_adj_close',
                     'qad_volume', 'qad_market_cap']]


def _write_pricing_output(data, recon_dt, output_dir=RECON_DIR):
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
    exec_orders = get_sent_orders(recon_dt)
    exec_orders['strategy_id'] = strategy_id

    # Merge and write
    _write_order_output(recon_orders, exec_orders, recon_dt)


def get_recon_orders(recon_dt):
    # 0. Checks meta and import position size
    position_size = gla.get_position_size()['gross_position_size']

    # 1. Import raw data
    raw_data = gla.import_raw_data_archive(recon_dt)

    # 2. Get model dir name
    dir_name = '{}_live'.format(recon_dt.strftime('%Y%m%d'))
    meta = json.load(open(os.path.join(
        BASE_DIR, 'archive', 'live_directories', dir_name,
        'meta.json'), 'r'))
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
    qad_live_data = get_qad_live_prices(scaling, recon_dt)
    qad_orders = strategy.run_live(qad_live_data)
    qad_orders = qad_orders.merge(qad_live_data[['SecCode',
                                                 'Ticker',
                                                 'RClose']], how='left')
    qad_orders['Dollars'] = qad_orders.PercAlloc * position_size
    qad_orders['NewShares'] = (qad_orders.Dollars / qad_orders.RClose)
    qad_orders.NewShares = qad_orders.NewShares.astype(int)

    return rollup_orders(qad_orders, 'qad')


def get_qad_live_prices(data, inp_date):
    assert('SecCode' in data.columns)

    # Get qad equity data
    seccodes = data.SecCode.values
    features = ['TICKER', 'AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose',
                'AdjVolume', 'AdjVwap', 'RClose']
    dh = DataHandlerSQL()
    qad_data = dh.get_seccode_data(seccodes, features, inp_date, inp_date)

    # Get qad index data
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


def get_sent_orders(recon_dt, arch_dir=ARCHIVE_DIR):

    alloc_dir = os.path.join(arch_dir, 'allocations')
    alloc_files = os.listdir(alloc_dir)
    file_name = dly_fls.get_filename_from_date(recon_dt, alloc_files)

    exec_orders = pd.read_csv(os.path.join(alloc_dir, file_name))
    exec_orders.SecCode = exec_orders.SecCode.astype(str)

    return rollup_orders(exec_orders, 'exec')


def rollup_orders(order_df, col_prfx=None):
    assert(set(['SecCode', 'Ticker', 'PercAlloc', 'RClose', 'Dollars',
               'NewShares']).issubset(set(order_df.columns)))

    grp = order_df.groupby('SecCode')
    rollup = pd.DataFrame(index=order_df.SecCode.unique())

    col_prfx = col_prfx + '_' if col_prfx is not None else ''
    rollup['{}ticker'.format(col_prfx)] = grp.Ticker.min()
    rollup['{}perc_alloc'.format(col_prfx)] = grp.PercAlloc.mean()
    rollup['{}dollars'.format(col_prfx)] = grp.Dollars.sum()
    rollup['{}close'.format(col_prfx)] = grp.RClose.mean()
    rollup['{}shares'.format(col_prfx)] = grp.NewShares.sum()

    rollup.reset_index(inplace=True)
    rollup.rename(columns={'index': 'SecCode'}, inplace=True)
    return rollup


def _write_order_output(recon_orders, exec_orders, recon_dt,
                        output_dir=RECON_DIR):
    datestamp = recon_dt.strftime('%Y%m%d')
    path = os.path.join(output_dir,
                        '{}_order_recon.csv'.format(datestamp))
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

'''
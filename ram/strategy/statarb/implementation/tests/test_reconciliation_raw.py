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


    def test_get_eze_signal_prices(self):
        self.assertRaises(IOError, get_eze_signal_prices, dt.date.today(),
                          self.test_dir)

        result = get_eze_signal_prices('1/1/2018', self.test_dir)
        benchmark = self.live_pricing
        benchmark.rename(columns={'ROpen': 'signal_open',
                                  'RHigh': 'signal_high',
                                  'RLow': 'signal_low',
                                  'RClose': 'signal_close',
                                  'RVolume': 'signal_volume',
                                  'RVwap': 'signal_vwap',
                                  'captured_time': 'signal_time'},
                        inplace=True)
        benchmark['signal_time'] = [dt.time(15, 44, 37, 10000)] * 4

        assert_frame_equal(result, self.live_pricing)

    def test_merge_trades_signal_prices(self):
        pricing_inp = get_signal_prices('1/1/2018', self.test_dir)
        pricing_inp = get_eze_signal_prices('1/1/2018', self.test_dir)
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

    def test_get_qad_signal_prices(self):
        data = pd.DataFrame(data={'SecCode': [36799, 6027]})
        result = get_qad_signal_prices(data, '1/4/2018')
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

    def Xtest_get_executed_orders(self):
        result = get_executed_orders(data, '1/4/2018')
        pass


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


def get_sent_orders(recon_dt, arch_dir=ARCHIVE_DIR):

    alloc_dir = os.path.join(arch_dir, 'allocations')
    alloc_files = os.listdir(alloc_dir)
    file_name = dly_fls.get_filename_from_date(recon_dt, alloc_files)

    exec_orders = pd.read_csv(os.path.join(alloc_dir, file_name))
    exec_orders.SecCode = exec_orders.SecCode.astype(str)

    return rollup_orders(exec_orders, 'exec')


'''
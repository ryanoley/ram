import os
import shutil
import unittest
import pandas as pd
import datetime as dt
from pandas.util.testing import assert_frame_equal

import ram.strategy.statarb.implementation.reconciliation_raw as recon_fcns


class TestPricingReconciliation(unittest.TestCase):

    def setUp(self):

        self.test_dir = os.path.join(os.getenv('GITHUB'), 'ram', 'test_data')

        if os.path.isdir(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.mkdir(self.test_dir)

        self.live_pricing = pd.DataFrame(data={
                            'SecCode': [36799, 11027692, 36799, 30655],
                            'Ticker': ['IBM', 'FB', 'IBM', 'GS'],
                            'AdjOpen': [100., 200., 50., 20.],
                            'AdjHigh': [105., 200., 60., 20.5],
                            'AdjLow': [95.5, 190., 40, 19.],
                            'AdjClose': [102.5, 195., 50., 20.],
                            'AdjVolume': [100, 200, 50, 20],
                            'AdjVwap': [102.5, 195.5, 48.5, 19.3],
                            'captured_time': [
                                '2018-05-07 15:44:37.01',
                                '2018-05-07 15:44:37.01',
                                '2018-05-07 15:44:37.01',
                                '2018-05-07 15:44:37.01']
                            })

        file_path = os.path.join(self.test_dir, '20180101_live_pricing.csv')
        self.live_pricing.to_csv(file_path, index=False)

    def test_get_live_prices(self):
        self.assertRaises(IOError, recon_fcns.get_live_prices, dt.date.today(),
                          self.test_dir)

        result = recon_fcns.get_live_prices('1/1/2018', self.test_dir)

        benchmark = self.live_pricing
        benchmark['captured_time'] = [dt.time(15, 44, 37, 10000)] * 4

        assert_frame_equal(result, self.live_pricing)

    def test_get_qad_live_prices(self):
        data = pd.DataFrame(data={'SecCode': [36799, 6027]})
        result = recon_fcns.get_qad_live_prices(data, '1/4/2018')
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

        result = recon_fcns.rollup_orders(order_df)
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

        result = recon_fcns.rollup_orders(order_df, 'XX')
        benchmark.columns = ['SecCode'] + ['XX_{}'.format(x) for x in
                                           benchmark.columns[1:]]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        if os.path.isdir(self.test_dir):
            shutil.rmtree(self.test_dir)


if __name__ == '__main__':
    unittest.main()

import os
import shutil
import unittest
import pandas as pd
from pandas.util.testing import assert_frame_equal

from ram.strategy.statarb.implementation.pricing_reconciliation import *


class TestPricingReconciliation(unittest.TestCase):

    def setUp(self):

        self.test_dir = os.path.join(os.getenv('GITHUB'), 'ram', 'test_data')

        if os.path.isdir(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.mkdir(self.test_dir)

        # EOD Master Positions and Fund Manager
        self.live_pricing = pd.DataFrame(data={
                            'SecCode': [36799, 11027692, 36799, 30655],
                            'Ticker': ['IBM', 'FB', 'IBM', 'GS'],
                            'AdjOpen': [100., 200., 50., 20.],
                            'AdjHigh': [105., 200., 60., 20.5],
                            'AdjLow': [95.5, 190., 40, 19.],
                            'AdjClose': [102.5, 195., 50., 20.],
                            'AdjVolume': [100, 200, 50, 20],
                            'AdjVwap': [102.5, 195.5, 48.5, 19.3]})

        file_path = os.path.join(self.test_dir, '20180101_live_pricing.csv')
        self.live_pricing.to_csv(file_path, index=False)

    def test_get_live_prices(self):
        self.assertRaises(IOError, get_live_prices, dt.date.today(),
                          self.test_dir)

        result = get_live_prices('1/1/2018', self.test_dir)
        assert_frame_equal(result, self.live_pricing)

    def tearDown(self):
        if os.path.isdir(self.test_dir):
            shutil.rmtree(self.test_dir)


if __name__ == '__main__':
    unittest.main()


import unittest
import os
import shutil
import datetime as dt
import pandas as pd
import numpy as np

from numpy.testing import assert_array_equal

from ram.data.intraday_data_manager import IntradayDataManager


class TestDataHandlerSQL(unittest.TestCase):

    def setUp(self):
        self.base_path = os.path.join(os.getenv('GITHUB'), 'ram', 'test_data')
        if os.path.isdir(self.base_path):
            shutil.rmtree(self.base_path)
        os.mkdir(self.base_path)
        # Create some data
        data = pd.DataFrame(columns=[
            'Ticker', 'DateTime', 'High', 'Low', 'Open', 'Close', 'Volume',
            'OpenInterest'])
        data.loc[0] = ['SPY', '2017-05-16 10:06:00', 10.5, 10.0, 10.2, 10.4,
                       2000, 1000]
        data.loc[1] = ['SPY', '2017-05-16 10:07:00', 10.5, 10.0, 10.2, 10.4,
                       2000, 1000]
        data.loc[2] = ['SPY', '2017-05-19 15:59:00', 10.5, 10.0, 10.2, 10.4,
                       2000, 1000]
        data.to_csv(os.path.join(self.base_path, 'SPY.csv'), index=False)
        data.Ticker = 'IWM'
        data.to_csv(os.path.join(self.base_path, 'IWM.csv'), index=False)
        ####
        self.idm = IntradayDataManager(self.base_path)

    def test_get_available_tickers(self):
        result = self.idm.get_available_tickers()
        benchmark = ['IWM', 'SPY']
        self.assertEqual(benchmark, result)

    def test_get_ticker_min_max_date(self):
        result = self.idm._get_ticker_min_max_date('SPY')
        benchmark = dt.datetime(2017, 5, 16, 10, 6, 00)
        self.assertEqual(benchmark, result[0])
        benchmark = dt.datetime(2017, 5, 19, 15, 59, 00)
        self.assertEqual(benchmark, result[1])

    def tearDown(self):
        shutil.rmtree(self.base_path)


if __name__ == '__main__':
    unittest.main()

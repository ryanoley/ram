import os
import shutil
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.statarb.implementation.execution.check_data import *
from ram.strategy.statarb.implementation.execution.check_data import \
    _import_bloomberg_dividends, _import_bloomberg_splits, \
    _import_bloomberg_spinoffs


class TestImplementationCheckData(unittest.TestCase):

    def setUp(self):
        self.imp_dir = os.path.join(os.getenv('GITHUB'), 'ram',
                                    'ram', 'test_data')
        # DELETE LEFTOVER DIRECTORIES
        if os.path.exists(self.imp_dir):
            shutil.rmtree(self.imp_dir)
        os.mkdir(self.imp_dir)
        path = os.path.join(self.imp_dir, 'StatArbStrategy')
        os.mkdir(path)
        path = os.path.join(self.imp_dir, 'StatArbStrategy', 'live_pricing')
        os.mkdir(path)
        path = os.path.join(self.imp_dir, 'StatArbStrategy', 'daily_raw_data')
        os.mkdir(path)
        # Raw data for today
        today = dt.date.today()
        data = pd.DataFrame()
        data['SecCode'] = [321, 456]
        data['Date'] = [today.strftime('%Y-%m-%d')] * 2
        file_name = today.strftime('%Y%m%d') + '_version_0024.csv'
        data.to_csv(os.path.join(path, file_name), index=None)
        # Dividends
        path = os.path.join(self.imp_dir, 'bloomberg_data')
        os.mkdir(path)
        today = dt.date.today()
        data = pd.DataFrame()
        data['CUSIP'] = [1, 2]
        data['DPS Last Gross'] = [0, 10]
        data['Dvd Ex Dt'] = [today.strftime('%Y-%m-%d')] * 2
        data['Market Cap'] = [100, 200]
        data['Market Cap#1'] = [100, 200]
        data['Price:D-1'] = [10, 20]
        data['Short Name'] = ['A', 'B']
        data['Ticker'] = ['AAPL US', 'TSLA US']
        file_name = today.strftime('%Y%m%d') + '_dividends.csv'
        data.to_csv(os.path.join(path, file_name), index=None)
        # Split Data
        data = pd.DataFrame()
        data['CUSIP'] = [1, 2]
        data['Current Stock Split Adjustment Factor'] = [1.5, .25]
        data['Market Cap'] = [100, 200]
        data['Next Stock Split Ratio'] = [1.5, .25]
        data['Short Name'] = ['A', 'B']
        data['Stk Splt Ex Dt'] = [today.strftime('%Y-%m-%d')] * 2
        data['Ticker'] = ['AAPL US', 'TSLA US']
        file_name = today.strftime('%Y%m%d') + '_splits.csv'
        data.to_csv(os.path.join(path, file_name), index=None)
        # Spinoff Data
        data = pd.DataFrame()
        data['CUSIP'] = [1, 2]
        data['CUSIP#1'] = [1, 2]
        data['Market Cap'] = [100, 200]
        data['Short Name'] = ['A', 'B']
        data['Spin Adj Fact Curr'] = [2.0, .5]
        data['Spin Adj Fact Nxt'] = [2.0, .5]
        data['Spinoff Ex Date'] = [today.strftime('%Y-%m-%d')] * 2
        data['Ticker'] = ['AAPL US', 'TSLA US']
        file_name = today.strftime('%Y%m%d') + '_spinoffs.csv'
        data.to_csv(os.path.join(path, file_name), index=None)

    def test_import_bloomberg_dividends(self):
        result = _import_bloomberg_dividends(self.imp_dir)
        benchmark = pd.DataFrame()
        benchmark['Ticker'] = ['TSLA']
        benchmark['Cusip'] = ['2']
        benchmark['DivMultiplier'] = [1.5]
        assert_frame_equal(result, benchmark)

    def test_import_bloomberg_splits(self):
        result = _import_bloomberg_splits(self.imp_dir)
        benchmark = pd.DataFrame()
        benchmark['Ticker'] = ['AAPL', 'TSLA']
        benchmark['Cusip'] = ['1', '2']
        benchmark['SplitMultiplier'] = [1.5, .25]
        assert_frame_equal(result, benchmark)

    def test_import_bloomberg_spinoffs(self):
        result = _import_bloomberg_spinoffs(self.imp_dir)
        benchmark = pd.DataFrame()
        benchmark['Ticker'] = ['AAPL', 'TSLA']
        benchmark['Cusip'] = ['1', '2']
        benchmark['SpinoffMultiplier'] = [0.5, 2.0]
        assert_frame_equal(result, benchmark)

    def test_get_universe_seccodes(self):
        result = get_universe_seccodes(self.imp_dir)
        benchmark = np.array([321, 456])
        assert_array_equal(result, benchmark)

    def test_process_bloomberg_data(self):
        mapping = pd.DataFrame()
        mapping['Cusip'] = ['1', '2']
        mapping['Ticker'] = ['AAPL', 'TSLA']
        mapping['SecCode'] = ['321', '456']
        mapping['Issuer'] = ['APPLE', 'TESLA']
        result = process_bloomberg_data(self.imp_dir, mapping=mapping)
        self.assertEqual(result, "Spotcheck dividend multiplier ['TSLA']; ")
        result = pd.read_csv(
            os.path.join(self.imp_dir, 'StatArbStrategy',
                         'live_pricing', 'bloomberg_scaling.csv'))
        benchmark = pd.DataFrame()
        benchmark['SecCode'] = [321, 456]
        benchmark['DivMultiplier'] = [1.0, 1.5]
        benchmark['SpinoffMultiplier'] = [0.5, 2.0]
        benchmark['SplitMultiplier'] = [1.5, 0.25]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        if os.path.exists(self.imp_dir):
            shutil.rmtree(self.imp_dir)


if __name__ == '__main__':
    unittest.main()

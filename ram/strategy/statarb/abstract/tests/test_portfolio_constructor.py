import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array

from ram.strategy.statarb.abstract.portfolio_constructor import \
    BasePortfolioConstructor


class PortfolioConstructor(BasePortfolioConstructor):

    def get_args(self):
        return None

    def set_args(self):
        return None

    def set_signals_constructor_data(self):
        return None

    def get_day_position_sizes(self):
        return None


class TestBasePortfolioConstructor(unittest.TestCase):

    def setUp(self):
        pass

    def test_filter_seccodes(self):
        pass

    def test_set_pricing_data(self):
        cons = PortfolioConstructor()
        data = pd.DataFrame()
        data['SecCode'] = ['A', 'A', 'B', 'B']
        data['Date'] = [dt.date(2010, 1, 1), dt.date(2010, 1, 2)] * 2
        vals = [10, 20, 30, 40]
        data['RClose'] = vals
        data['RCashDividend'] = vals
        data['SplitMultiplier'] = vals
        data['AvgDolVol'] = vals
        data['MarketCap'] = vals
        cons.set_pricing_data(1, data)
        data = pd.DataFrame()
        data['SecCode'] = ['A', 'A', 'B', 'B']
        data['Date'] = [dt.date(2010, 1, 2), dt.date(2010, 1, 3)] * 2
        vals = [66, 77, 88, 99]
        data['RClose'] = vals
        data['RCashDividend'] = vals
        data['SplitMultiplier'] = vals
        data['AvgDolVol'] = vals
        data['MarketCap'] = vals
        cons.set_pricing_data(2, data)
        result = cons._pricing['closes'].keys()
        result.sort()
        benchmark = [dt.date(2010, 1, 1),
                     dt.date(2010, 1, 2),
                     dt.date(2010, 1, 3)]
        self.assertListEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

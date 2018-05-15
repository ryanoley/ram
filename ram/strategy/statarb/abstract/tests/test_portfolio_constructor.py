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

    def set_signal_data(self):
        return None

    def set_other_data(self):
        return None

    def get_day_position_sizes(self, date, signals):
        return None


class TestBasePortfolioConstructor(unittest.TestCase):

    def setUp(self):
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
        closes = cons._pricing['closes']
        self.assertEqual(closes[dt.date(2010, 1, 2)]['A'], 20)
        self.assertEqual(closes[dt.date(2010, 1, 2)]['B'], 40)
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
        closes = cons._pricing['closes']
        result = closes.keys()
        result.sort()
        benchmark = [dt.date(2010, 1, 1),
                     dt.date(2010, 1, 2),
                     dt.date(2010, 1, 3)]
        self.assertListEqual(result, benchmark)
        self.assertEqual(closes[dt.date(2010, 1, 2)]['A'], 66)
        self.assertEqual(closes[dt.date(2010, 1, 2)]['B'], 88)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

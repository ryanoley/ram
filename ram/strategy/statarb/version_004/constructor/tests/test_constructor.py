import unittest
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.utils.time_funcs import convert_date_array
from ram.strategy.statarb.version_004.constructor.constructor import *


class TestPortfolioConstructor(unittest.TestCase):

    def setUp(self):
        other_data = pd.DataFrame()
        other_data['SecCode'] = ['A', 'B', 'C', 'D']
        other_data['Date'] = [dt.date(2010, 1, 1)] * 4
        other_data['keep_inds'] = True
        other_data['V1'] = [1, 2, 3, 4]
        other_data['V2'] = [8, 7, 6, 5]
        other_data2 = pd.DataFrame()
        other_data2['SecCode'] = ['A', 'B', 'C', 'D']
        other_data2['Date'] = [dt.date(2010, 1, 2)] * 4
        other_data2['keep_inds'] = True
        other_data2['V1'] = [1, 2, 3, 4]
        other_data2['V2'] = [8, 7, 6, 5]
        self.other_data = other_data.append(other_data2).reset_index(drop=True)
        signal_data = pd.DataFrame()
        signal_data['SecCode'] = ['A', 'B', 'C', 'D']
        signal_data['Date'] = [dt.date(2010, 1, 1)] * 4
        signal_data['Signal'] = [10, 20, 15, 5]
        signal_data2 = pd.DataFrame()
        signal_data2['SecCode'] = ['A', 'B', 'C', 'D']
        signal_data2['Date'] = [dt.date(2010, 1, 2)] * 4
        signal_data2['Signal'] = [10, 20, 15, 5]
        self.signal_data = signal_data.append(
            signal_data2).reset_index(drop=True)

    def test_set_args_get_day_position_sizes1(self):
        cons = PortfolioConstructor()
        # These two methods happen before setting args
        cons.set_other_data(self.other_data)
        cons.set_signal_data(self.signal_data)
        # Set args
        cons.set_args(score_var='V2',
                      per_side_count=1,
                      holding_period=2)
        # Run method for test
        result = cons.get_day_position_sizes(dt.date(2010, 1, 1), 0)
        benchmark = {'A': -0.25, 'C': 0.25}
        self.assertDictEqual(result, benchmark)
        result = cons.get_day_position_sizes(dt.date(2010, 1, 2), 0)
        benchmark = {'A': -0.5, 'C': 0.5}
        self.assertDictEqual(result, benchmark)
        # Test size container
        result = cons._size_containers[0].sizes.keys()
        result.sort()
        benchmark = [dt.date(2010, 1, 1), dt.date(2010, 1, 2)]
        self.assertListEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

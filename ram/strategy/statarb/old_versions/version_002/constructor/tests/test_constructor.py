import unittest
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.utils.time_funcs import convert_date_array
from ram.strategy.statarb.version_002.constructor.constructor import *


class TestPortfolioConstructor(unittest.TestCase):

    def setUp(self):

        other_data = pd.DataFrame()
        other_data['SecCode'] = ['A', 'B', 'C', 'D']
        other_data['Date'] = [dt.date(2010, 1, 1)] * 4
        other_data['keep_inds'] = True
        other_data['V1'] = [1, 2, 3, 4]
        other_data['V2'] = [8, 7, 6, 5]
        self.other_data = other_data
        # Signals
        signal_data = pd.DataFrame()
        signal_data['SecCode'] = ['A', 'B', 'C', 'D']
        signal_data['Date'] = [dt.date(2010, 1, 1)] * 4
        signal_data['Signal'] = [10, 20, 15, 5]
        self.signal_data = signal_data

    def test_set_args_get_day_position_sizes(self):
        cons = PortfolioConstructor()
        cons._holding_period = 2
        cons.set_other_data(self.other_data)
        cons.set_signal_data(self.signal_data)
        cons.set_args(score_var='V2',
                      per_side_count=1,
                      holding_period=2)
        result = cons.get_day_position_sizes(dt.date(2010, 1, 1), 0)
        benchmark = {'A': -2500000.0, 'C': 2500000.0}
        self.assertDictEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

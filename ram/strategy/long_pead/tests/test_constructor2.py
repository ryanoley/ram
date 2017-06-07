import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.utils.time_funcs import convert_date_array
from ram.strategy.long_pead.constructor.constructor2 import PortfolioConstructor2


class TestConstructor2(unittest.TestCase):

    def setUp(self):
        dates = ['2015-01-01', '2015-01-02', '2015-01-03', '2015-01-04']
        self.data = pd.DataFrame({
            'SecCode': ['AAPL'] * 4 + ['GOOGL'] * 4 + ['IBM'] * 4,
            'Date': dates * 3,
            'AdjClose': [10, 9, 5, 5] + [10, 20, 18, 15] + [9, 10, 11, 12],
            'RClose': [10, 9, 5, 5] + [10, 20, 18, 15] + [9, 10, 11, 12],
            'RCashDividend': [0] * 12,
            'SplitFactor': [1] * 12,
            'EARNINGSFLAG': [0, 0, 0, 1] + [1, 0, 0, 0] + [0, 1, 0, 0],
            'TestFlag': [True] * 12
        })
        self.data['Date'] = convert_date_array(self.data.Date)

    def Xtest_get_position_sizes(self):
        cons = PortfolioConstructor()
        mrets = {'AAPL': 4, 'IBM': 10, 'TSLA': -10, 'BAC': 4, 'GS': np.nan}
        result = cons._get_position_sizes(mrets, 1, 100)

    def Xtest_set_and_prep_data(self):
        cons = PortfolioConstructor(booksize=200,)
        cons.set_and_prep_data(self.data, time_index=0,
                          blackout_offset1=-1,
                          blackout_offset2=1,
                          anchor_init_offset=1,
                          anchor_window=2)
        result = cons.close_dict[pd.Timestamp('2015-01-01')]
        benchmark = {'AAPL': 10, 'GOOGL': 10, 'IBM': 9}
        self.assertDictEqual(result, benchmark)
        result = cons.close_dict[pd.Timestamp('2015-01-04')]
        benchmark = {'AAPL': 5, 'GOOGL': 15, 'IBM': 12}
        self.assertDictEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

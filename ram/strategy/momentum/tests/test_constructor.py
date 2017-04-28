import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array

from ram.strategy.momentum.constructor import MomentumConstructor


class TestMomentumConstructor(unittest.TestCase):

    def setUp(self):
        dates = ['2015-01-01', '2015-01-02', '2015-01-03', '2015-04-04']
        data = pd.DataFrame()
        data['SecCode'] = ['1234'] * 4
        data['Date'] = convert_date_array(dates)
        data['AdjClose'] = [1, 2, 3, 4]
        data['MA5_AdjClose'] = [1, 2, 3, 4]
        data['MA80_AdjClose'] = [1, 2, 3, 4]
        data['GSECTOR'] = [10] * 4
        data['EARNINGSFLAG'] = [0] * 4
        data['TestFlag'] = [True] * 4
        data2 = data.copy()
        data2['SecCode'] = ['5678'] * 4
        data2['MA5_AdjClose'] += -1
        self.data = data.append(data2).reset_index(drop=True)
        self.dates = dates

    def test_format_data(self):
        con = MomentumConstructor()
        result = con._format_data(self.data)
        benchmark = pd.DataFrame(index=self.dates)
        benchmark['1234'] = [1.0]*4
        benchmark['5678'] = [0.00, 0.50, 0.66666666666666663, 0.75]
        benchmark.columns.name = 'SecCode'
        benchmark.index = convert_date_array(benchmark.index)
        benchmark.index.name = 'Date'
        assert_frame_equal(result[0], benchmark)
        benchmark = {
            dt.date(2015, 1, 3): {'1234': 3, '5678': 3},
            dt.date(2015, 4, 4): {'1234': 4, '5678': 4},
            dt.date(2015, 1, 1): {'1234': 1, '5678': 1},
            dt.date(2015, 1, 2): {'1234': 2, '5678': 2}
        }
        self.assertDictEqual(result[1], benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

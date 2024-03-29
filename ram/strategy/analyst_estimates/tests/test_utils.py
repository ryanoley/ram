import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.analyst_estimates.base.utils import *


class TestUtils(unittest.TestCase):

    def test_make_variable_dict(self):
        dates = [dt.date(2015,1,1), dt.date(2015,1,2), dt.date(2015,1,3),
                     dt.date(2015,1,4)]
        data = pd.DataFrame({
            'SecCode': ['AAPL'] * 4 + ['GOOGL'] * 4 + ['IBM'] * 4,
            'Date': dates * 3,
            'AdjClose': [10, 9, 5, 5] + [10, 20, 18, 15] + [9, 10, 11, 12],
            'RClose': [10, 9, 5, 5] + [10, 20, 18, 15] + [9, 10, 11, 12],
            'RCashDividend': [0] * 12,
            'SplitFactor': [1] * 12,
            'EARNINGSFLAG': [0, 0, 0, 1] + [1, 0, 0, 0] + [0, 1, 0, 0],
            'TestFlag': [True] * 12
        })
        result = make_variable_dict(data, 'AdjClose')

        benchmark = {'AAPL': 5, 'GOOGL': 18, 'IBM': 11}
        self.assertDictEqual(result[dt.date(2015, 1, 3)], benchmark)

        benchmark = {'AAPL': 10, 'GOOGL': 10, 'IBM': 9}
        self.assertDictEqual(result[dt.date(2015, 1, 1)], benchmark)

        data = pd.DataFrame({
            'SecCode': ['a'] * 3 + ['b'] * 3,
            'Date': ['2010-01-01', '2010-01-02', '2010-01-03'] * 2,
            'V1': range(6),
            'V2': range(1, 7)
        })
        data.iloc[2, 2] = np.nan
        data.iloc[5, 3] = np.nan
        result = make_variable_dict(data, 'V1', fillna=np.nan)
        self.assertTrue(np.isnan(result['2010-01-03']['a']))
        self.assertEqual(result['2010-01-03']['b'], 5.0)

        result = make_variable_dict(data, 'V2', fillna=-99.0)
        self.assertFalse(np.isnan(result['2010-01-03']['b']))
        self.assertEqual(result['2010-01-03']['a'], 3.0)
        self.assertEqual(result['2010-01-03']['b'], -99.0)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

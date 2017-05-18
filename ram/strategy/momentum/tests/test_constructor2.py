import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array

from ram.strategy.momentum.constructor2 import MomentumConstructor2


class TestMomentumConstructor2(unittest.TestCase):

    def setUp(self):
        np.random.seed(123)
        dates = ['2015-01-{0:02d}'.format(x) for x in range(12, 32)] + \
            ['2015-02-{0:02d}'.format(x) for x in range(1, 7)]
        data = pd.DataFrame()
        data['SecCode'] = ['1234'] * 26
        data['Date'] = convert_date_array(dates)
        data['AdjClose'] = np.cumsum(np.random.randn(26) * 1)
        data['EARNINGSFLAG'] = [0] * 26
        data['TestFlag'] = [False] * 12 + [True] * 14
        data2 = data.copy()
        data2['SecCode'] = ['5678'] * 26
        data['AdjClose'] = np.cumsum(np.random.randn(26) * 2)
        self.data = data.append(data2).reset_index(drop=True)

    def test_get_signals(self):
        con = MomentumConstructor2()
        import pdb; pdb.set_trace()
        result = con._get_signals(self.data, 4, 1)

    def test_get_date_iterable(self):
        con = MomentumConstructor2()
        result = con._get_date_iterable(self.data, 'M',
                                        holding_period=2, n_ports=2)
        self.assertEqual(result[0][0], dt.date(2015, 1, 24))
        self.assertEqual(result[0][-1], dt.date(2015, 2, 2))
        assert_array_equal(result[1], np.array([1, 2, 1, 2, 1,
                                                2, 1, 2, 0, 0.]))
        assert_array_equal(result[2], np.array([0, 0, 1, 2, 1,
                                                2, 1, 2, 1, 2.]))

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

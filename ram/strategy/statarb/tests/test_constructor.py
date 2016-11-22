import unittest
import numpy as np
import pandas as pd

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.constructor.constructor import PortfolioConstructor


class TestConstructor(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_daily_pl(self):
        close = pd.DataFrame({'V1': [10, 11, 10, 9],
                              'V2': [10, 14, 20, 22],
                              'V3': [10, 13, 10, 4]})
        dividends = close.copy()
        splits = close.copy()
        dividends[:] = 0
        splits[:] = 1
        zscores = pd.DataFrame({'V1_V2': [2, 3, 4, 5],
                                'V1_V3': [-2, -3, 0, 0],
                                'V2_V3': [0, 0, -2, -4]})
        port = PortfolioConstructor()
        port.n_pairs = 2
        result = port.get_daily_pl(scores=zscores, data=data)

        benchmark = pd.DataFrame([])
        benchmark['PL'] = [-0.12, 10, 89.919, 63.919]
        benchmark['Exposure'] = [400, 490, 390, 0.]
        assert_frame_equal(result, benchmark)
        # Lower max_position_exposure
        result = port.get_daily_pl(
            scores=zscores, booksize=400, Close=close, Dividend=dividends,
            SplitMultiplier=splits, n_pairs=2, max_pos_exposure=.5)
        benchmark = pd.DataFrame([])
        benchmark['PL'] = [-0.12, 10, 89.91, 4.814]
        benchmark['Exposure'] = [400, 490, 400, 0.]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

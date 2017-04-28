import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal, assert_almost_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.responses.response1 import response_strategy_1


class TestResponses1(unittest.TestCase):

    def setUp(self):
        pass

    def test_responses_1(self):
        close1 = pd.DataFrame({
            0: [10, 11, 13, 16],
            1: [10, 11, 13, 16],
            2: [10, 10, 10, 10]})
        close2 = pd.DataFrame({
            0: [10, 10, 10, 10],
            1: [10, 4, 9, 10],
            2: [10, 10, 11, 11]})
        result = response_strategy_1(close1, close2, .3, 3)
        assert_almost_equal(result[0][0], [-0.6, -0.6, 0.1])
        assert_almost_equal(result[1][0], [0.3, 0.7, -0.1])

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

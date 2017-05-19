import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal, assert_almost_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.statarb.responses.response2 import response_strategy_2
from ram.strategy.statarb.responses.response2 import _response_strategy_2


class TestResponses2(unittest.TestCase):

    def setUp(self):
        pass

    def test_responses_2(self):
        close1 = pd.DataFrame({
            0: [10, 11, 13, 16],
            1: [10, 11, 13, 16],
            2: [10, 10, 10, 10]}).values
        close2 = pd.DataFrame({
            0: [10, 10, 10, 10],
            1: [10, 4, 9, 10],
            2: [10, 10, 11, 11]}).values
        zscores = np.array([
            [0, 3, 2, 0],
            [0, 0, -3, 1],
            [3, 0, -4, 0]
        ])
        import pdb; pdb.set_trace()
        responses1 = np.zeros(close1.shape)
        responses2 = np.zeros(close1.shape)
        result = _response_strategy_2(
            close1, close2, responses1, responses2,
            zscores, z_open=2, z_close=1)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

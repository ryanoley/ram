import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.analysis.selection import *


class TestSelection(unittest.TestCase):

    def setUp(self):
        pass

    def Xtest_lower_partial_moment(self):
        #result = stats._lower_partial_moment(self.returns)
        benchmark = pd.DataFrame(
            [8.6167, 0.2333], index=['V1', 'V2'], columns=['LPM_2'])
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

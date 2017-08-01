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

    def test_basic_model_selection(self):
        data = pd.DataFrame(
            index=[dt.date(2010, 1, i) for i in range(1, 11)]
        )
        data['0'] = [1, 2, 3, 4, 5.] * 2
        data['1'] = [6, 7, 8, 9, 10.] * 2
        result = basic_model_selection(data, window=4)
        benchmark = pd.DataFrame(index=data.index)
        benchmark['Rets'] = [np.nan] * 4 + [10, 6, 7, 8, 9, 10.]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

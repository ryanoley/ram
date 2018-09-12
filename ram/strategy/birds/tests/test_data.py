import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.birds.data import *


class TestData(unittest.TestCase):

    def setUp(self):
        test_dates = [dt.date(2010, 1, i) for i in [1, 2, 3, 4, 5]]
        data = pd.DataFrame(index=test_dates)
        data['A'] = [1, 2, 3, 4, 5]
        data['B'] = [6, 7, 8, 9, 10]
        data['C'] = [11, 12, 13, 14, 16]
        data['D'] = [16, 17, 18, 19, 20]
        data['E'] = [21, 22, 23, 24, 25]
        self.data = data

    def test_make_groups(self):
        pass

    def test_get_index_features(self):
        pass

    def test_get_index_returns(self):
        pass

    def test_get_index_responses(self):
        pass

    def test_make_indexes(self):
        import pdb; pdb.set_trace()
        result = make_indexes(data=self.data,
                              close_prices=self.data,
                              test_dates=self.data.index,
                              label='V1')

    def test_extract_test_dates(self):
        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

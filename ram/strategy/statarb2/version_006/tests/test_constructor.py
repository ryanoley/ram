import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb2.version_006.constructor import *


class TestPortfolioConstructor(unittest.TestCase):

    def setUp(self):
        pass

    def Xtest_get_day_position_sizes(self):
        cons = PortfolioConstructor()
        cons._split_perc = 50
        scores = pd.Series(index=['a', 'b', 'c', 'd'])
        signals = scores.copy()
        scores[:] = [2, -1, 4, -3]
        signals[:] = [1, 2, 3, 4]
        result = cons.get_day_position_sizes(scores, signals)
        benchmark = {'a': -1000000.0, 'c': 0.0, 'b': 0.0, 'd': 1000000.0}
        self.assertDictEqual(result, benchmark)

    def test_SizeContainer(self):
        sizes = SizeContainer(2)
        sizes.update_sizes(0, {'A': -99, 'B': 99, 'C': 0})
        sizes.update_sizes(1, {'A': 10, 'B': -10, 'C': 0})
        sizes.update_sizes(2, {'A': 10, 'B': 0, 'C': -10})
        result = sizes.get_sizes()
        benchmark = {'A': 10.0, 'B': -5.0, 'C': -5.0}
        self.assertDictEqual(result, benchmark)
        sizes.update_sizes(3)
        result = sizes.get_sizes()
        benchmark = {'A': 5.0, 'B': 0.0, 'C': -5.0}
        self.assertDictEqual(result, benchmark)
        sizes.update_sizes(4)
        result = sizes.get_sizes()
        benchmark = {'A': 0.0, 'B': 0.0, 'C': 0.0}
        self.assertDictEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb2.version_004.constructor import *


class TestPortfolioConstructor(unittest.TestCase):

    def setUp(self):
        pass

    def test_filter_seccodes(self):
        data = {1: 10, 2: 5, 3: 30, 4: np.nan}
        result = filter_seccodes(data, 12)
        benchmark = [1, 2, 4]
        self.assertListEqual(result, benchmark)

    def Xtest_get_day_position_sizes(self):
        cons = PortfolioConstructor()
        cons._prma_x = 10
        cons._split_perc = 30
        cons._daily_drop = True
        scores = {'A': -10, 'B': -5, 'C': 5, 'D': 10, 'E': np.nan}
        day_rets = {'A': -2, 'B': -1, 'C': 1, 'D': 2, 'E': 100}
        result = cons.get_day_position_sizes(scores, day_rets)
        benchmark = {'A': 1000000.0, 'C': 0.0, 'B': 0.0,
                     'D': -1000000.0, 'E': 0.0}
        self.assertDictEqual(result, benchmark)
        cons._split_perc = 50
        cons._daily_drop = False
        result = cons.get_day_position_sizes(scores, day_rets)
        benchmark = {'A': 500000.0, 'C': -500000.0, 'B': 500000.0,
                     'E': 0.0, 'D': -500000.0}
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

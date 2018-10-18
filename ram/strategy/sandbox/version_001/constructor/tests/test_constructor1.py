import unittest
import pandas as pd
import datetime as dt
from gearbox import convert_date_array

from ram.strategy.sandbox.base.portfolio import Portfolio
from ram.strategy.sandbox.version_001.data.data_container1 import DataContainer1
from ram.strategy.sandbox.version_001.constructor.constructor1 import *

from pandas.util.testing import assert_frame_equal, assert_series_equal


class TestPortfolioConstructor1(unittest.TestCase):

    def setUp(self):
        self.constructor = PortfolioConstructor1(100)

    def test_make_scores_dict(self):
        signal_df = pd.DataFrame(data={
                                'SecCode':np.repeat(['a','b','c'], 3),
                                'Date':[dt.date(2017,1,1), dt.date(2017,1,2),
                                        dt.date(2017,1,3)] * 3,
                                'signal': [1, 1, 0, 0, 0, 0, 1, 1, 1]
                                })
        result = make_scores_dict(signal_df)

        benchmark = {dt.date(2017,1,1):{'a':1, 'b':0, 'c':1},
                        dt.date(2017,1,2):{'a':1, 'b':0, 'c':1},
                        dt.date(2017,1,3):{'a':0, 'b':0, 'c':1}}
        self.assertEqual(result, benchmark)

    def test_get_scores(self):
        scores_dict = {dt.date(2017,1,1):{'a':1, 'b':1},
                        dt.date(2017,1,2):{'a':0, 'b':0}}

        result = get_scores(scores_dict, date=dt.date(2017, 1, 1))
        benchmark = pd.DataFrame(data = {'score':[1, 1],
                                            'weight':[1, 1]},
                                index=['a', 'b'])
        assert_frame_equal(result, benchmark)

        result = get_scores(scores_dict, date=dt.date(2025, 1, 1))
        benchmark = pd.DataFrame([], columns=['score', 'weight'])
        assert_frame_equal(result, benchmark)

    def test_get_position_sizes(self):
        portfolio = Portfolio()
        portfolio.update_prices({'a':10, 'b':12, 'c':20, 'd':15})

        scores = pd.DataFrame(data = {'score': [0, 0, 1, -1],
                                        'weight':[0, 0, 1, -1]},
                              index = ['a', 'b', 'c', 'd'])
        result = self.constructor.get_position_sizes(scores, portfolio,
                                                     max_pos=.1)
        benchmark = pd.Series(data=[.1, -.1],
                              index=['c', 'd'],
                              name='weight')
        assert_series_equal(result[0].sort_index(), benchmark.sort_index())
        self.assertEqual(result[1], 0.)

        scores = pd.DataFrame(data = {'score': [1, -1, 1, -1],
                                        'weight':[1, -1, 1, -1]},
                              index = ['a', 'b', 'c', 'd'])
        result = self.constructor.get_position_sizes(scores, portfolio,
                                                     max_pos=.5)
        benchmark = pd.Series(data=[.25, -.25, .25, -.25],
                              index=['a','b', 'c', 'd'],
                              name='weight')
        assert_series_equal(result[0].sort_index(), benchmark.sort_index())
        self.assertEqual(result[1], 0.)

        scores = pd.DataFrame(data = {'score': [1, -1, 0, 0],
                                        'weight':[1, -1, 0, 0]},
                              index = ['a','b','c', 'd'])
        result = self.constructor.get_position_sizes(scores, portfolio,
                                                     max_pos=.5)
        benchmark = pd.Series(data=[.5, -.5, 0, 0],
                              index=['a','b','c', 'd'],
                              name='weight')
        assert_series_equal(result[0].sort_index(), benchmark.sort_index())
        self.assertEqual(result[1], 0.)


    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()






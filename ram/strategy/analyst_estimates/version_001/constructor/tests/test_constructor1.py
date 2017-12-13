import unittest
import pandas as pd
import datetime as dt
from gearbox import convert_date_array

from ram.strategy.analyst_estimates.base.portfolio import Portfolio
from ram.strategy.analyst_estimates.version_001.constructor.constructor1 import *

from pandas.util.testing import assert_frame_equal, assert_series_equal


class TestPortfolioConstructor1(unittest.TestCase):

    def setUp(self):
        self.constructor = PortfolioConstructor1(100)
        pass

    def test_get_position_sizes(self):
        portfolio = Portfolio()
        portfolio.update_prices({'1000':10, '1001':12, '1002':20, '1003':15})
        scores = pd.DataFrame(data = {'score': [.02, -.01],
                                        'weight':[1, -1]},
                              index = ['1002', '1003'])

        result = self.constructor.get_position_sizes(scores, portfolio,
                                                     close_seccodes=set([]),
                                                     pos_size = .05)
        benchmark = pd.Series(data=[.05, -.05],
                              index=['1002', '1003'],
                              name='weight')
        assert_series_equal(result[0].sort_index(), benchmark.sort_index())
        self.assertEqual(result[1], 0.)

        result = self.constructor.get_position_sizes(
                                        scores,
                                        portfolio,
                                        close_seccodes=set(['1002', '1003']),
                                        pos_size = .05)
        benchmark = pd.Series(data=[0., 0.],
                      index=['1002', '1003'],
                      name='weight')
        assert_series_equal(result[0].sort_index(), benchmark.sort_index())
        self.assertEqual(result[1], 0.)

        scores.weight = [1, -2]
        result = self.constructor.get_position_sizes(
                                        scores,
                                        portfolio,
                                        close_seccodes=set([]),
                                        pos_size = .5)
        benchmark = pd.Series(data=[.25, -.5],
                      index=['1002', '1003'],
                      name='weight')
        assert_series_equal(result[0].sort_index(), benchmark.sort_index())
        self.assertEqual(result[1], -.25)

    def test_get_scores(self):
        scores_dict = {
                dt.date(2017, 1, 1):
                    {'1000':.005, '1001':.02, '1002':-.005, '1003':-.015},
                dt.date(2017, 1, 2):
                    {'1000':0.0, '1001':.025, '1002':-.0025, '1003':-.015}
                }

        result = get_scores(scores_dict, date=dt.date(2017, 6, 1),
                            long_thresh=.01, short_thresh=-.01,
                            scale_weights=False)
        benchmark = pd.DataFrame(columns=['score', 'weight'])
        assert_frame_equal(result, benchmark)

        result = get_scores(scores_dict, date=dt.date(2017, 1, 1),
                            long_thresh=.01, short_thresh=.01,
                            scale_weights=False)
        benchmark = pd.DataFrame(data={'score':[.005, .02, -.005, -.015],
                                        'weight':[0., 1., 0., -1.]},
                                    index = ['1000', '1001', '1002', '1003'])
        assert_frame_equal(result, benchmark)

        result = get_scores(scores_dict, date=dt.date(2017, 1, 2),
                            long_thresh=.01, short_thresh=.01,
                            scale_weights=True)
        benchmark = pd.DataFrame(data={'score':[0., .025, -.0025, -.015],
                                        'weight':[0., 2., 0., -1.5]},
                                    index = ['1000', '1001', '1002', '1003'])
        assert_frame_equal(result, benchmark)


    def test_make_scores_dict(self):
        preds_df = pd.DataFrame(data={
                                'SecCode':['1000', '1001', '1002', '1003'],
                                'Date':[dt.date(2017,1,1), dt.date(2017,1,1),
                                        dt.date(2017,1,2), dt.date(2017,1,3)],
                                'preds': [-.05, .01, .05, -.0025]
                                })
        preds_df3 = preds_df.copy()
        preds_df3.Date = [dt.date(2017,1,2), dt.date(2017,1,2),
                            dt.date(2017,1,3), dt.date(2017,1,4)]
        preds_dict = {2:preds_df, 3:preds_df3}

        result = make_scores_dict(preds_dict, entry_dates=[2])
        benchmark = {dt.date(2017,1,1):{'1000':-.05, '1001':.01},
                        dt.date(2017,1,2):{'1002':.05},
                        dt.date(2017,1,3):{'1003':-.0025}
                    }
        self.assertEqual(result, benchmark)

        result = make_scores_dict(preds_dict, entry_dates=[2, 3])
        benchmark = {dt.date(2017,1,1):{'1000':-.05, '1001':.01},
                        dt.date(2017,1,2):{'1000':-.05, '1001':.01, '1002':.05},
                        dt.date(2017,1,3):{'1002':.05, '1003':-.0025},
                        dt.date(2017,1,4):{'1003':-.0025}
                    }
        self.assertEqual(result, benchmark)


    def test_get_closing_seccodes(self):
        portfolio = Portfolio()
        portfolio.update_prices({'1000':10,'1001':12,'1002':20,'1003':15})
        # Put position on
        portfolio.update_holding_days({'1000':1, '1001':-1})
        # Position held for one full day
        portfolio.update_holding_days({'1000':1, '1001':-1})
        result = self.constructor.get_closing_seccodes(portfolio, hold_per=2)
        benchmark = set(['1000', '1001'])
        self.assertEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()



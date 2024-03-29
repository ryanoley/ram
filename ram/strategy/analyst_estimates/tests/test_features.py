import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.strategy.analyst_estimates.base.features import *


class TestFeatures(unittest.TestCase):

    def setUp(self):
        data = pd.DataFrame()
        data['SecCode'] = ['1234'] * 8
        data['Date'] = pd.date_range(dt.date(2015,1,1), dt.date(2015,1,8))
        data['AdjClose'] = [1, 2, 3, 4, 5, 6, 7, 8]
        data['EARNINGSFLAG'] = [0, 0, 1, 0, 0, 0, 0, 0]
        data['EPSESTIMATEFQ1'] = [.5, .5, .75, .85, .85, .85, .85, .85]
        data['EPSESTIMATEFQ2'] = [.75, .75, 1., 1., 1., 1., 1., 1.]

        data2 = data.copy()
        data2['SecCode'] = ['5678'] * 8
        data2['AdjClose'] = data2.AdjClose * 10
        data = data.append(data2).reset_index(drop=True)
        data['AdjVwap'] = data['AdjClose'].copy()
        data['LEAD1_AdjVwap'] = data['AdjClose'].copy()
        data['LEAD11_AdjVwap'] = data['AdjClose'].copy() + \
                                    np.random.choice([1,-1], len(data))
        self.data = data

        dataB = pd.DataFrame()
        dataB['SecCode'] = ['1234'] * 8
        dataB['Date'] = pd.date_range(dt.date(2015,1,1), dt.date(2015,1,8))
        dataB['EARNINGSFLAG'] = [0, 1, 0, 0, 0, 1, 0, 0]
        dataB['EARNINGSRETURN'] = [.1, .1, .5, .5, .5, .5, .7, .7]

        data2 = dataB.copy()
        data2['SecCode'] = ['5678'] * 8
        dataB = dataB.append(data2).reset_index(drop=True)
        self.dataB = dataB

    def test_get_previous_ern_return(self):
        result = get_previous_ern_return(self.dataB)
        benchmark = np.array([np.nan, np.nan, .1, .1, .1, .1, .5, .5] * 2)
        assert_array_equal(np.round(result.PrevRet, 3), np.round(benchmark, 3))

        prev_data = self.dataB.copy()
        prev_data['Date'] = [x - dt.timedelta(356) for x in prev_data.Date]
        result = get_previous_ern_return(self.dataB, prior_data = prev_data)
        benchmark = np.array([.5, .5, .1, .1, .1, .1, .5, .5] * 2)
        assert_array_equal(np.round(result.PrevRet, 3), np.round(benchmark, 3))

    def test_get_cum_delta(self):
        result = get_cum_delta(self.data, 'EPSESTIMATEFQ1', 'eps', 3)
        result = np.array(result['eps'])
        benchmark = np.array([0., 0., 0.25, .35, .35, .0, .0, 0.] * 2)
        assert_array_equal(np.round(result, 3), np.round(benchmark, 3))

        result = get_cum_delta(self.data, 'EPSESTIMATE', 'eps', 3,
                                  smart_est_column=True)
        result = np.array(result['eps'])
        benchmark = np.array([0., 0., 0., .1, .1, 0., 0., 0.] * 2)
        assert_array_equal(np.round(result, 3), np.round(benchmark, 3))

        result = get_cum_delta(self.data, 'EPSESTIMATE', 'eps', 5,
                                  smart_est_column=True)
        result = np.array(result['eps'])
        benchmark = np.array([0., 0., 0., .1, .1, .1, .1, 0.] * 2)
        assert_array_equal(np.round(result, 3), np.round(benchmark, 3))

    def test_ern_date_blackout(self):
        result = ern_date_blackout(self.data, -1, 1)
        benchmark = np.array([0, 1, 1, 1, 0, 0, 0, 0] * 2)
        assert_array_equal(result.blackout.values, benchmark)
        result = ern_date_blackout(self.data, 0, 1)
        benchmark = np.array([0, 0, 1, 1, 0, 0, 0, 0] * 2)
        assert_array_equal(result.blackout.values, benchmark)
        result = ern_date_blackout(self.data, 0, 3)
        benchmark = np.array([0, 0, 1, 1, 1, 1, 0, 0] * 2)
        assert_array_equal(result.blackout.values, benchmark)

    def test_ern_price_anchor(self):
        data = self.data.copy()
        data = ern_date_blackout(data, -1, 1)
        result = ern_price_anchor(data, 1, 3)
        benchmark = np.array([np.nan] * 4 + [4, 4, 5, 6.])
        benchmark = np.append(benchmark, benchmark * 10)
        assert_array_equal(result.anchor_price.values, benchmark)
        #
        result = ern_price_anchor(data, 0, 3)
        benchmark = np.array([np.nan] * 4 + [3, 4, 5, 6.])
        benchmark = np.append(benchmark, benchmark * 10)
        assert_array_equal(result.anchor_price.values, benchmark)
        #
        data2 = self.data.copy()
        data2 = ern_date_blackout(data2, -1, 1)
        data2['SecCode'] = ['1234'] * 16
        data2['Date'] = ['2015-01-01', '2015-01-02', '2015-01-03',
                         '2015-01-04', '2015-01-05', '2015-01-06',
                         '2015-01-07', '2015-01-08', '2015-01-09',
                         '2015-01-10', '2015-01-11', '2015-01-12',
                         '2015-01-13', '2015-01-14', '2015-01-15',
                         '2015-01-16']
        result = ern_price_anchor(data2, 1, 3)
        benchmark = np.array([np.nan] * 4 + [4, 4, 5, 6, 7.] +
                             [np.nan] * 3 + [40, 40, 50, 60.])
        assert_array_equal(result.anchor_price.values, benchmark)

    def test_get_vwap_returns(self):
        data = self.data.copy()
        result = get_vwap_returns(data, 10)
        benchmark = (data.LEAD11_AdjVwap / data.LEAD1_AdjVwap) - 1
        assert_array_equal(benchmark.values, result.Ret10.values)

        mkt_data = data.copy()
        mkt_data['SecCode'] = 'HEDGE'
        mkt_data.drop_duplicates('Date', inplace=True)
        result = get_vwap_returns(data, 10, hedged=True, market_data=mkt_data)
        mkt_rets = (mkt_data.LEAD11_AdjVwap / mkt_data.LEAD1_AdjVwap) - 1
        benchmark =  benchmark.values - mkt_rets.append(mkt_rets).values
        assert_array_equal(benchmark, result.Ret10.values)

    def test_act_vs_est_missbeat(self):
        est_data = self.data.copy()
        est_data['act'] = 10
        est_data['est'] = 8
        result = act_vs_est_missbeat(est_data, 'est', 'act', 'test')

        benchmark = est_data.copy()
        benchmark['test'] = .25
        benchmark.loc[benchmark.Date < dt.date(2015,1,3), 'test'] = np.nan
        assert_frame_equal(result, benchmark)

        est_data = self.data.copy()
        est_data['act'] = range(len(est_data))
        est_data['est'] = 8
        result = act_vs_est_missbeat(est_data, 'est', 'act', 'test')

        benchmark = est_data.copy()
        benchmark['test'] = np.array(range(len(est_data)))/8. -1
        benchmark.loc[benchmark.Date < dt.date(2015,1,3), 'test'] = np.nan
        assert_frame_equal(result, benchmark)

    def test_get_industry_binaries(self):
        ind_data = self.data.copy()
        ind_data['GGROUP'] = [4510] * 8 + [5510] * 8
        result = get_industry_binaries(ind_data)

        benchmark = ind_data.copy()
        benchmark['Ind10'] = 0
        benchmark['Ind15'] = 0
        benchmark['Ind20'] = 0
        benchmark['Ind25'] = 0
        benchmark['Ind30'] = 0
        benchmark['Ind35'] = 0
        benchmark['Ind40'] = 0
        benchmark['Ind45'] = [1] * 8 + [0] * 8
        benchmark['Ind50'] = 0
        benchmark['Ind55'] = [0] * 8 + [1] * 8
        benchmark['Ind60'] = 0
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()


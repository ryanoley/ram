import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from gearbox import convert_date_array
from ram.strategy.starmine.data.features import *


class TestDataContainer1(unittest.TestCase):


    def setUp(self):
        dates = ['2015-01-01', '2015-01-02', '2015-01-03', '2015-01-04',
                 '2015-01-05', '2015-01-06', '2015-01-07', '2015-01-08']
        data = pd.DataFrame()
        data['SecCode'] = ['1234'] * 8
        data['Date'] = convert_date_array(dates)
        data['AdjClose'] = [1, 2, 3, 4, 5, 6, 7, 8]
        data['EARNINGSFLAG'] = [0, 0, 1, 0, 0, 0, 0, 0]
        data['EPSESTIMATEFQ1'] = [.5, .5, .75, .85, .85, .85, .85, .85]
        data['EPSESTIMATEFQ2'] = [.75, .75, 1., 1., 1., 1., 1., 1.]
        data2 = data.copy()
        data2['SecCode'] = ['5678'] * 8
        data2['AdjClose'] = data2.AdjClose * 10
        data = data.append(data2).reset_index(drop=True)
        data['AdjVwap'] = data['AdjClose'].copy()
        self.data = data

        dataB = pd.DataFrame()
        dataB['SecCode'] = ['1234'] * 8
        dataB['Date'] = convert_date_array(dates)
        dataB['EARNINGSFLAG'] = [0, 1, 0, 0, 0, 1, 0, 0]
        dataB['EARNINGSRETURN'] = [.1, .1, .5, .5, .5, .5, .7, .7]
        data2 = dataB.copy()
        data2['SecCode'] = ['5678'] * 8
        dataB = dataB.append(data2).reset_index(drop=True)
        self.dataB = dataB


    def test_filter_entry_window(self):

        result = filter_entry_window(self.data, 2)
        self.assertEqual(len(result), 4)
        result = filter_entry_window(self.data, 3)
        self.assertEqual(len(result), 6)

    def test_get_previous_ern_return(self):
        result = get_previous_ern_return(self.dataB, fillna=True)
        benchmark = np.array([0., 0., .1, .1, .1, .1, .5, .5] * 2)
        assert_array_equal(np.round(result.PrevRet, 3), np.round(benchmark, 3))

        prev_data = self.dataB.copy()
        prev_data['Date'] = [x - dt.timedelta(356) for x in prev_data.Date]
        result = get_previous_ern_return(self.dataB, fillna=True,
                                         prior_data = prev_data)
        benchmark = np.array([.5, .5, .1, .1, .1, .1, .5, .5] * 2)
        assert_array_equal(np.round(result.PrevRet, 3), np.round(benchmark, 3))

    def test_get_se_revisions(self):
        result = get_se_revisions(self.data, 'EPSESTIMATE', 'eps', 3)
        result = np.array(result['eps'])
        benchmark = np.array([0., 0., 0., .1, .1, 0., 0., 0.] * 2)
        assert_array_equal(np.round(result, 3), np.round(benchmark, 3))

        result = get_se_revisions(self.data, 'EPSESTIMATE', 'eps', 5)
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

    def test_make_anchor_ret_rank(self):
        data = self.data.copy()
        data = ern_date_blackout(data, -1, 1)
        result = make_anchor_ret_rank(data)
        pass



if __name__ == '__main__':
    unittest.main()

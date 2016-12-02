import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

# Create correlated data using Cholesky decomposition
from numpy.linalg import cholesky as chol

from ram.data.dh_file import DataHandlerFile
from ram.strategy.statarb.pairselector.pairs1 import PairsStrategy1


class PortfolioConstructorShell(object):
    pass


class TestPairs(unittest.TestCase):
    """
    Tests the implementation class DataClass
    """
    def setUp(self):
        # Dimensions for five quarters and five stocks
        dims = (325, 3)
        # Independent random variables
        np.random.seed(124)
        X = np.matrix(np.random.normal(size=dims))
        # Create correlation matrix
        sigma = np.eye(3).astype('f')
        sigma[0, 1] = sigma[1, 0] = 0.85
        sigma[0, 2] = sigma[2, 0] = 0.49
        sigma[1, 2] = sigma[2, 1] = 0.04
        sigma = np.asmatrix(sigma)
        # Correlated random variables
        # Cholesky decomposition: LX = Z.
        Z = np.array((chol(sigma) * X.T).T) / 100. + 1
        Z = np.cumprod(Z, axis=0)
        close = Z.T.flatten()
        # Simulate close price data frame
        dates = pd.date_range(start='2014-01-01', end='2015-03-31')
        inds = np.array([d.weekday() not in [5, 6] for d in dates])
        dates = dates[inds]
        dates = np.array([d.to_datetime() for d in dates] * 3)
        ids = ['a', 'b', 'c'] * (len(dates) / 3)
        ids.sort()
        data = pd.DataFrame({'Date': dates, 'SecCode': ids, 'ADJClose': close})
        self.bdh = DataHandlerFile(data)
        # Smaller example
        dates = pd.date_range(start='2014-03-30', end='2014-04-04')
        dates = np.array([d.to_datetime() for d in dates] * 2)
        ids = np.array(['a', 'b'] * 6)
        ids.sort()
        close = range(1, 13)
        data = pd.DataFrame({'Date': dates, 'SecCode': ids, 'ADJClose': close})
        self.bdh2 = DataHandlerFile(data)

    def test_get_corr_coef(self):
        df = pd.DataFrame({'V1': range(10), 'V2': range(10)[::-1],
                           'V3': [2, 4, 3, 1, 5, 4, 3, 2, 5, 2]})
        df_a = np.array(df)
        result = PairsStrategy1._get_corr_coef(df_a)
        benchmark = np.array([1, -1.0, 0.09373])
        assert_array_equal(result[0].round(5), benchmark)

    def test_get_corr_moves(self):
        df = pd.DataFrame({'V1': [1, -1, 1, 1], 'V2': [1, 1, 1, 1],
                           'V3': [-1, -1, -1, 1], 'V4': [-1, 1, -1, -1]})
        rets_a = np.array(df)
        result = PairsStrategy1._get_corr_moves(rets_a[:, 0], rets_a)
        benchmark = np.array([1, .75, .5, .0])
        assert_array_equal(result, benchmark)

    def test_get_vol_ratios(self):
        df = pd.DataFrame({'V1': range(10), 'V2': range(10)[::-1],
                           'V3': [2, 4, 3, 1, 5, 4, 3, 2, 5, 2]})
        df_a = np.array(df)
        result = PairsStrategy1._get_vol_ratios(df_a[:, 0], df_a)
        benchmark = np.array([1.0, 1.0, 0.45260])
        assert_array_equal(result.round(5), benchmark)

    def test_get_stats_all_pairs(self):
        # Some extra dates to make sure filter dates gets done correctly
        pairs = PairsStrategy1()
        t_start = dt.datetime(2014, 1, 1)
        t_end = dt.datetime(2015, 3, 31)
        data = self.bdh.get_filtered_univ_data(
                               univ_size=3,
                               features='ADJClose',
                               start_date=t_start,
                               end_date=t_end)
        close_data = data.pivot(index='Date',
                                columns='SecCode',
                                values='ADJClose')
        result = pairs._get_stats_all_pairs(close_data)
        self.assertListEqual(result.Leg1.tolist(), ['a', 'a', 'b'])
        self.assertListEqual(result.Leg2.tolist(), ['b', 'c', 'c'])
        benchmark = ['Leg1', 'Leg2', 'corrcoef', 'corrmoves', 'volratio']
        self.assertListEqual(result.columns.tolist(), benchmark)

    def test_get_spread_zscores(self):
        prices = pd.DataFrame({'V1': [10, 12, 15, 14],
                               'V2': [21, 24, 25, 22]})
        close1 = prices.loc[:, 'V1']
        close2 = prices.loc[:, 'V2']
        pairs = PairsStrategy1()
        results = pairs._get_spread_zscores(close1, close2, 3)
        benchmark = pd.Series([np.nan, np.nan, 1.131308968, 0.795301976],
                              name='V1')
        assert_series_equal(results, benchmark)
        # Missing values in close prices
        prices = pd.DataFrame({'V1': [10, 12, 15, np.nan],
                               'V2': [21, 24, 25, 22]})
        close1 = prices.loc[:, 'V1']
        close2 = prices.loc[:, 'V2']
        results = pairs._get_spread_zscores(close1, close2, 3)
        benchmark = pd.Series([np.nan, np.nan, 1.131308968, np.nan],
                              name='V1')
        assert_series_equal(results, benchmark)

    def test_get_test_zscores(self):
        pairs = PairsStrategy1()
        t_start = dt.datetime(2014, 1, 1)
        t_eval = dt.datetime(2015, 1, 1)
        t_end = dt.datetime(2015, 3, 31)
        data = self.bdh.get_filtered_univ_data(
                               univ_size=3,
                               features='ADJClose',
                               start_date=t_start,
                               end_date=t_end)
        close = data.pivot(index='Date',
                           columns='SecCode',
                           values='ADJClose')
        result = pairs.get_best_pairs(data, t_eval)
        benchmark = close.index[close.index >= t_eval]
        self.assertEqual(len(result), len(benchmark))

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

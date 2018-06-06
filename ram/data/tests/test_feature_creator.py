import os
import json
import shutil
import unittest
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal, assert_array_almost_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal

from ram.data.feature_creator import *


class TestFeatureCreator(unittest.TestCase):

    def setUp(self):
        pass

    def test_clean_pivot_raw_data(self):
        df = pd.DataFrame()
        df['SecCode'] = ['a'] * 3 + ['b'] * 3 + ['c'] * 3
        df['Date'] = [dt.date(2010, 1, i) for i in range(1, 4)] * 3
        df['AdjHigh'] = [np.nan, 2, 3, 4, 5, 6, 7, np.nan, 9]
        result = clean_pivot_raw_data(df, 'AdjHigh')
        # # Median values: 5.5, 3.5, 6.0
        benchmark = result.copy()
        benchmark['a'] = [np.nan, 2, 3]
        benchmark['b'] = [4, 5, 6.]
        benchmark['c'] = [7, 7, 9.]
        assert_frame_equal(result, benchmark)
        #
        df = pd.DataFrame()
        df['SecCode'] = ['a'] * 7
        df['Date'] = [dt.date(2010, 1, i) for i in range(1, 8)]
        df['AdjHigh'] = [10] + [np.nan] * 6
        result = clean_pivot_raw_data(df, 'AdjHigh')
        # # Median values: 5.5, 3.5, 6.0
        benchmark = result.copy()
        benchmark['a'] = [10] * 6 + [np.nan] * 1
        assert_frame_equal(result, benchmark)

    def test_data_rank(self):
        df = pd.DataFrame({
            'SecCode': ['a', 'a', 'a', 'b', 'b', 'b',
                        'c', 'c', 'c', 'd', 'd', 'd',
                        'e', 'e', 'e'],
            'Date': [1, 2, 3] * 5,
            'V1': [1, 2, 1, 2, 2, 1, 100, -100,
                   323, 3, 3, 3, np.nan, np.nan, 2]
        })
        df = clean_pivot_raw_data(df, 'V1')
        result = data_rank(df)
        benchmark = df.copy()
        benchmark['a'] = [0.25, 0.625, 0.300]
        benchmark['b'] = [0.50, 0.625, 0.300]
        benchmark['c'] = [1.00, 0.25, 1.00]
        benchmark['d'] = [0.75, 1.00, 0.8]
        benchmark['e'] = [np.nan, np.nan, 0.6]
        assert_frame_equal(result, benchmark)

    def test_FeatureAggregator(self):
        # DataFrame with multiple dates and SecCodes in columns
        data = pd.DataFrame(columns=['A', 'B', 'C'])
        data.loc[dt.date(2010, 1, 1)] = [1, 2, 3]
        data.loc[dt.date(2010, 1, 2)] = [1, 2, 3]
        data.loc[dt.date(2010, 1, 3)] = [1, 2, 3]
        data = data.astype(float)
        data.columns.name = 'SecCode'
        data.index.name = 'Date'
        feat = FeatureAggregator()
        feat.add_feature(data, 'VAR1')
        feat.add_feature(data * -9, 'VAR2')
        data[:] = np.nan
        feat.add_feature(data, 'VAR3')
        result = feat.make_dataframe()
        benchmark = pd.DataFrame()
        benchmark['SecCode'] = ['A', 'A', 'A', 'B', 'B', 'B', 'C', 'C', 'C']
        benchmark['Date'] = [dt.date(2010, 1, 1), dt.date(2010, 1, 2),
                             dt.date(2010, 1, 3)] * 3
        benchmark['VAR1'] = [1, 1, 1, 2, 2, 2, 3, 3, 3.]
        benchmark['VAR2'] = [-9, -9, -9, -18, -18, -18, -27, -27, -27.]
        benchmark['VAR3'] = np.nan
        benchmark.VAR3 = benchmark.VAR3.astype(object)  # To make test work
        assert_frame_equal(result, benchmark)

    def test_prma(self):
        dates = [dt.date(2010, 1, 1) + dt.timedelta(days=x) for x in range(4)]
        data = pd.DataFrame()
        data['V1'] = [2, 6, 2, 8]
        data['V2'] = [2, 6, 2, 3]
        data.index = dates
        result1 = PRMA().calculate_all_dates(data, 2)
        benchmark = data.copy()
        benchmark['V1'] = [np.nan, 1.5, 0.5, 1.6]
        benchmark['V2'] = [np.nan, 1.5, 0.5, 1.2]
        assert_frame_equal(result1, benchmark)
        result2 = PRMA().calculate_last_date(data, 2)
        benchmark = pd.Series([1.6, 1.2], index=['V1', 'V2'])
        assert_series_equal(result2, benchmark)

    def test_discount(self):
        dates = [dt.date(2010, 1, 1) + dt.timedelta(days=x) for x in range(5)]
        data = pd.DataFrame()
        data['V1'] = [2, 6, 3, 8, 6]
        data['V2'] = [2, 8, 2, 10, np.nan]
        data.index = dates
        result1 = DISCOUNT().calculate_all_dates(data, 2)
        benchmark = data.copy()
        benchmark['V1'] = [np.nan, 1.0, 0.5, 1.0, 0.75]
        benchmark['V2'] = [np.nan, 1.0, 0.25, 1.0, np.nan]
        assert_frame_equal(result1, benchmark)
        result2 = DISCOUNT().calculate_last_date(data, 2)
        benchmark = pd.Series([0.75, np.nan], index=['V1', 'V2'])
        assert_series_equal(result2, benchmark)

    def test_boll(self):
        # TODO: create asserts
        dates = [dt.date(2010, 1, 1) + dt.timedelta(days=x) for x in range(4)]
        data = pd.DataFrame()
        data['V1'] = [2, 4, 8, 10]
        data['V2'] = [2, 8, 2, 10]
        data.index = dates
        result1 = BOLL().calculate_all_dates(data, 2)
        result2 = BOLL().calculate_last_date(data, 2)
        assert_array_equal(result1.iloc[-1].values, result2.values)

    def test_mfi(self):
        dates = [dt.date(2010, 1, 1) + dt.timedelta(days=x) for x in range(6)]
        data = pd.DataFrame()
        data['V1'] = [2, 3, 4, 3, 2, 7]
        data['V2'] = [2, 3, 4, 5, 6, 7]
        data['V3'] = [7, 6, 5, 4, 3, 2]
        data.index = dates
        volume = data.copy()
        volume[:] = 1
        result1 = MFI().calculate_all_dates(data, data, data, volume, 3)
        result2 = MFI().calculate_last_date(data, data, data, volume, 3)
        assert_array_equal(result1.iloc[-1].values, result2.values)

    def test_vol(self):
        dates = [dt.date(2010, 1, 1) + dt.timedelta(days=x) for x in range(6)]
        data = pd.DataFrame()
        data['V1'] = [2, 3, 4, 3, 2, 7]
        data['V2'] = [2, 3, 4, 5, 6, 7]
        data.index = dates
        result1 = VOL().calculate_all_dates(data, 3)
        result2 = VOL().calculate_last_date(data, 3)
        assert_array_almost_equal(result1.iloc[-1].values, result2.values)

    def test_rsi(self):
        dates = [dt.date(2010, 1, 1) + dt.timedelta(days=x) for x in range(6)]
        data = pd.DataFrame()
        data['V1'] = [2, 3, 4, 3, 2, 7]
        data['V2'] = [2, 3, 4, 5, 6, 7]
        data.index = dates
        result1 = RSI().calculate_all_dates(data, 3)
        result2 = RSI().calculate_last_date(data, 3)
        assert_array_almost_equal(result1.iloc[-1].values, result2.values)

    def test_data_fill_median(self):
        data = pd.DataFrame(index=range(3, 8))
        data['V1'] = [np.nan, 1, 2, 3, 4]
        data['V2'] = [np.nan, 3, 4, 5, 6]
        data['V3'] = [np.nan, np.nan, 10, 23, np.nan]
        result = data_fill_median(data, True)
        benchmark = pd.DataFrame(index=range(3, 8))
        benchmark['V1'] = [2, 1, 2, 3, 4.]
        benchmark['V2'] = [2, 3, 4, 5, 6.]
        benchmark['V3'] = [2, 2, 10, 23, 5.]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

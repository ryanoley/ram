import os
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.utils.read_write import import_sql_output

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal


DDIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'temp_ern_pead')


class TestTechnicalVars1(unittest.TestCase):

    data1 = import_sql_output(os.path.join(
        DDIR, 'earnings', 'technical_vars_1.csv'))
    data2 = import_sql_output(os.path.join(
        DDIR, 'pead', 'technical_vars_1.csv'))

    def setUp(self):
        pass

    def test_apple_data(self):
        data_earnings = self.data1[self.data1.SecCode == 6027].copy()
        data_pead = self.data2[self.data2.SecCode == 6027].copy()
        # Report Dates
        benchmark = pd.Series(
            [dt.datetime(2015, 7, 21), dt.datetime(2016, 7, 26),
             dt.datetime(2016, 1, 26), dt.datetime(2015, 4, 27),
             dt.datetime(1999, 7, 14), dt.datetime(2009, 1, 21)])
        self.assertTrue(benchmark.isin(data_earnings.ReportDate).all())
        self.assertTrue(benchmark.isin(data_pead.ReportDate).all())
        # Individual date data
        # Earnings
        test_data = data_earnings[
            data_earnings.ReportDate == dt.datetime(2016, 1, 26)].copy()
        self.assertEqual(test_data.PRMA5.iloc[0].round(6), 0.023804)
        self.assertEqual(test_data.PRMA10.iloc[0].round(6), 0.029948)
        self.assertEqual(test_data.PRMA20.iloc[0].round(6), 0.028374)
        self.assertEqual(test_data.PRMA60.iloc[0].round(6), -0.028183)
        # These rank values are just approximate ranges and taken from
        # the file as the date of writing these tests. They are to represent
        # and approximate range, if if these fail, then the universe
        # should be investigated.
        self.assertTrue(test_data.PRMA5_Rank.iloc[0] >= 0.85)
        self.assertTrue(test_data.PRMA5_Rank.iloc[0] <= 0.91)
        self.assertTrue(test_data.PRMA20_Rank.iloc[0] >= 0.80)
        self.assertTrue(test_data.PRMA20_Rank.iloc[0] <= 0.85)
        test_data = data_earnings[
            data_earnings.ReportDate == dt.datetime(1999, 7, 14)].copy()
        self.assertEqual(test_data.PRMA5.iloc[0].round(6), 0.039790)
        self.assertEqual(test_data.PRMA10.iloc[0].round(6), 0.104626)
        self.assertEqual(test_data.PRMA20.iloc[0].round(6), 0.125506)
        self.assertEqual(test_data.PRMA60.iloc[0].round(6), 0.165851)
        self.assertTrue(test_data.PRMA5_Rank.iloc[0] >= 0.89)
        self.assertTrue(test_data.PRMA5_Rank.iloc[0] <= 0.93)
        self.assertTrue(test_data.PRMA20_Rank.iloc[0] >= 0.90)
        self.assertTrue(test_data.PRMA20_Rank.iloc[0] <= 0.95)
        # Pead
        test_data = data_pead[
            data_pead.ReportDate == dt.datetime(2016, 1, 26)].copy()
        self.assertEqual(test_data.PRMA5.iloc[0].round(6), 0.001253)
        self.assertEqual(test_data.PRMA10.iloc[0].round(6), 0.009638)
        self.assertEqual(test_data.PRMA20.iloc[0].round(6), 0.015866)
        self.assertEqual(test_data.PRMA60.iloc[0].round(6), -0.037708)
        self.assertTrue(test_data.PRMA5_Rank.iloc[0] >= 0.49)
        self.assertTrue(test_data.PRMA5_Rank.iloc[0] <= 0.52)
        self.assertTrue(test_data.PRMA20_Rank.iloc[0] >= 0.65)
        self.assertTrue(test_data.PRMA20_Rank.iloc[0] <= 0.70)
        test_data = data_pead[
            data_pead.ReportDate == dt.datetime(1999, 7, 14)].copy()
        self.assertEqual(test_data.PRMA5.iloc[0].round(6), 0.018063)
        self.assertEqual(test_data.PRMA10.iloc[0].round(6), 0.091732)
        self.assertEqual(test_data.PRMA20.iloc[0].round(6), 0.138878)
        self.assertEqual(test_data.PRMA60.iloc[0].round(6), 0.179916)
        self.assertTrue(test_data.PRMA5_Rank.iloc[0] >= 0.74)
        self.assertTrue(test_data.PRMA5_Rank.iloc[0] <= 0.79)
        self.assertTrue(test_data.PRMA20_Rank.iloc[0] >= 0.90)
        self.assertTrue(test_data.PRMA20_Rank.iloc[0] <= 0.97)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

import os
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram import config

from ram.utils.read_write import import_sql_output

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal


DDIR = config.ERN_PEAD_DIR


class TestTechnicalVars2(unittest.TestCase):

    data1 = import_sql_output(os.path.join(
        DDIR, 'earnings', 'technical_vars_2.txt'))
    data2 = import_sql_output(os.path.join(
        DDIR, 'pead', 'technical_vars_2.txt'))

    def setUp(self):
        pass

    def test_ibm_data(self):
        data_earnings = self.data1[self.data1.SecCode == 36799].copy()
        data_pead = self.data2[self.data2.SecCode == 36799].copy()
        # Report Dates
        benchmark = pd.Series(
            [dt.datetime(2000, 1, 19), dt.datetime(2004, 4, 15),
             dt.datetime(2006, 10, 17), dt.datetime(2008, 1, 17),
             dt.datetime(2010, 10, 18), dt.datetime(2015, 7, 20)])
        self.assertTrue(benchmark.isin(data_earnings.ReportDate).all())
        self.assertTrue(benchmark.isin(data_pead.ReportDate).all())
        # Earnings
        test_data = data_earnings[
            data_earnings.ReportDate == dt.datetime(2015, 7, 20)].copy()
        self.assertEqual(test_data.Vol10.iloc[0].round(6), 0.009466)
        self.assertEqual(test_data.Vol30.iloc[0].round(6), 0.009094)
        self.assertEqual(test_data.Vol60.iloc[0].round(6), 0.009415)
        self.assertEqual(test_data.PercentB10.iloc[0].round(6), 0.913912)
        self.assertEqual(test_data.PercentB30.iloc[0].round(6), 1.033366)
        self.assertEqual(test_data.RSI10.iloc[0].round(4), 75.8128)        
        # Pead
        test_data = data_pead[
            data_pead.ReportDate == dt.datetime(2015, 7, 20)].copy()
        self.assertEqual(test_data.Vol10.iloc[0].round(6), 0.009313)
        self.assertEqual(test_data.Vol30.iloc[0].round(6), 0.008979)
        self.assertEqual(test_data.Vol60.iloc[0].round(6), 0.008695)
        self.assertEqual(test_data.PercentB10.iloc[0].round(6), 0.858443)
        self.assertEqual(test_data.PercentB30.iloc[0].round(6), 1.106474)
        self.assertEqual(test_data.RSI10.iloc[0].round(4), 80.6056)

    def test_uvn_data(self):
        data_earnings = self.data1[self.data1.SecCode == 78542].copy()
        data_pead = self.data2[self.data2.SecCode == 78542].copy()
        # Report Dates
        benchmark = pd.Series(
            [dt.datetime(2000, 1, 24), dt.datetime(2003, 8, 7),
             dt.datetime(2005, 2, 28), dt.datetime(2006, 8, 3)])
        self.assertTrue(benchmark.isin(data_earnings.ReportDate).all())
        self.assertTrue(benchmark.isin(data_pead.ReportDate).all())
        # Earnings
        test_data = data_earnings[
            data_earnings.ReportDate == dt.datetime(1999, 10, 18)].copy()
        self.assertEqual(test_data.Vol10.iloc[0].round(6), 0.017608)
        self.assertEqual(test_data.Vol30.iloc[0].round(6), 0.016128)
        self.assertEqual(test_data.Vol60.iloc[0].round(6), 0.014994)
        self.assertEqual(test_data.PercentB10.iloc[0].round(6), 0.465597)
        self.assertEqual(test_data.PercentB30.iloc[0].round(6), 0.552793)
        self.assertEqual(test_data.RSI10.iloc[0].round(4), 59.4286)
        # Pead
        test_data = data_pead[
            data_pead.ReportDate == dt.datetime(1999, 10, 18)].copy()
        self.assertEqual(test_data.Vol10.iloc[0].round(6), 0.019361)
        self.assertEqual(test_data.Vol30.iloc[0].round(6), 0.015357)
        self.assertEqual(test_data.Vol60.iloc[0].round(6), 0.015281)
        self.assertEqual(test_data.PercentB10.iloc[0].round(6), 0.132065)
        self.assertEqual(test_data.PercentB30.iloc[0].round(6), 0.197264)
        self.assertEqual(test_data.RSI10.iloc[0].round(4), 51.7413)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

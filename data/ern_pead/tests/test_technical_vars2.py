import os
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.utils.read_write import import_sql_output

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_series_equal, assert_frame_equal


DDIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'temp_ern_pead')


class TestTechnicalVars2(unittest.TestCase):

    print('Importing Technical Vars 2')
    data1 = import_sql_output(os.path.join(
        DDIR, 'earnings', 'technical_vars_2.csv'))
    data2 = import_sql_output(os.path.join(
        DDIR, 'pead', 'technical_vars_2.csv'))

    def setUp(self):
        pass

    def test_ibm_data(self):
        data_earnings = self.data1[self.data1.SecCode == 6027].copy()
        data_pead = self.data2[self.data2.SecCode == 6027].copy()
        # Report Dates
        benchmark = pd.Series(
            [dt.datetime(2015, 7, 21), dt.datetime(2016, 7, 26),
             dt.datetime(2016, 1, 26), dt.datetime(2015, 4, 27),
             dt.datetime(1999, 7, 14), dt.datetime(2009, 1, 21)])
        self.assertTrue(benchmark.isin(data_earnings.ReportDate).all())
        self.assertTrue(benchmark.isin(data_pead.ReportDate).all())

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

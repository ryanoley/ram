import os
import unittest
import numpy as np
import pandas as pd
import datetime as dt

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_frame_equal

from ram.utils.read_write import import_sql_output


class TestReadWrite(unittest.TestCase):

    def setUp(self):
        # SQL FILE OUTPUT
        with open('sql_file_output.txt', 'w') as f:
            f.write("X1  |X2  |ReportDate\n")
            f.write("----!----\n")
            f.write("Test, comma|12.34|2015-02-03 10:34:23.23423\n")

    def test_import_sql_output(self):
        result = import_sql_output('sql_file_output.txt')
        benchmark = pd.DataFrame()
        benchmark['X1'] = ['Test comma']
        benchmark['X2'] = [12.34]
        benchmark['ReportDate'] = [dt.datetime(2015, 2, 3)]
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        os.remove('sql_file_output.txt')


if __name__ == '__main__':
    unittest.main()

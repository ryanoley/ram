import unittest
import numpy as np
import datetime as dt

from numpy.testing import assert_array_equal

from ram.utils.time_funcs import check_input_date, convert_date_array


class TestTimeFuncs(unittest.TestCase):

    def setUp(self):
        pass

    def test_check_input_date(self):
        result = check_input_date('2010-01-01')
        benchmark = dt.datetime(2010, 1, 1)
        self.assertEqual(result, benchmark)
        result = check_input_date(dt.datetime(2010, 1, 1, 14, 23, 23, 233))
        benchmark = dt.datetime(2010, 1, 1)
        self.assertEqual(result, benchmark)
        result = check_input_date('01/20/1993')
        benchmark = dt.datetime(1993, 1, 20)
        self.assertEqual(result, benchmark)
        result = check_input_date('01/20/1993 10:20:30')
        benchmark = dt.datetime(1993, 1, 20)
        self.assertEqual(result, benchmark)
        result = check_input_date('01/20/93 10:20:30')
        benchmark = dt.datetime(1993, 1, 20)
        self.assertEqual(result, benchmark)

    def test_convert_date_array(self):
        dates = ['2010-01-01', '2010-02-01']
        result = convert_date_array(dates)
        benchmark = np.array([dt.datetime(2010, 1, 1),
                              dt.datetime(2010, 2, 1)])
        assert_array_equal(result, benchmark)
        dates = [dt.date(2010, 1, 1), dt.date(2010, 2, 1)]
        result = convert_date_array(dates)
        benchmark = np.array([dt.datetime(2010, 1, 1),
                              dt.datetime(2010, 2, 1)])
        assert_array_equal(result, benchmark)
        dates = ['20100101', '20100201']
        result = convert_date_array(dates)
        benchmark = np.array([dt.datetime(2010, 1, 1),
                              dt.datetime(2010, 2, 1)])
        assert_array_equal(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

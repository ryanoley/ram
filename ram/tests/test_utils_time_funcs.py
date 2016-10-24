import unittest
import datetime as dt

from ram.utils.time_funcs import check_input_date


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

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

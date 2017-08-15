import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.main import LongPeadStrategy, make_arg_iter


class TestMain(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_univ_filter_args(self):
        strategy = LongPeadStrategy()
        result = strategy.get_univ_filter_args()
        self.assertEqual(result['filter'], 'AvgDolVol')
        self.assertTrue(result['where'].find('MarketCap') > -1)
        self.assertTrue('univ_size' in result)

    def test_get_univ_date_parameters(self):
        strategy = LongPeadStrategy()
        result = strategy.get_univ_date_parameters()
        self.assertTrue('start_year' in result)
        self.assertTrue('train_period_length' in result)
        self.assertTrue('frequency' in result)
        self.assertTrue('test_period_length' in result)

    def test_make_arg_iter(self):
        parameters = {'V1': [1, 2], 'V2': [3, 4]}
        result = make_arg_iter(parameters)
        benchmark = [{'V1': 1, 'V2': 3}, {'V1': 1, 'V2': 4},
                     {'V1': 2, 'V2': 3}, {'V1': 2, 'V2': 4}]
        self.assertListEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

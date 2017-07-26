
import unittest
import datetime as dt
import numpy as np
from numpy.testing import assert_array_equal

from ram.data.data_handler_sql import DataHandlerSQL


class TestDataHandlerSQL(unittest.TestCase):

    def setUp(self):
        self.dh = DataHandlerSQL()
        assert self.dh._connection.connected == 1

    def test_prior_trading_date(self):
        result = self.dh.prior_trading_date('2017-03-01')
        benchmark = dt.date(2017, 2, 28)
        self.assertEqual(result, benchmark)
        result = self.dh.prior_trading_date(['2017-03-01', '2017-07-15',
                                             '2017-01-01'])
        benchmark = [dt.date(2017, 2, 28), dt.date(2017, 7, 14),
                     dt.date(2016, 12, 30)]
        assert_array_equal(result, benchmark)
        result = self.dh.prior_trading_date('notadate')
        self.assertTrue(np.isnan(result))

    def test_map_ticker_to_id(self):
        result = self.dh._map_ticker_to_id('SPY')
        self.assertEqual(result.SecCode.iloc[0], 61494)
        self.assertEqual(result.Ticker.iloc[0], 'SPY')

    def tearDown(self):
        self.dh.close_connections()


if __name__ == '__main__':
    unittest.main()

import unittest
import numpy as np

from ram.strategy.analyst_estimates.base.position import Position


class TestPosition(unittest.TestCase):

    def setUp(self):
        pass

    def test_update_position_size(self):
        position = Position('IBM', 100., comm=.005)

        position.update_position_size(1000., 101.)
        self.assertEqual(position.shares, 10)
        self.assertEqual(position.exposure, 1010.)
        self.assertEqual(position.daily_pl, -.05)
        self.assertEqual(position.daily_turnover, 1010.)

        position.update_position_size(0., 99.)
        self.assertEqual(position.shares, 0)
        self.assertEqual(position.exposure, 0.)
        self.assertEqual(position.daily_pl, -20.1)
        self.assertEqual(position.daily_turnover, 2000.)

    def test_update_position_prices(self):
        position = Position('IBM', 100., comm=.005)

        position.update_position_prices(101.)
        self.assertEqual(position.exposure, 0.)
        self.assertEqual(position.daily_pl, 0.)
        self.assertEqual(position.cumulative_return, 0.)
        self.assertEqual(position.current_price, 101.)

        position.update_position_size(1010., 101.)
        self.assertEqual(position.daily_pl, -.05)
        self.assertEqual(position.shares, 10)

        position.update_position_prices(103.)
        self.assertEqual(position.exposure, 1030.)
        self.assertEqual(position.daily_pl, 19.95)
        self.assertEqual(position.cumulative_return, 19.95 / 1030)
        self.assertEqual(position.current_price, 103.)

    def test_split_adjustment(self):
        position = Position('IBM', 100., comm=.005)

        position.update_position_size(1000., 100.)
        self.assertEqual(position.shares, 10)

        position.split_adjustment(2)
        self.assertEqual(position.shares, 20)
        self.assertEqual(position.current_price, 50)

    def test_dividend_adjustment(self):
        position = Position('IBM', 100., comm=.005)

        position.update_position_size(1000., 100.)
        self.assertEqual(position.shares, 10)
        self.assertEqual(position.daily_pl, -.05)

        position.dividend_adjustment(.25)
        self.assertEqual(position.daily_pl, 2.45)

    def test_close_position(self):
        position = Position('IBM', 100., comm=.005)

        position.update_position_size(1000., 100.)
        self.assertEqual(position.shares, 10)
        self.assertEqual(position.daily_pl, -.05)
        self.assertEqual(position.daily_turnover, 1000.)

        position.update_position_prices(101.)
        self.assertEqual(position.daily_pl, 9.95)

        position.close_position()
        self.assertEqual(np.round(position.daily_pl, 4), 9.90)
        self.assertEqual(position.daily_turnover, 2010.)
        self.assertEqual(position.shares, 0)
        self.assertEqual(position.exposure, 0)
        self.assertEqual(position.position_weight, 0.)

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()


import unittest
import numpy as np


from ram.strategy.sandbox.base.hedged_position import HedgedPosition


class TestHedgedPosition(unittest.TestCase):

    def setUp(self):
        self.comm = .005

    def test_update_position_size(self):
        position = HedgedPosition('IBM', 100., comm=self.comm)

        position.update_position_size(1000., 100.)
        self.assertEqual(position.shares, 10)
        self.assertEqual(position.daily_pl, -.05)

        position.update_hedge_price({'HEDGE':20.})
        self.assertEqual(position.market_entry_price, 20.)
        self.assertEqual(position.market_curent_price, 20.)

        position.update_position_prices(101.)
        self.assertEqual(position.daily_pl, 9.95)
        self.assertEqual(position.exposure, 1010.)
        self.assertEqual(position.cumulative_return, 9.95 / 1010)
        self.assertEqual(position.current_price, 101.)

        position.update_hedge_price({'HEDGE':19.})
        self.assertEqual(position.market_entry_price, 20.)
        self.assertEqual(position.market_curent_price, 19.)
        self.assertEqual(np.round(position.market_return, 4), -.05)
        self.assertEqual(np.round(position.cumulative_return, 5),
                         np.round((9.95 / 1010) + .05, 5))

        position = HedgedPosition('IBM', 100., comm=self.comm)

        position.update_position_size(-1000., 100.)
        self.assertEqual(position.shares, -10)
        self.assertEqual(position.daily_pl, -.05)

        position.update_hedge_price({'HEDGE':20.})
        self.assertEqual(position.market_entry_price, 20.)
        self.assertEqual(position.market_curent_price, 20.)

        position.update_position_prices(101.)
        self.assertEqual(position.daily_pl, -10.05)
        self.assertEqual(position.exposure, -1010.)
        self.assertEqual(position.cumulative_return, -10.05 / 1010)
        self.assertEqual(position.current_price, 101.)

        position.update_hedge_price({'HEDGE':19.})
        self.assertEqual(position.market_entry_price, 20.)
        self.assertEqual(position.market_curent_price, 19.)
        self.assertEqual(np.round(position.market_return, 4), -.05)
        self.assertEqual(np.round(position.cumulative_return, 5),
                         np.round((-10.05 / 1010) - .05, 5))

        position = HedgedPosition('IBM', 100., comm=self.comm)

        position.update_position_size(-1000., 100.)
        self.assertEqual(position.shares, -10)
        self.assertEqual(position.daily_pl, -.05)

        position.update_hedge_price({'HEDGE':20.})
        self.assertEqual(position.market_entry_price, 20.)
        self.assertEqual(position.market_curent_price, 20.)

        position.update_position_prices(101.)
        self.assertEqual(position.daily_pl, -10.05)
        self.assertEqual(position.exposure, -1010.)
        self.assertEqual(position.cumulative_return, -10.05 / 1010)
        self.assertEqual(position.current_price, 101.)

        position.update_hedge_price({'HEDGE':21.})
        self.assertEqual(position.market_entry_price, 20.)
        self.assertEqual(position.market_curent_price, 21.)
        self.assertEqual(np.round(position.market_return, 4), .05)
        self.assertEqual(np.round(position.cumulative_return, 5),
                         np.round((-10.05 / 1010) + .05, 5))

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
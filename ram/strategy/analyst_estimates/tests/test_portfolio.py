import unittest
import numpy as np


from ram.strategy.analyst_estimates.base.portfolio import Portfolio


class TestPortfolio(unittest.TestCase):

    def setUp(self):
        portfolio = Portfolio()
        closes = {'IBM':100, 'AAPL':50}
        portfolio.update_prices(closes)
        self.portfolio = portfolio

    def test_update_prices(self):
        self.assertListEqual(self.portfolio.positions.keys(), ['AAPL', 'IBM'])

        closes = {'IBM':101., 'AAPL':51.}
        self.portfolio.update_prices(closes)
        self.assertEqual(self.portfolio.positions['AAPL'].current_price, 51.)
        self.assertEqual(self.portfolio.positions['IBM'].current_price, 101.)

        closes = {'IBM':100., 'AAPL':50.}
        self.portfolio.update_prices(closes)

    def test_update_position_sizes(self):
        sizes = {'IBM':100, 'AAPL':-100}
        closes = {'IBM':100., 'AAPL':50.}
        self.portfolio.update_position_sizes(sizes, closes)
        self.assertEqual(self.portfolio.positions['IBM'].shares, 1)
        self.assertEqual(self.portfolio.positions['AAPL'].shares, -2)
        self.assertEqual(self.portfolio.positions['IBM'].daily_pl, -.005)
        self.assertEqual(self.portfolio.positions['AAPL'].daily_pl, -.01)
        self.assertEqual(self.portfolio.positions['IBM'].daily_turnover, 100)
        self.assertEqual(self.portfolio.positions['AAPL'].daily_turnover, 100)

        sizes = {'IBM':0, 'AAPL':0}
        self.portfolio.update_position_sizes(sizes, closes)
        self.portfolio.reset_daily_pl()
        self.portfolio.get_portfolio_daily_turnover()

    def test_add_sector_info(self):
        sectors = {'IBM':[4510], 'AAPL':[5510, 4510]}
        self.portfolio.add_sector_info(sectors)
        self.assertTrue(np.isnan(self.portfolio.positions['AAPL'].sector))
        self.assertEqual(self.portfolio.positions['IBM'].sector, '45')

        sectors = {'IBM':[4510], 'AAPL':[5510]}
        self.portfolio.add_sector_info(sectors)
        self.assertEqual(self.portfolio.positions['AAPL'].sector, '55')
        self.assertEqual(self.portfolio.positions['IBM'].sector, '45')

    def test_get_portfolio_daily_pl(self):
        sizes = {'IBM':100, 'AAPL':-100}
        closes = {'IBM':100., 'AAPL':50.}
        self.portfolio.update_position_sizes(sizes, closes)
        pl_long, pl_short = self.portfolio.get_portfolio_daily_pl()
        self.assertEqual(pl_long, -.005)
        self.assertEqual(pl_short, -.01)

        closes = {'IBM':101., 'AAPL':49.}
        self.portfolio.update_prices(closes)
        pl_long, pl_short = self.portfolio.get_portfolio_daily_pl()
        self.assertEqual(pl_long, 0.995)
        self.assertEqual(pl_short, 1.99)

        sizes = {'IBM':0, 'AAPL':0}
        closes = {'IBM':100., 'AAPL':50.}
        self.portfolio.update_position_sizes(sizes, closes)
        self.portfolio.reset_daily_pl()
        self.portfolio.get_portfolio_daily_turnover()

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()

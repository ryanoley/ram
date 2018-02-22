import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb2.version_005.constructor import *


class TestPortfolioConstructor(unittest.TestCase):

    def setUp(self):
        self.ids = ['a_b~c_d', 'b_d~e_f', 'c_a~f_z', 'f_b~s_z', 'a_b~z_r']

    def Xtest_get_day_position_sizes(self):
        cons = PortfolioConstructor()
        zscores = pd.Series([10, -10, 5, -5, 0],
                            index=self.ids)
        result = cons.get_day_position_sizes(zscores)

    def Xtest_PortfolioContainer_add_port(self):
        port = PortfolioContainer(3, 0.5)
        port.add_port(self.ids[0], 1)
        benchmark = {'a': 1, 'c': -1, 'b': 1, 'd': -1}
        self.assertDictEqual(port._positions, benchmark)
        benchmark = {'a_b~c_d': 1}
        self.assertDictEqual(port._ports, benchmark)

    def Xtest_PortfolioContainer_check_new_port(self):
        port = PortfolioContainer(3, 0.5)
        port.add_port(self.ids[0], 1)
        self.assertFalse(port.check_new_port(self.ids[4], 1))
        self.assertTrue(port.check_new_port(self.ids[4], -1))

    def Xtest_PortfolioContainer_check_zscore(self):
        port = PortfolioContainer(3, 0.5)
        port.add_port(self.ids[0], 1)
        port.check_zscore(self.ids[0], -0.5)
        benchmark = {'a': 0, 'c': 0, 'b': 0, 'd': 0}
        self.assertDictEqual(port._positions, benchmark)

    def Xtest_PortfolioContainer_get_sizes(self):
        port = PortfolioContainer(3, 0.5)
        port.add_port(self.ids[0], 1)
        result = port.get_sizes()
        benchmark = {'a': 0.25, 'c': -0.25, 'b': 0.25, 'd': -0.25}
        self.assertDictEqual(result, benchmark)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

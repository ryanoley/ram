import unittest
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.main import StatArbStrategy


class TestMain(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_column_params(self):
        strategy = StatArbStrategy(strategy_code_version='version_001')
        strategy.strategy_init()
        result = strategy.get_column_parameters()
        result = result[0]
        self.assertTrue('drop_market_variables' in result)
        self.assertTrue('training_qtrs' in result)
        self.assertTrue('params' in result)
        self.assertTrue('drop_ibes' in result)
        #self.assertTrue('model_params' in result)
        self.assertTrue('drop_accounting' in result)
        self.assertTrue('drop_starmine' in result)
        self.assertTrue('response_params' in result)

    def test_capture_output(self):
        strategy = StatArbStrategy(strategy_code_version='version_001')
        strategy.strategy_init()
        stats = {}
        results = pd.DataFrame({'PL': [1, 2, 3]})
        results.index = ['2010-01-01', '2010-01-02', '2010-01-03']
        strategy._capture_output(results, stats, arg_index=0)
        strategy._capture_output(results, stats, arg_index=1)
        self.assertListEqual(strategy.output_returns.columns.tolist(), [0, 1])
        self.assertListEqual(strategy.output_all_output.columns.tolist(),
                             ['PL_0', 'PL_1'])

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

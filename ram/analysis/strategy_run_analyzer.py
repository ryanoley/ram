import os
import numpy as np
import pandas as pd

from gearbox import convert_date_array

from ram.config import SIMULATION_OUTPUT_DIR


class StrategyRunAnalyzer(object):

    def __init__(self, strategy, run_name):
        self.strategy = strategy
        self.run_name = run_name
        self._import_returns()

    def _import_returns(self):
        returns = pd.DataFrame([])
        dpath = os.path.join(SIMULATION_OUTPUT_DIR, self.strategy,
                             self.run_name, 'index_outputs')
        for file_name in os.listdir(dpath):
            rets = pd.read_csv(os.path.join(dpath, file_name), index_col=0)
            returns = returns.add(rets, fill_value=0)
        returns.index = convert_date_array(returns.index)
        self.returns = returns

import json
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base2 import Strategy2

from ram.strategy.birds.signals.signals1 import Signals1
from ram.strategy.birds.constructor.constructor import PortfolioConstructor


class BirdsStrategy(Strategy2):

    def get_column_parameters(self):
        return []

    def run_index(self, index):
        import pdb; pdb.set_trace()
        data = self.read_data_from_index(index)
        # Signal Generator
        signals = Signals1(.25)
        signals.register_index_variables(['PRMA10_AdjClose'])
        data_signals = signals.generate_portfolio_signals(data)
        # Portfolio Constructor
        constructor = PortfolioConstructor()
        constructor.set_and_prep_data(data, data_signals)
        results = constructor.get_daily_pl()

if __name__ == '__main__':
    strategy = BirdsStrategy('version_020')
    strategy.start()

import json
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base2 import Strategy2

from ram.strategy.birds.signals.signals1 import Signals1
from ram.strategy.birds.constructor.constructor import PortfolioConstructor


FEATURES = ['PRMA5_AvgDolVol', 'PRMA10_AvgDolVol','PRMA20_AvgDolVol',
            'PRMA5_AdjClose', 'PRMA10_AdjClose', 'PRMA20_AdjClose',
            'VOL5_AdjClose', 'VOL10_AdjClose', 'VOL20_AdjClose',
            'BOLL5_AdjClose', 'BOLL10_AdjClose', 'BOLL20_AdjClose',
            'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose',
            'DISCOUNT252_AdjClose',
            'RSI5_AdjClose', 'RSI10_AdjClose', 'RSI20_AdjClose',
            'MFI5_AdjClose', 'MFI10_AdjClose', 'MFI20_AdjClose',
            'ACCTPRICESALES', 'ACCTEPSGROWTH', 'EARNINGSRETURN']


class BirdsStrategy(Strategy2):

    def get_column_parameters(self):
        return []

    def run_index(self, index):
        data = self.read_data_from_index(index)
        # Signal Generator
        signals = Signals1(.25)
        signals.register_index_variables(FEATURES)
        data_signals = signals.generate_portfolio_signals(data)
        # Portfolio Constructor
        constructor = PortfolioConstructor()
        constructor.set_and_prep_data(data, data_signals)
        results = constructor.get_daily_pl()
        self.write_index_results(results, index)


if __name__ == '__main__':

    strategy = BirdsStrategy('version_025', True)
    strategy.start()

import json
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy

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


class BirdsStrategy(Strategy):

    signal_args = {
        'quantile': [0.15, 0.20, 0.25, 0.30],
        'feature': FEATURES
    }

    constructor_args = {
    }

    def get_column_parameters(self):
        args1 = make_arg_iter(self.signal_args)
        #args2 = make_arg_iter(self.constructor_args)
        output = {}
        for i, x in enumerate(args1):
            z = {}
            z.update(x)
            output[i] = z
        return output

    def run_index(self, index):

        data = self.read_data_from_index(index)

        signals = Signals1()
        constructor = PortfolioConstructor()

        # Iterable arguments
        args1 = make_arg_iter(self.signal_args)
        #args2 = make_arg_iter(self.constructor_args)
        ind = 0
        output_results = pd.DataFrame()
        output_stats = {}

        for a1 in args1:
            data_signals = signals.generate_portfolio_signals(data, **a1)
            # Portfolio Constructor
            constructor.set_and_prep_data(data, data_signals)
            results, stats = constructor.get_daily_pl()
            results.columns = [ind]
            output_results = output_results.join(results, how='outer')
            output_stats[ind] = stats
            ind += 1

        self.write_index_results(output_results, index)
        self.write_index_stats(output_stats, index)


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]


if __name__ == '__main__':

    strategy = BirdsStrategy('version_025', False)
    strategy.start()

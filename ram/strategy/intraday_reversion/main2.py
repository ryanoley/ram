import os
import sys
import pypyodbc
import itertools
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy
from ram.strategy.intraday_reversion.src.intraday_return_simulator import IntradayReturnSimulator
from ram.strategy.intraday_reversion.src.trade_signals_1 import *


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]


class IntradayReversion(Strategy):

    args1 = make_arg_iter({
        'n_estimators': [100],
        'min_samples_split': [75],
        'min_samples_leaf': [20]
    })

    args2 = make_arg_iter({
        'zLim': [.35, .45, .55],
    })

    args3 = make_arg_iter({
        'perc_take': [0.002, 0.004],
        'perc_stop': [0.002, 0.004]
    })

    def get_column_parameters(self):
        output_params = {}
        for col_ind, (x, y, z) in enumerate(
                itertools.product(self.args1, self.args2, self.args3)):
            params = dict(x)
            params.update(y)
            params.update(z)
            output_params[col_ind] = params
        return output_params

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def run_index(self, index):

        data = self.read_data_from_index(index)

        irs = IntradayReturnSimulator()

        import pdb; pdb.set_trace()

        for a1 in self.args1:

            predictions = get_predictions(data, **a1)
            predictions.to_csv('/Users/mitchellsuter/Desktop/predictions.csv')
            sys.exit()

            for a2 in self.args2:

                signals = get_trade_signals(predictions, **a2)

                for a3 in self.args3:

                    returns = irs.get_returns(signals, **a3)

        self.write_index_results(output_results, index)
        return

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_features(self):
        """
        Overriden method from Strategy
        """
        return [
            'AdjOpen', 'AdjClose', 'LAG1_AdjVolume', 'LAG1_AdjOpen',
            'LAG1_AdjHigh', 'LAG1_AdjLow', 'LAG1_AdjClose',
            'LAG2_AdjOpen', 'LAG2_AdjClose', 'LAG3_AdjOpen',
            'LAG3_AdjClose', 'LAG4_AdjOpen', 'LAG4_AdjClose',
            'LAG5_AdjOpen', 'LAG5_AdjClose', 'LAG6_AdjOpen',
            'LAG6_AdjClose', 'LAG7_AdjOpen', 'LAG7_AdjClose',
            'LAG8_AdjOpen', 'LAG8_AdjClose', 'LAG9_AdjOpen',
            'LAG9_AdjClose', 'LAG10_AdjOpen', 'LAG10_AdjClose',
            'LAG11_AdjOpen', 'LAG11_AdjClose',
            'LAG1_VOL90_AdjClose', 'LAG1_VOL10_AdjClose',
            'LAG1_PRMA10_AdjClose', 'LAG1_PRMA20_AdjClose',
            'LAG1_PRMA50_AdjClose', 'LAG1_PRMA200_AdjClose',
            'LAG1_RSI10', 'LAG1_RSI30',
            'LAG1_MFI10', 'LAG1_MFI30'
        ]

    def get_ids_filter_args(self):
        """
        Overriden method from Strategy
        """
        return {
            'ids': ['SPY', 'QQQ', 'IWM', 'VXX'],
            'start_date': '2002-04-24',
            'end_date': '2017-06-07'}

    def get_constructor_type(self):
        """
        Overriden method from Strategy
        """
        return 'etfs'


if __name__ == '__main__':

    from ram.strategy.base import make_argument_parser
    make_argument_parser(IntradayReversion)

import numpy as np
import pandas as pd
import os
import datetime
import itertools
from ram.strategy.intraday_reversion.src.intraday_return_simulator import IntradayReturnSimulator
from ram.strategy.intraday_reversion.src.trade_signals import *
from ram.strategy.base import Strategy

from ram.data.data_handler_sql import DataHandlerSQL

def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]

def make_ts_iter(variants):
    return [(x, y) for x, y in itertools.product(*variants.values()) if x > y]


class IntradayReversion(Strategy):

    args1 = make_arg_iter({
        'n_estimators': [100],
        'min_samples_split': [80],
        'min_samples_leaf': [20]
    })

    args2 = make_arg_iter({
        'zLim': [0.25, 0.35, 0.45],
        'gap_down_limit_1': [0.2, 0.35],
        'gap_down_limit_2': [0.2, 0.35],
        'gap_up_limit_1': [0.2, 0.35],
        'gap_up_limit_2': [0.2, 0.35],
    })

    args_take_stop = make_ts_iter({
        'takes' : [.005, .007, .01, .0125, .015],
        'stops' : [.002, .004, .007, .01, .0125]
    })

    def get_column_parameters(self):
        output_params = {}
        for col_ind, (x, y) in enumerate(
                itertools.product(self.args1, self.args2)):
            params = dict(x)
            params.update(y)
            output_params[col_ind] = params
        return output_params

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 
    def run_index(self, index):
        irs = IntradayReturnSimulator()
        data = self.read_data_from_index(index)
        data = format_raw_data(data)
        irs = IntradayReturnSimulator()
        self.preprocess_take_stop_reponses(data, irs)

        i = 0
        for a1 in self.args1:
            predictions = get_predictions(data, **a1)

            for a2 in self.args2:
                signals = get_trade_signals(predictions, **a2)
                signals = irs.get_optimal_take_stops2(signals)
                returns, stats = irs.get_returns(signals)
                self._capture_output(returns, stats, i)
                i += 1
                print i

        self.write_index_results(self.output_returns, index)
        return

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _capture_output(self, returns, arg_index):
        returns.name = arg_index
        if arg_index == 0:
            self.output_returns = pd.DataFrame(returns)
        else:
            self.output_returns = self.output_returns.join(returns, how='outer')
        self.output_stats[arg_index] = stats

    def preprocess_take_stop_reponses(self, data, irs):        
        tickers = data.Ticker.unique()

        print('\nPre Processing Returns: ')
        for ticker, take_stop in tqdm(itertools.product(tickers, self.args_take_stop)):
            irs._preprocess_returns(ticker,
                                    perc_take = take_stop[0],
                                    perc_stop = take_stop[1])
            irs.get_responses(ticker,
                              perc_take = take_stop[0],
                              perc_stop = take_stop[1])
        return

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def get_features(self):
        '''
        Overriden method from Strategy
        '''
        return ['AdjOpen', 'AdjClose', 'LAG1_AdjVolume','LAG1_AdjOpen',
                'LAG1_AdjHigh', 'LAG1_AdjLow', 'LAG1_AdjClose',
                'LAG2_AdjOpen', 'LAG2_AdjClose', 'LAG3_AdjOpen',
                'LAG3_AdjClose', 'LAG4_AdjOpen', 'LAG4_AdjClose',
                'LAG5_AdjOpen', 'LAG5_AdjClose', 'LAG6_AdjOpen',
                'LAG6_AdjClose', 'LAG7_AdjOpen', 'LAG7_AdjClose',
                'LAG8_AdjOpen', 'LAG8_AdjClose', 'LAG9_AdjOpen',
                'LAG9_AdjClose', 'LAG10_AdjOpen', 'LAG10_AdjClose',
                'LAG11_AdjOpen', 'LAG11_AdjClose',
                'LAG1_VOL90_AdjClose','LAG1_VOL10_AdjClose',
                'LAG1_PRMA10_AdjClose', 'LAG1_PRMA20_AdjClose',
                'LAG1_PRMA50_AdjClose', 'LAG1_PRMA200_AdjClose',
                'LAG1_RSI10', 'LAG1_RSI30', 
                'LAG1_MFI10', 'LAG1_MFI30']

    def get_ids_filter_args(self):
        '''
        Overriden method from Strategy
        '''
        return {
            'ids': ['SPY', 'QQQ', 'IWM', 'VXX', 'GLD', 'TLT', 'XLF', 'XLI',
                    'XLK', 'XLP', 'XLU', 'XLV', 'XOP'],
            'start_date': '2002-04-24',
            'end_date': '2017-06-07'}

    def get_constructor_type(self):
        '''
        Overriden method from Strategy
        '''
        return 'etfs'




def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--data', action='store_true',
        help='Run DataConstructor')
    parser.add_argument(
        '-w', '--write_simulation', action='store_true',
        help='Run simulatoin')
    parser.add_argument(
        '-s', '--simulation', action='store_true',
        help='Run simulatoin')
    parser.add_argument(
        '-l', '--live', action='store_true',
        help='Run simulatoin')
    args = parser.parse_args()

    if args.data:
        IntradayReversion().make_data()
    elif args.write_simulation:
        strategy = IntradayReversion('version_0001', True)
        strategy.start()
    elif args.simulation:
        strategy = IntradayReversion('version_0001', False)
        strategy.start()
    elif args.live:
        strategy = IntradayReversion('version_0001', False)
        trades = strategy.get_live_trades()
        trades.to_csv('C:/temp/IntradayTrades.csv', index=False)
        # Send to Trade Engine


if __name__ == '__main__':
    main()

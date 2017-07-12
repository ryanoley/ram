import os
import numpy as np
import pandas as pd
import datetime as dt
import itertools

from gearbox import read_csv, convert_date_array
from ram.strategy.base import Strategy



class VictorStrategy(Strategy):
    
    def get_column_parameters(self):
        return {}

    def run_index(self, index):
        nSmallCaps = 50
        nBigCaps = 15
        data = self.read_data_from_index(index)
        st_dt = data.Date.min()
        data['Ret'] = (data.AdjClose - data.LAG1_AdjClose) / data.LAG1_AdjClose
        
        smallCaps = data[data.Date==st_dt].sort_values(by='MarketCap').SecCode[:nSmallCaps].values
        bigCaps = data[data.Date==st_dt].sort_values(by='MarketCap').SecCode[-nBigCaps:].values
        
        smallAvg = data[data.SecCode.isin(smallCaps)].groupby('Date').Ret.mean()
        bigAvg = data[data.SecCode.isin(bigCaps)].groupby('Date').Ret.mean()
        out = pd.DataFrame(index = data.Date.unique())
        out['Ret'] = bigAvg
        
        self.write_index_results(out, index)

        return

    def create_features(self, df):
        return 

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def get_features(self):
        '''
        Overriden method from Strategy
        '''
        return ['AdjOpen', 'AdjVwap','LAG1_AdjClose', 'AdjClose', 'MarketCap',
                'AdjVolume','RVolume']

    def get_univ_filter_args(self):
        '''
        Overriden method from Strategy
        '''
        return {'filter': 'MarketCap',
                'where': 'MarketCap >= 1 ',
                'univ_size': 500}

    def get_univ_date_parameters(self):
        '''
        Overriden method from Strategy
        '''
        return {'frequency': 'Q',
                'train_period_length': 0,
                'test_period_length': 1,
                'start_year': 2000}


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]


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
    args = parser.parse_args()

    if args.data:
        VictorStrategy().make_data()
    elif args.write_simulation:
        strategy = VictorStrategy('version_0001', True)
        strategy.start()
    elif args.simulation:
        strategy = VictorStrategy('version_0001', False)
        strategy.start()



if __name__ == '__main__':
    main()
    
    
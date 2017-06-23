import os
import numpy as np
import pandas as pd
import datetime as dt
import itertools

from gearbox import read_csv, convert_date_array
from ram.strategy.base import Strategy



class OvernightQQQ(Strategy):
    
    def get_column_parameters(self):
        return {}

    def run_index(self, index):
        return

    def create_features(self, df):

        return 

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def get_features(self):
        '''
        Overriden method from Strategy
        '''
        return ['AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose', 'GSECTOR',
                'AdjVolume', 'RVolume', 'LAG1_AdjClose', 'MIN250_AdjClose',
                'MAX250_AdjClose', 'MIN125_AdjClose', 'MAX125_AdjClose',
                'VOL5_AdjClose', 'VOL20_AdjClose',
                'VOL50_AdjClose', 'VOL200_AdjClose',
                'PRMA5_AdjClose', 'PRMA20_AdjClose',
                'PRMA50_AdjClose', 'PRMA200_AdjClose']

    def get_filter_args(self):
        '''
        Overriden method from Strategy
        '''
        return {'filter': 'MarketCap',
                'where': 'MarketCap >= 1 and GSECTOR = 45',
                'univ_size': 150}

    def get_date_parameters(self):
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
        OvernightQQQ().make_data()
    elif args.write_simulation:
        strategy = OvernightQQQ('version_0001', True)
        strategy.start()
    elif args.simulation:
        strategy = OvernightQQQ('version_0001', False)
        strategy.start()



if __name__ == '__main__':
    main()
    
    
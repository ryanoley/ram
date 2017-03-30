import os
import numpy as np
import pandas as pd
import datetime as dt
import itertools

from gearbox import read_csv
from ram.strategy.base import Strategy
from ram.data.data_handler_sql import DataHandlerSQL

COST = 0.0015

class GapStrategy(Strategy):
    
    def get_column_parameters(self):
        z_args = make_arg_iter({'z': [.5, .75, 1., 1.25, 1.5]})
        output = {}
        for i, x in enumerate(list(z_args)):
            output[i] = x
        return output

    def run_index(self, index):
        
        data = self.read_data_from_index(index)
        data = self.create_features(data)

        iter_params = make_arg_iter({'z': [.5, .75, 1., 1.25, 1.5]})

        ind = 0
        output_results = pd.DataFrame()
        for params in iter_params:
            results = self.get_trades(data, **params)
            results.columns = [ind]
            output_results = output_results.join(results, how='outer')
            ind += 1

        self.write_index_results(output_results, index)

    def create_features(self, df):
        # Create Gap measure and Z Measure
        df['Ret'] = (df.AdjClose - df.AdjOpen) / df.AdjOpen

        df['GapDown'] = (df.AdjOpen - df.LAG1_AdjLow) / df.LAG1_AdjLow
        df['GapUp'] = (df.AdjOpen - df.LAG1_AdjHigh) / df.LAG1_AdjHigh

        df['zUp'] = df.GapUp / df.LAG1_VOL90_AdjClose
        df['zDown'] = df.GapDown / df.LAG1_VOL90_AdjClose
        
        df['MktRet'] = (df.MKT_AdjClose - df.MKT_AdjOpen) / df.MKT_AdjOpen
        df['RetH'] = df.Ret - df.MktRet

        return df.reset_index(drop=True)

    def get_trades(self, data, z):
        dts = data.Date.unique()
        #  Filter using Z and Momentum
        data = data[data.EARNINGSFLAG == 0]
        gapUpBelowMA = (data.zUp >= z) & (data.AdjOpen <= data.LAG1_MA20_AdjClose)
        gapDwnAbvMA = (data.zDown <= -z) & (data.AdjOpen >= data.LAG1_MA20_AdjClose)
        data = data.loc[gapUpBelowMA | gapDwnAbvMA]
        data.sort_values('Date', inplace=True)
        data.reset_index(drop=True, inplace=True)

        # Trades and costs 
        shorts = data.loc[(data.zUp > 0) & (data.GapUp > 0)].copy()
        longs = data.loc[(data.zDown < 0) & (data.GapDown < 0)].copy()
        shorts.RetH += COST
        longs.RetH -= COST

        # Sum all trades
        longReturns = longs.groupby('Date').RetH.mean()
        longCounts = longs.groupby('Date').RetH.count()
        shortReturns = shorts.groupby('Date').RetH.mean()
        shortCounts = shorts.groupby('Date').RetH.count()
        
        # Output df
        output = pd.DataFrame(0, columns=['LongRet', 'nLong', 'ShortRet',
                                          'nShort'], index = dts)
        output.loc[longReturns.index, 'LongRet'] = longReturns
        output.loc[longCounts.index, 'nLong'] = longCounts
        output.loc[shortReturns.index, 'ShortRet'] = shortReturns
        output.loc[shortCounts.index, 'nShort'] = shortCounts
        output['TotRet'] = output.LongRet - output.ShortRet

        return pd.DataFrame(output['TotRet'])

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def get_features(self):
        '''
        Overriden method from Strategy
        '''
        return ['AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose', 'LAG1_AdjHigh',
                'LAG1_AdjLow', 'LAG1_VOL90_AdjClose', 'LAG1_MA20_AdjClose',
                'EARNINGSFLAG', 'MKT_AdjClose', 'MKT_AdjOpen']

    def get_filter_args(self):
        '''
        Overriden method from Strategy
        '''
        return {'filter': 'AvgDolVol',
                'where': 'MarketCap >= 100 ' +
                'and Close_ between 15 and 1000',
                'univ_size': 800}

    def get_date_parameters(self):
        '''
        Overriden method from Strategy
        '''
        return {'frequency': 'Q',
                'train_period_length': 0,
                'start_year': 2003}


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
    
    import ipdb; ipdb.set_trace()
    if args.data:
        GapStrategy().make_data()
    elif args.write_simulation:
        strategy = GapStrategy('version_0001', True)
        strategy.start()
    elif args.simulation:
        strategy = GapStrategy('version_0001', False)
        strategy.start()



if __name__ == '__main__':
    main()
    
    
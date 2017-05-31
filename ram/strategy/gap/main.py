import os
import numpy as np
import pandas as pd
import datetime as dt
import itertools

from gearbox import read_csv, convert_date_array
from ram.strategy.base import Strategy

COST = 0.001
AUM = 98e6

class GapStrategy(Strategy):
    
    def get_column_parameters(self):
        sigArgs = make_arg_iter({'z1': [1., 1.5, 2.],
                                    'z2': [1., 1.5, 2.],
                                    'z3': [.35, .5, .75, 1.] ,
                                    'v1': [.5, 1, 1.5],
                                    'v2': [.5, .75, 1.]})
        output = {}
        for i, x in enumerate(sigArgs):
            output[i] = x
        return output

    def run_index(self, index):
        max_gross = 10e6
        base_pos = .003
        sigArgs = make_arg_iter({'z1': [1.25, 1.75, 2.25],
                                    'z2': [1., 1.5, 2.],
                                    'z3': [.5, .75, 1.] ,
                                    'v1': [.5, 1, 1.5, 2.],
                                    'v2': [.5, .75, 1.]})

        data = self.read_data_from_index(index)
        data = self.create_features(data)

        ind = 0
        output_results = pd.DataFrame()
        output_stats = {}

        for args in sigArgs:
            data = self.get_signals(data, **args)
            results, stats = self.calc_returns(data, max_gross, base_pos)
            results.columns = [ind]
            output_results = output_results.join(results, how='outer')
            output_stats[ind] = stats
            ind += 1

        self.write_index_results(output_results, index)
        self.write_index_stats(output_stats, index)

    def create_features(self, df):
        # Filter earnings announcements
        secChange = df.SecCode != df.SecCode.shift(1)
        ernRoll = df.EARNINGSFLAG.rolling(window=2,center=False).sum()
        ernRoll[secChange] = 0.
        df = df[ernRoll == 0.].copy()

        # Create Gap measure and Z Measure
        df['GapDown'] = (df.AdjOpen - df.LAG1_AdjLow) / df.LAG1_AdjLow
        df['GapUp'] = (df.AdjOpen - df.LAG1_AdjHigh) / df.LAG1_AdjHigh
        df['zUp90'] = df.GapUp / df.LAG1_VOL90_AdjClose
        df['zDown90'] = df.GapDown / df.LAG1_VOL90_AdjClose
        df['zUp20'] = df.GapUp / df.LAG1_VOL20_AdjClose
        df['zDown20'] = df.GapDown / df.LAG1_VOL20_AdjClose
        df['zUp5'] = df.GapUp / df.LAG1_VOL5_AdjClose
        df['zDown5'] = df.GapDown / df.LAG1_VOL5_AdjClose

        # Returns
        df['Ret'] = (df.AdjClose - df.AdjOpen) / df.AdjOpen
        df['MktRet'] = (df.MKT_AdjClose - df.MKT_AdjOpen) / df.MKT_AdjOpen
        df['RetH'] = df.Ret - df.MktRet

        # Volatility measures
        df['Vol5_90'] = df.LAG1_VOL5_AdjClose / df.LAG1_VOL90_AdjClose
        df['Vol10_90'] = df.LAG1_VOL10_AdjClose / df.LAG1_VOL90_AdjClose

        return df.reset_index(drop=True)

    def get_signals(self, data, z1=1., z2=.1, z3=1., v1=1., v2=1.):
        # Get long/short signals
        # short window
        gapUpAbvMA1 = ((data.zUp5 >= z1) &
                        (data.AdjOpen >= data.LAG1_MA5_AdjClose))
        gapDwnBlwMA1 = ((data.zDown5 <= -z1) &
                        (data.AdjOpen <= data.LAG1_MA5_AdjClose))

        # medium window
        gapUpAbvwMA2 = ((data.zUp20 >= z2) &
                        (data.AdjOpen >= data.LAG1_MA10_AdjClose))
        gapDwnBlwMA2 = ((data.zDown20 <= -z2) &
                        (data.AdjOpen <= data.LAG1_MA10_AdjClose))

        # long window
        gapUpBelowMA3 = ((data.zUp90 >= z3) &
                        (data.AdjOpen <= data.LAG1_MA20_AdjClose))
        gapDwnAbvMA3 = ((data.zDown90 <= -z3) &
                        (data.AdjOpen >= data.LAG1_MA20_AdjClose))
        
        # Volatility filters
        volSmallWindow = data.Vol5_90 > v1
        volMidWindow = data.Vol10_90 > v2

        data['Short'] = np.where((gapUpAbvMA1 & volSmallWindow) |
                                    (gapUpAbvwMA2 & volMidWindow) |
                                    (gapUpBelowMA3), 1, 0)

        data['Long'] = np.where((gapDwnBlwMA1 & volSmallWindow) |
                                    (gapDwnBlwMA2 & volMidWindow) |
                                    (gapDwnAbvMA3), 1, 0)

        return data

    def calc_returns(self, data, max_gross, base_pos):
        dts = data.Date.unique()
        dts.sort()
        longs = data[data.Long == 1].copy()
        shorts = data[data.Short == 1].copy()
        shorts.RetH += COST
        longs.RetH -= COST

        # Number of trades and mean Returns
        longCounts = data.groupby('Date').Long.sum()
        shortCounts = data.groupby('Date').Short.sum()
        longReturns = longs.groupby('Date').RetH.mean()
        shortReturns = shorts.groupby('Date').RetH.mean()

        # Determine position size per day
        n_pos = longCounts + shortCounts
        base_pos = base_pos * AUM
        pos_size = np.where((max_gross/n_pos) < base_pos, (max_gross/n_pos),
            base_pos)

        # Output df
        output = pd.DataFrame(data={'nLong': longCounts,
                                    'nShort': shortCounts,
                                    'longRet': longReturns,
                                    'shortRet': shortReturns},
                              index = dts)
        output.longRet.fillna(0., inplace=True)
        output.shortRet.fillna(0., inplace=True)
        output['longAlloc'] = output.nLong * pos_size
        output['shortAlloc'] = output.nShort * -pos_size
        output['longPL'] = output.longAlloc * output.longRet
        output['shortPL'] = output.shortAlloc * output.shortRet
        output['totalPL'] = output.longPL + output.shortPL

        stats = {'nLong':longCounts.sum().astype(float),
                 'nShort':shortCounts.sum().astype(float)}

        return pd.DataFrame(output['totalPL'] / AUM), stats

    # ~~~~~~ DataConstructor params ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def get_features(self):
        '''
        Overriden method from Strategy
        '''
        return ['AdjOpen', 'AdjClose', 'LAG1_AdjHigh', 'LAG1_AdjLow',
                'LAG1_AdjClose', 'AvgDolVol', 'EARNINGSFLAG', 'GSECTOR',
                'LAG1_VOL90_AdjClose', 'LAG1_VOL20_AdjClose',
                'LAG1_VOL5_AdjClose', 'LAG1_VOL10_AdjClose', 
                'LAG1_MA5_AdjClose', 'LAG1_MA10_AdjClose',
                'LAG1_MA20_AdjClose',
                'LAG1_MKT_AdjHigh', 'LAG1_MKT_AdjLow',
                'LAG1_MKT_AdjClose', 'MKT_AdjClose', 'MKT_AdjOpen', ]

    def get_filter_args(self):
        '''
        Overriden method from Strategy
        '''
        return {'filter': 'MarketCap',
                'where': 'MarketCap >= 100 ' +
                'and Close_ between 15 and 1000',
                'univ_size': 800}

    def get_date_parameters(self):
        '''
        Overriden method from Strategy
        '''
        return {'frequency': 'Q',
                'train_period_length': 0,
                'test_period_length': 1,
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

    if args.data:
        GapStrategy().make_data()
    elif args.write_simulation:
        strategy = GapStrategy('version_0005', True)
        strategy.start()
    elif args.simulation:
        strategy = GapStrategy('version_0005', False)
        strategy.start()



if __name__ == '__main__':
    main()
    
    
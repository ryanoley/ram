import os
import numpy as np
import pandas as pd
import datetime as dt
import itertools

from gearbox import read_csv, convert_date_array
from ram.strategy.base import Strategy


COST = 0.0015
AUM = 98e6


class GapStrategy(Strategy):
    
    def get_column_parameters(self):
        z_args = make_arg_iter({'z': [.5, .75, 1., 1.25, 1.5]})
        mktZArgs = make_arg_iter({'mktZ': [.5, .75, 1., 1.25, 1.5, 5.]})
        output = {}
        for i, (x, y) in enumerate(list(itertools.product(z_args, mktZArgs))):
            z = {}
            z.update(x)
            z.update(y)
            output[i] = z
        return output
    
    def run_index(self, index):
        max_gross = 10e6
        base_pos = .005

        args1 = make_arg_iter({'z': [.5, .75, 1., 1.25, 1.5]})
        mktZ = [.5, .75, 1., 1.25, 1.5, 5.]

        data = self.read_data_from_index(index)
        data = self.create_features(data)
        mktData = pd.read_csv("C:/temp/market_data.csv")
        mktData.Date = convert_date_array(mktData.Date)

        ind = 0
        output_results = pd.DataFrame()
        output_stats = {}

        for arg in args1:
            for mZ in mktZ:
                filter_dts = mktData.loc[np.abs(mktData.volZ) >= mZ, 'Date'].values
                data = self.get_signals(data, filter_dts, **arg)
                results, stats = self.calc_returns(data, max_gross, base_pos)
                results.columns = [ind]
                output_results = output_results.join(results, how='outer')
                output_stats[ind] = stats
                ind += 1

        self.write_index_results(output_results, index)
        self.write_index_stats(output_stats, index)

    def create_features(self, df):
        # Filter earnings announcements
        secChange = data.SecCode != data.SecCode.shift(1)
        ernRoll = df.EARNINGSFLAG.rolling(window=2,center=False).sum()
        ernRoll[secChange] = 0.
        df = df[ernRoll == 0.].copy()
        
        # Create Gap measure and Z Measure
        df['GapDown'] = (df.AdjOpen - df.LAG1_AdjLow) / df.LAG1_AdjLow
        df['GapUp'] = (df.AdjOpen - df.LAG1_AdjHigh) / df.LAG1_AdjHigh
        df['zUp'] = df.GapUp / df.LAG1_VOL90_AdjClose
        df['zDown'] = df.GapDown / df.LAG1_VOL90_AdjClose
        
        # Returns
        df['Ret'] = (df.AdjClose - df.AdjOpen) / df.AdjOpen
        df['MktRet'] = (df.MKT_AdjClose - df.MKT_AdjOpen) / df.MKT_AdjOpen
        df['RetH'] = df.Ret - df.MktRet

        return df.reset_index(drop=True)

    def get_signals(self, data, filter_dts, z):
        # Widen bands
        upper = data.LAG1_MA20_AdjClose
        lower = data.LAG1_MA20_AdjClose

        # Get long/short signals
        gapUpBelowMA = (data.zUp >= z) & (data.AdjOpen <= upper)
        gapDwnAbvMA = (data.zDown <= -z) & (data.AdjOpen >= lower)
        data['Short'] = np.where(gapUpBelowMA, 1, 0)
        data['Long'] = np.where(gapDwnAbvMA, 1, 0)
        
        # Filter out market dts
        data.loc[data.Date.isin(filter_dts), 'Short'] = 0
        data.loc[data.Date.isin(filter_dts), 'Long'] = 0

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
                'LAG1_VOL90_AdjClose', 'LAG1_MA20_AdjClose', 'AvgDolVol',
                'EARNINGSFLAG', 'MKT_AdjClose', 'MKT_AdjOpen',
                'PRMAH5_AdjClose', 'PRMAH10_AdjClose', 'PRMAH20_AdjClose'
                ]

    def get_filter_args(self):
        '''
        Overriden method from Strategy
        '''
        return {'filter': 'AvgDolVol',
                'where': 'MarketCap >= 100 ' +
                'and Close_ between 15 and 1000',
                'univ_size': 500}

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

    if args.data:
        GapStrategy().make_data()
    elif args.write_simulation:
        strategy = GapStrategy('version_0002', True)
        strategy.start()
    elif args.simulation:
        strategy = GapStrategy('version_0002', False)
        strategy.start()



if __name__ == '__main__':
    main()
    
    
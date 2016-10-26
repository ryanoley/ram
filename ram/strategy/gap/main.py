import numpy as np
import pandas as pd
import datetime as dt

from gearbox import read_csv

from ram.data.dh_file import DataHandlerFile
from ram.strategy.base import Strategy

from ram.utils.report import create_strategy_report

COST = .0015


class GapStrategy(Strategy):

    def __init__(self, dpath):
        df = read_csv(dpath)
        df = df[['ID', 'Ticker', 'Date','Open', 'High', 'PrevHigh', 'Low',
                 'PrevLow', 'Close', 'AdjClose', 'PrevAdjClose', 'AvgDolVol', 
                 'AdjCloseMA20', 'StdevRet']]
        df.loc[:,'Date'] = df.Date.apply(lambda x: x[:10])

        self.data = DataHandlerFile(df)

    def get_results(self):
        return self.results


    def start(self, z=1., top_n=10):
        # Get all data
        df = self._create_features(z)
        dts = np.unique(df.Date)
        results = pd.DataFrame(columns=['R1', 'R2', 'R3'],
                               index=dts)

        # Iterate through dates
        for t in dts:
            dtdf = df.loc[df.Date==t]
            updf = dtdf.loc[(dtdf.zUp > 0) & (dtdf.GapUp > 0)].copy()
            dwndf = dtdf.loc[(dtdf.zDown < 0) & (dtdf.GapDown < 0)].copy()

            if len(updf)>0:
                updf.sort_values('zUp', inplace=True)
                updf.reset_index(drop=True, inplace=True)
                upRet = updf.Ret[-top_n:].mean() + COST
            else:
                upRet = 0.

            if len(dwndf)>0:
                dwndf.sort_values('zDown', inplace=True)
                dwndf.reset_index(drop=True, inplace=True)
                dwnRet = dwndf.Ret[:top_n].mean() - COST
            else:
                dwnRet = 0.

            results.loc[t, 'R1'] = upRet * -1
            results.loc[t, 'R2'] = dwnRet
            results.loc[t, 'R3'] = dwnRet - upRet

        self.results = results.dropna()


    def start_live(self): 
        return

    ###########################################################################

    def _create_features(self, z = 1.):
        # Get all of data history
        prices = self.data.get_filtered_univ_data(
            univ_size=1000,
            features  = ['Ticker', 'Open', 'High', 'PrevHigh', 'Low',
                         'PrevLow', 'Close', 'AdjClose', 'AdjCloseMA20',
                         'AvgDolVol', 'StdevRet'],
            start_date = '2013-01-02',
            filter_date = '2013-01-02',
            end_date = dt.datetime.today(),
            filter_column = 'AvgDolVol')
        # AdjCloseMA20 and StdevRet are offset in sql file
        prices['Ret'] = (prices.Close - prices.Open) / prices.Open
        prices['GapDown'] = (prices.Open - prices.PrevLow) / prices.PrevLow
        prices['GapUp'] = (prices.Open - prices.PrevHigh) / prices.PrevHigh
        prices['zUp'] = prices.GapUp / prices.StdevRet
        prices['zDown'] = prices.GapDown / prices.StdevRet
        #  Filter using gap measure and momentum measure
        prices = prices.loc[
            ((prices.Open <= prices.AdjCloseMA20) & (prices.zUp >= z)) |
            ((prices.Open >= prices.AdjCloseMA20) & (prices.zDown <= -z))]
        prices.sort_values('Date', inplace=True)

        return prices.reset_index(drop=True)


if __name__ == '__main__':
    dpath = 'C:/temp/basedata.csv'
    strategy = GapStrategy(dpath)
    strategy.start(z=1., top_n=10)

    path = 'C:/temp/gap'
    name = 'GapStrategy'
    create_strategy_report(strategy.get_results(), name, path)

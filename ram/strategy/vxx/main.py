import os
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import read_csv

from ram.data.base import DataHandler
from ram.strategy.base import Strategy

from sklearn.ensemble import RandomForestClassifier


class VXXStrategy(Strategy):

    def __init__(self):
        pass

    def run_iteration(self, t):

        # DATA Interface specific to Strategy
        train_data = self.data.get_data(t)

        # Model is specific to 
        model = RandomForestClassifier()
        # create features
        X, y = self._create_features(train_data)
        model.fit(X=X.iloc[:-1], y=y.iloc[:-1, 2])

        zz = model.predict(X.iloc[-1:])
        ret = np.where(zz[0], y.iloc[-1, 0], -y.iloc[-1, 0])
        self.result = ret

    def get_result(self):
        return self.result

    def _create_features(self, prices):
        # Split Adjust
        prices = prices.copy()
        prices['Open'] = prices.Open * prices.AdjFactor
        prices['High'] = prices.High * prices.AdjFactor
        prices['Low'] = prices.Low * prices.AdjFactor
        prices['Close'] = prices.Close * prices.AdjFactor

        X = pd.DataFrame(index=prices.index)

        X['V1'] = prices['Close'].pct_change(1)
        X['V2'] = prices['Close'].pct_change(2)
        X['V3'] = prices['Close'].pct_change(3)
        X['V4'] = prices['Close'] / prices['Open'] - 1
        X['V5'] = (prices['High'] - prices['Low']) / prices['Close'] - 1
        X['V6'] = prices['Close'] / prices['Low'] - 1

        X['Ret1'] = X.V1.shift(-1)
        X['Ret2'] = X.V4.shift(-1)

        # Remove nans from variable creation
        X = X.iloc[3:-1]

        y = X.pop('Ret1')
        y = pd.DataFrame(y)
        y['Ret2'] = X.pop('Ret2')
        y['signal1'] = y.Ret1 > 0
        y['signal2'] = y.Ret2 > 0

        return X, y


if __name__ == '__main__':

    dpath = '/Users/mitchellsuter/Desktop/vxx_output.csv'

    import pdb; pdb.set_trace()
    df = read_csv(dpath)

    # Weird fix
    df.columns = df.columns
    df.columns = ['ID', 'Date',
                  'Open', 'High', 'Low', 'Close', 'VWAP', 'Volume',
                  'AdjFactor', 'AvgDolVol', 'MarketCap']

    dh = DataHandler(df)

    strategy = VXXStrategy()
    strategy.attach_data_source(dh)
    strategy.start()

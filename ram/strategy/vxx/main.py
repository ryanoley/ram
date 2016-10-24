import numpy as np
import pandas as pd
import datetime as dt

from gearbox import read_csv

from ram.data.dh_file import DataHandlerFile
from ram.strategy.base import Strategy

from sklearn.linear_model import LogisticRegression


class VXXStrategy(Strategy):

    def __init__(self):

        # TEMP!!!
        dpath = '/Users/mitchellsuter/Desktop/vxx_output.csv'
        df = read_csv(dpath)
        # Weird fix
        df.columns = df.columns
        df.columns = ['ID', 'Date',
                      'Open', 'High', 'Low', 'Close', 'VWAP', 'Volume',
                      'AdjFactor', 'AvgDolVol', 'MarketCap']
        df.Date = df.Date.apply(lambda x: x[:-5])
        # TEMP!!!
        self.data = DataHandlerFile(df)

    def get_results(self):
        return self.results

    def start(self):
        # Get all data
        X, y = self._create_features()

        model = LogisticRegression()

        results = pd.DataFrame(columns=['R1', 'R2'],
                               index=X.index)

        # Iterate through dates
        for t in np.unique(X.index)[30:]:
            # TRAIN - datetime indexing is inclusive thus trim by 1
            X_train, y_train = X.loc[:t].iloc[:-1], y.loc[:t].iloc[:-1]
            # TEST
            X_test, y_test = X.loc[t:t, :], y.loc[t:t, :]

            model.fit(X=X_train, y=y_train.loc[:, 'signal1'])
            pred1 = model.predict(X_test)[0]

            model.fit(X=X_train, y=y_train.loc[:, 'signal2'])
            pred2 = model.predict(X_test)[0]

            results.loc[t, 'R1'] = np.where(pred1, 1, -1) * y_test['Ret1'][0]
            results.loc[t, 'R2'] = np.where(pred1, 1, -1) * y_test['Ret2'][0]

        self.results = results.dropna()

    def start_live(self):
        # Get all data
        X, y = self._create_features()

        model = LogisticRegression()

        results = pd.DataFrame(columns=['R1', 'R2'],
                               index=X.index)
        # Iterate through dates
        t = np.unique(X.index)[-1]
        # TRAIN - datetime indexing is inclusive thus trim by 1
        X_train, y_train = X.loc[:t].iloc[:-1], y.loc[:t].iloc[:-1]
        # TEST
        X_test, y_test = X.loc[t:t, :], y.loc[t:t, :]

        model.fit(X=X_train, y=y_train.loc[:, 'signal1'])
        pred1 = model.predict(X_test)[0]

        model.fit(X=X_train, y=y_train.loc[:, 'signal2'])
        pred2 = model.predict(X_test)[0]

        results.loc[t, 'R1'] = np.where(pred1, 1, -1) * y_test['Ret1'][0]
        results.loc[t, 'R2'] = np.where(pred1, 1, -1) * y_test['Ret2'][0]

        self.results = results.iloc[-1:]

    ###########################################################################

    def _create_features(self):

        # Pull entire VXX history
        prices = self.data.get_id_data(
            ids=140062,
            features=['Open', 'High', 'Low', 'Close', 'AdjFactor'],
            start_date='1993-01-01',
            end_date=dt.datetime.utcnow())

        # Split Adjust
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

        # Add dates to indexes
        X.index = prices.Date.shift(-1)

        # Remove nans from variable creation
        X = X.iloc[3:-1]

        y = X.pop('Ret1')
        y = pd.DataFrame(y)
        y['Ret2'] = X.pop('Ret2')
        y['signal1'] = y.Ret1 > 0
        y['signal2'] = y.Ret2 > 0

        return X, y


if __name__ == '__main__':

    strategy = VXXStrategy()
    strategy.start()
    #strategy.start_live()
    print strategy.get_results()

import os
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import read_csv

from ram.data.dh_sql import DataHandlerSQL
from ram.strategy.base import Strategy
from ram.utils.statistics import create_strategy_report

from sklearn.linear_model import LogisticRegression


class VXXStrategy(Strategy):

    def get_iter_index(self):
        return [0]

    def run_index(self, index):
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

            # Long and short
            r1 = y_test['Ret1'][0]
            r2 = y_test['Ret2'][0]
            results.loc[t, 'R1'] = np.where(pred1, r1, -r1)
            results.loc[t, 'R2'] = np.where(pred2, r1, -r1) 
            # Long only
            results.loc[t, 'R3'] = np.where(pred1, r1, np.nan)
            results.loc[t, 'R4'] = np.where(pred1, r1, np.nan)

        deliverable = {'returns': results.dropna(),
                       'column_params': {},
                       'statistics': {}}

        return deliverable

    ###########################################################################

    def _create_features(self):
        # Pull entire VXX history
        start_date = '2009-01-30'
        end_date = '2020-01-01'

        prices = self.datahandler.get_etf_data(
            tickers='VXX',
            features=['AdjOpen', 'AdjHigh', 'AdjLow', 'AdjClose'],
            start_date=start_date,
            end_date=end_date)

        prices.columns = ['SecCode', 'Date', 'Open', 'High', 'Low', 'Close']

        X = pd.DataFrame(index=prices.index)

        X['V1'] = prices['Close'].pct_change(1)
        X['V2'] = prices['Close'].pct_change(2)
        X['V3'] = prices['Close'].pct_change(3)
        X['V4'] = prices['Close'] / prices['Open'] - 1
        X['V5'] = (prices['High'] - prices['Low']) / \
            prices['Close'] - 1
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

    import pdb; pdb.set_trace()
    strategy = VXXStrategy()
    strategy.start()

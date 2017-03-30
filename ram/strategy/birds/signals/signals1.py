import numpy as np
import pandas as pd


class Signals1(object):

    def generate_portfolio_signals(self, data, quantile, feature):
        feature, side = feature
        data = data[['SecCode', 'Date', feature]].copy()
        data = data.sort_values(['SecCode', 'Date'])
        # Get tops and bottoms
        tops = data[['Date', feature]].groupby('Date').quantile(
            1-quantile).reset_index()
        bottoms = data[['Date', feature]].groupby('Date').quantile(
            quantile).reset_index()
        # Merge and rename
        data = data.merge(tops, on='Date', suffixes=('', '_top'))
        data = data.merge(bottoms, on='Date', suffixes=('', '_bottom'))
        data.columns = ['SecCode', 'Date', 'feature',
                        'feature_top', 'feature_bottom']
        # Create column for output
        data.loc[:, feature] = np.where(
            data.feature >= data.feature_top, side,
            np.where(data.feature <= data.feature_bottom, -side, 0))
        output = data[['SecCode', 'Date', feature]].sort_values(
            ['SecCode', 'Date']).reset_index(drop=True)
        return output

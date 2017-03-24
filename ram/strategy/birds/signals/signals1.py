import numpy as np
import pandas as pd


class Signals1(object):

    def __init__(self, quantile=0.25):
        self._quantile = quantile

    def register_index_variables(self, features):
        self._features = features

    def generate_portfolio_signals(self, data):
        assert hasattr(self, '_features')
        # Get tops and bottoms for variables
        features = ['Date'] + self._features
        bottoms = data[features].groupby('Date').quantile(
            self._quantile).reset_index()
        tops = data[features].groupby('Date').quantile(
            1-self._quantile).reset_index()
        # Create output object
        features = ['SecCode', 'Date'] + self._features
        output = data[features].copy().sort_values(['SecCode', 'Date'])
        output.iloc[:, 2:] = 0.0
        for feature in self._features:
            temp = data[['SecCode', 'Date', feature]].merge(
                tops[['Date', feature]], on='Date',
                suffixes=('', '_top')).merge(
                    bottoms[['Date', feature]],
                    on='Date', suffixes=('', '_bottom'))
            temp = temp.sort_values(['SecCode', 'Date'])
            temp.columns = ['SecCode', 'Date', 'feature',
                            'feature_top', 'feature_bottom']
            output.loc[:, feature] = np.where(
                temp.feature >= temp.feature_top, 1,
                np.where(temp.feature <= temp.feature_bottom, -1, 0))
        return output

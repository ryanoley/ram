import numba
import numpy as np
import pandas as pd
import itertools as it

from sklearn.cluster import KMeans
from sklearn.preprocessing import Imputer


class PairSelector(object):

    def rank_pairs(self, data, z_window=20):
        # Reshape Close data
        close_data = data.pivot(index='Date',
                                columns='SecCode',
                                values='AdjClose')
        cut_date = data.Date[~data.TestFlag].max()
        train_close = close_data.loc[close_data.index <= cut_date]
        train_close = train_close.T.dropna().T
        pair_info = self._filter_pairs(train_close)
        pair_info = self._get_accounting_groups(pair_info, data, cut_date)
        spreads, zscores = self._get_spreads_zscores(
            pair_info, close_data, z_window)
        return pair_info, spreads, zscores

    def _filter_pairs(self, close_data, n_pairs=None):
        pairs = self._prep_output(close_data)
        pairs['distances'] = self._flatten(self._get_distances(close_data))
        pairs = pairs.sort_values('distances').reset_index(drop=True)
        return pairs

    # ~~~~~~ Z-Scores ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_spreads_zscores(self, pair_info, close_data, window=20):
        spreads = self._get_spread_index(pair_info, close_data)
        ma = spreads.rolling(window=window).mean()
        std = spreads.rolling(window=window).std()
        return spreads, (spreads - ma) / std

    @staticmethod
    def _get_spread_index(pair_info, close_data):
        # Create two data frames that represent Leg1 and Leg2
        close1 = close_data.loc[:, pair_info.Leg1]
        close2 = close_data.loc[:, pair_info.Leg2]
        spreads = np.subtract(np.log(close1), np.log(close2))
        # Add correct column names
        spreads.columns = ['{0}~{1}'.format(x, y) for x, y in
                           zip(pair_info.Leg1, pair_info.Leg2)]
        return spreads

    # ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @staticmethod
    def _flatten(data):
        # Capture going first over columns, then down rows
        index = it.combinations(range(data.shape[1]), 2)
        return [data[z] for z in index]

    @staticmethod
    def _get_distances(close_data):
        p_index = np.array(close_data / close_data.iloc[0])
        return np.apply_along_axis(get_abs_distance, 0, p_index, p_index)

    @staticmethod
    def _prep_output(close_data):
        legs = zip(*it.combinations(close_data.columns.values, 2))
        stat_df = pd.DataFrame({'Leg1': legs[0]})
        stat_df['Leg2'] = legs[1]
        return stat_df

    # ~~~~~~ Accounting Groups ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_accounting_groups(self, pair_info, data, cut_date):
        accounting_features = [
            'NETINCOMEQ', 'NETINCOMETTM',
            'SALESQ', 'SALESTTM', 'ASSETS',
            'CASHEV', 'FCFMARKETCAP',
            'NETINCOMEGROWTHQ',
            'NETINCOMEGROWTHTTM',
            'OPERATINGINCOMEGROWTHQ',
            'OPERATINGINCOMEGROWTHTTM',
            'EBITGROWTHQ',
            'EBITGROWTHTTM',
            'SALESGROWTHQ',
            'SALESGROWTHTTM',
            'FREECASHFLOWGROWTHQ',
            'FREECASHFLOWGROWTHTTM',
            'GROSSPROFASSET',
            'GROSSMARGINTTM',
            'EBITDAMARGIN',
            'PE'
        ]
        # On last train day
        filter_inds = data.Date == data.Date[~data.TestFlag].max()
        data2 = data.loc[filter_inds].copy()
        imp = Imputer(strategy='median', axis=0)
        data2.loc[:, accounting_features] = imp.fit_transform(
            data2[accounting_features].values)
        km = KMeans(n_clusters=4)
        data2['group'] = km.fit_predict(data2[accounting_features])
        data2 = data2[['SecCode', 'group']]
        data3 = data2.copy()
        data2.columns = ['Leg1', 'group1']
        data3.columns = ['Leg2', 'group2']
        pair_info = pair_info.merge(data2).merge(data3)
        pair_info['same_accounting'] = pair_info.group1 == pair_info.group2
        return pair_info.drop(['group1', 'group2'], axis=1).sort_values('distances').reset_index(drop=True)


# ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_abs_distance(x_index, indexes):
    return np.sum(np.abs(x_index[:, None] - indexes), axis=0)

import numba
import numpy as np
import pandas as pd
import itertools as it


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
        spreads, zscores = self._get_spreads_zscores(
            pair_info, close_data, z_window)
        return pair_info, spreads, zscores

    def _filter_pairs(self, close_data):
        pairs = self._prep_output(close_data)
        pairs['distances'] = self._flatten(self._get_distances(close_data))
        pairs = pairs.sort_values('distances').reset_index(drop=True)
        return pairs

    # ~~~~~~ Z-Scores ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_spreads_zscores(self, pair_info, close_data, window=20):
        spreads = self._get_spread_index(pair_info, close_data)
        zscores = self._get_zscores(spreads, window)
        return spreads, zscores

    @staticmethod
    def _get_zscores(spreads, window):
        ma = spreads.rolling(window=window).mean()
        std = spreads.rolling(window=window).std()
        return (spreads - ma) / std

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


# ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_abs_distance(x_index, indexes):
    return np.sum(np.abs(x_index[:, None] - indexes), axis=0)

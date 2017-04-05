import numba
import numpy as np
import pandas as pd
import itertools as it


class PairSelector2(object):

    def get_iterable_args(self):
        return {'pair_test_flag': [True]}

    def rank_pairs(self, data, pair_test_flag):
        # Reshape Close data
        close_data = data.pivot(index='Date',
                                columns='SecCode',
                                values='AdjClose')
        cut_date = data.Date[~data.TestFlag].max()
        train_close = close_data.loc[close_data.index <= cut_date]
        train_close = train_close.T.dropna().T
        self.pair_info = self._filter_pairs(train_close)
        self.close_data = close_data.fillna(method='pad')

    def get_responses(self, z_window=20, enter_z=2, exit_z=1):
        zscores = self.get_zscores(z_window).fillna(0).values
        close1 = self.close_data[self.pair_info.Leg1].values
        close2 = self.close_data[self.pair_info.Leg2].values
        return pd.DataFrame(
            get_trade_signal_series(zscores, close1, close2, enter_z, exit_z),
            index=self.close_data.index,
            columns = ['{}~{}'.format(x, y) for x, y in
                       self.pair_info[['Leg1', 'Leg2']].values])

    def _filter_pairs(self, close_data, n_pairs=15000):
        pairs = self._prep_output(close_data)
        pairs['distances'] = self._flatten(self._get_distances(close_data))
        pairs = pairs.sort_values('distances').reset_index(drop=True)
        return pairs.iloc[:n_pairs]

    # ~~~~~~ Z-Scores ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_zscores(self, z_window=20):
        # Create two data frames that represent Leg1 and Leg2
        closes_leg1 = self.close_data.loc[:, self.pair_info.Leg1]
        closes_leg2 = self.close_data.loc[:, self.pair_info.Leg2]
        zscores = self._get_spread_zscores(closes_leg1, closes_leg2, z_window)
        # Add correct column names
        zscores.columns = ['{0}~{1}'.format(x, y) for x, y in
                           zip(self.pair_info.Leg1, self.pair_info.Leg2)]
        return zscores

    @staticmethod
    def _get_spread_zscores(close1, close2, window):
        """
        Simple normalization
        """
        spreads = np.subtract(np.log(close1), np.log(close2))
        ma = spreads.rolling(window=window).mean()
        std = spreads.rolling(window=window).std()
        return (spreads - ma) / std

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


# ~~~~~~ Optimized Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#@numba.jit(nopython=True)
def get_return_series(enter_z, exit_z,
                      zscores, close1, close2,
                      returns):
    """
    Takes in multidimensional zscores, closes1/closes2 etc
    """
    n_days, n_pairs = close1.shape
    for j in xrange(n_pairs):
        side = 0
        for i in xrange(n_days-1):
            # LONGS
            if (side != 1) & (zscores[i, j] >= enter_z):
                side = 1
                returns[i+1, j] = close2[i+1, j] / close2[i, j] - close1[i+1, j] / close1[i, j]
            elif (side == 1) & (zscores[i, j] <= exit_z):
                side = 0
            elif (side == 1):
                returns[i+1, j] = close2[i+1, j] / close2[i, j] - close1[i+1, j] / close1[i, j]

            # SHORTS
            elif (side != -1) & (-zscores[i, j] >= enter_z):
                side = -1
                returns[i+1, j] = close1[i+1, j] / close1[i, j] - close2[i+1, j] / close2[i, j]
            elif (side == -1) & (-zscores[i, j] <= exit_z):
                side = 0
            elif (side == -1):
                returns[i+1, j] = close1[i+1, j] / close1[i, j] - close2[i+1, j] / close2[i, j]


# ~~~~~~ Response Variables ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_trade_signal_series(zscores, close1, close2, enter_z, exit_z):
    signals = np.zeros(zscores.shape)
    _get_trade_signal_series(zscores, close1, close2, enter_z, exit_z, signals)
    return signals


@numba.jit(nopython=True)
def _get_trade_signal_series(zscores, close1, close2,
                             enter_z, exit_z, signals):
    n_days, n_pairs = zscores.shape
    for col in xrange(n_pairs):
        i = 0
        j = 0
        while i < (n_days - 1):
            # LONGS
            if zscores[i, col] >= enter_z:
                j = i + 1
                while True:
                    if (zscores[j, col] <= exit_z) or (j == (n_days - 1)):
                        signals[i, col] = close2[j, col] / close2[i, col] - \
                            close1[j, col] / close1[i, col]
                        break
                    j += 1
                i = j + 1
            # SHORTS
            elif -zscores[i, col] >= enter_z:
                j = i + 1
                while True:
                    if (-zscores[j, col] <= exit_z) or (j == (n_days - 1)):
                        signals[i, col] = close1[j, col] / close1[i, col] - \
                            close2[j, col] / close2[i, col]
                        break
                    j += 1
                i = j + 1
            else:
                i += 1


# ~~~~~~ Helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_abs_distance(x_index, indexes):
    return np.sum(np.abs(x_index[:, None] - indexes), axis=0)

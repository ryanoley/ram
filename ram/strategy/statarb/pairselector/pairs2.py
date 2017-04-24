import numba
import numpy as np
import pandas as pd
import itertools as it

from sklearn.model_selection import KFold


class PairSelector2(object):

    def get_iterable_args(self):
        return {'pairs2': [True],
                'n_pairs': [500, 5000]}

    def rank_pairs(self, data, n_pairs=10, pairs2=False):
        # Reshape Close data
        close_data = data.pivot(index='Date',
                                columns='SecCode',
                                values='AdjClose')
        volume_data = data.pivot(index='Date',
                                 columns='SecCode',
                                 values='AdjVolume')
        cut_date = data.Date[~data.TestFlag].max()
        train_close = close_data.loc[close_data.index <= cut_date]
        train_close = train_close.T.dropna().T
        if pairs2:
            pair_info = self._filter_pairs2(train_close)
        else:
            pair_info = self._filter_pairs(train_close)
        return pair_info.iloc[:n_pairs]

    def get_responses(self, z_window=20, enter_z=2, exit_z=1):
        zscores = self.get_zscores(z_window).fillna(0).values
        close1 = self.close_data[self.pair_info.Leg1].values
        close2 = self.close_data[self.pair_info.Leg2].values
        return pd.DataFrame(
            get_trade_signal_series(zscores, close1, close2, enter_z, exit_z),
            index=self.close_data.index,
            columns = ['{}~{}'.format(x, y) for x, y in
                       self.pair_info[['Leg1', 'Leg2']].values])

    def get_responses2(self, take=0.05, maxdays=20):
        close1 = self.close_data[self.pair_info.Leg1].values
        close2 = self.close_data[self.pair_info.Leg2].values
        resp1, resp2 = get_response_series(close1, close2, take, maxdays)
        output1 = pd.DataFrame(
            resp1,
            index=self.close_data.index,
            columns = ['{}~{}'.format(x, y) for x, y in
                       self.pair_info[['Leg1', 'Leg2']].values])
        output2 = output1.copy()
        output2[:] = resp2
        return output1, output2

    def _filter_pairs(self, close_data, n_pairs=15000):
        pairs = self._prep_output(close_data)
        pairs['distances'] = self._flatten(self._get_distances(close_data))
        pairs = pairs.sort_values('distances').reset_index(drop=True)
        return pairs.iloc[:n_pairs]

    def _filter_pairs2(self, close_data, n_pairs=15000):
        pairs = self._prep_output(close_data)
        pairs['distances'] = self._get_distances2(close_data)
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

    def get_spread_index(self):
        # Create two data frames that represent Leg1 and Leg2
        close1 = self.close_data.loc[:, self.pair_info.Leg1]
        close2 = self.close_data.loc[:, self.pair_info.Leg2]
        spreads = np.subtract(np.log(close1), np.log(close2))
        # Add correct column names
        spreads.columns = ['{0}~{1}'.format(x, y) for x, y in
                           zip(self.pair_info.Leg1, self.pair_info.Leg2)]
        return spreads

    # ~~~~~~ Volume Spread ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_volume_spread(self, z_window=20):
        # Create two data frames that represent Leg1 and Leg2
        volume1 = self.volume_data.loc[:, self.pair_info.Leg1]
        volume2 = self.volume_data.loc[:, self.pair_info.Leg2]
        # MAs
        volume1 = volume1 / volume1.rolling(z_window).mean()
        volume2 = volume2 / volume2.rolling(z_window).mean()
        spreads = np.subtract(volume1, volume2)
        # Add correct column names
        spreads.columns = ['{0}~{1}'.format(x, y) for x, y in
                           zip(self.pair_info.Leg1, self.pair_info.Leg2)]
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

    def _get_distances2(self, close_data):
        rets_data = close_data.pct_change().iloc[1:].values
        # Ten folds
        kf = KFold(10, False, 123)
        out = []
        for train, test in kf.split(rets_data):
            out.append(self._flatten(np.apply_along_axis(
                get_abs_distance, 0, rets_data[test], rets_data[test])))
        return np.argsort(np.argsort(np.array(out))).mean(axis=0)

    @staticmethod
    def _prep_output(close_data):
        legs = zip(*it.combinations(close_data.columns.values, 2))
        stat_df = pd.DataFrame({'Leg1': legs[0]})
        stat_df['Leg2'] = legs[1]
        return stat_df


# ~~~~~~ Optimized Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@numba.jit(nopython=True)
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

def get_response_series(close1, close2, stop, max_days):
    responses1 = np.zeros(close1.shape)
    responses2 = np.zeros(close1.shape)
    # Ensure this is a negative number
    stop = -1 * abs(stop)
    return _get_response_series(close1, close2,
                                responses1, responses2,
                                stop, max_days)

# This is more a stop
@numba.jit(nopython=True)
def _get_response_series(close1, close2,
                         responses1, responses2,
                         stop, max_days):
    n_days, n_pairs = close1.shape
    for c in range(n_pairs):
        for i in range(n_days-1):
            # Days forward
            days_forward = (i+max_days+1) if (i+max_days+1) < (n_days-1) \
                else (n_days-1)
            # Long Close2, Short Close1
            for j in range(i+1, days_forward):
                if close2[j, c] / close2[i, c] - \
                        close1[j, c] / close1[i, c] < stop:
                    break
            responses1[i, c] = close2[j, c] / close2[i, c] - \
                close1[j, c] / close1[i, c]

            # Long Close1, Short Close2
            for j in range(i+1, days_forward):
                if close1[j, c] / close1[i, c] - \
                        close2[j, c] / close2[i, c] < stop:
                    break
            responses2[i, c] = close1[j, c] / close1[i, c] - \
                close2[j, c] / close2[i, c]
    return responses1, responses2

# ~~~~~~ Trade Signals ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

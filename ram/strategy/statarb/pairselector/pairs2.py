import numpy as np
import pandas as pd
import itertools as it

from ram.strategy.statarb.pairselector.base import BasePairSelector


class PairsStrategy2(BasePairSelector):

    def get_feature_names(self):
        return ['AdjClose', 'AvgDolVol', 'GSECTOR']

    def get_best_pairs(self, data, cut_date, z_window, max_pairs):

        # Reshape Close data
        close_data = data.pivot(index='Date',
                                columns='SecCode',
                                values='AdjClose')

        train_close = close_data.loc[close_data.index < cut_date]

        # Clean data for training
        train_close = train_close.T.dropna().T

        pairs = self._get_stats_all_pairs(train_close)
        fpairs = self._filter_pairs(pairs, data, max_pairs)

        # Create daily z-scores
        test_pairs = self._get_test_zscores(close_data, cut_date,
                                            fpairs, z_window)
        return test_pairs, fpairs

    def _filter_pairs(self, pairs, data, max_pairs=2000):
        """
        Function is to score based on incoming stats
        """
        # Merge GSECTOR
        gsectors = data.groupby('SecCode')['GSECTOR'].min().reset_index()
        pairs = pairs.merge(gsectors, left_on='Leg1', right_on='SecCode')
        pairs = pairs.merge(gsectors, left_on='Leg2', right_on='SecCode')
        pairs = pairs[pairs.GSECTOR_x == pairs.GSECTOR_y]
        pairs['Sector'] = pairs.GSECTOR_x
        pairs = pairs.loc[:, ['Leg1', 'Leg2', 'distances', 'Sector']]
        # Rank values
        rank1 = np.argsort(np.argsort(pairs.distances))
        pairs.loc[:, 'score'] = rank1 + 1
        pairs = pairs.sort_values('score', ascending=True)
        pairs = pairs[pairs.score <= max_pairs].reset_index()
        return pairs

    def _get_stats_all_pairs(self, close_data):
        # Convert to index
        index_vals = close_data / close_data.iloc[0]
        index_vals = np.array(index_vals)
        # Get all stats
        X2 = np.apply_along_axis(self._get_abs_distance, 0,
                                 index_vals, index_vals)
        # Reformat
        legs = zip(*it.combinations(close_data.columns.values, 2))
        stat_df = pd.DataFrame({'Leg1': legs[0]})
        stat_df['Leg2'] = legs[1]
        z1 = list(it.combinations(range(len(close_data.columns)), 2))
        stat_df['distances'] = [X2[z1[i]] for i in range(len(z1))]
        return stat_df

    @staticmethod
    def _get_abs_distance(x_index, indexes):
        return np.sum(np.abs(x_index[:, None] - indexes), axis=0)

    def _get_test_zscores(self, Close, cut_date, fpairs, window):
        # Create two data frames that represent Leg1 and Leg2
        df_leg1 = Close.loc[:, fpairs.Leg1]
        df_leg2 = Close.loc[:, fpairs.Leg2]
        outdf = self._get_spread_zscores(df_leg1, df_leg2, window)
        # Add correct column names
        outdf.columns = ['{0}_{1}'.format(x, y) for x, y in
                         zip(fpairs.Leg1, fpairs.Leg2)]
        return outdf.loc[outdf.index >= cut_date]

    def _get_spread_zscores(self, close1, close2, window):
        """
        Simple normalization
        """
        spreads = np.subtract(np.log(close1), np.log(close2))
        ma, std = self._get_moving_avg_std(spreads, window)
        return (spreads - ma) / std

    @staticmethod
    def _get_moving_avg_std(X, window):
        """
        Optimized calculation of rolling mean and standard deviation.
        """
        ma_df = X.rolling(window=window).mean()
        std_df = X.rolling(window=window).std()
        return ma_df, std_df

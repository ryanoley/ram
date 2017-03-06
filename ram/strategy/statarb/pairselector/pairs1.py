import numpy as np
import pandas as pd
import itertools as it

from ram.strategy.statarb.pairselector.base import BasePairSelector


class PairsStrategy1(BasePairSelector):

    def get_iterable_args(self):
        return {'z_window': [10, 20, 30],
                'max_pairs': [3000, 6000],
                'same_sector': [True, False],
                'vol_ratio_filter': [0.5]}

    def get_feature_names(self):
        return ['AdjClose', 'AvgDolVol', 'GSECTOR']

    def get_best_pairs(self, data, cut_date, z_window, max_pairs,
                       same_sector, vol_ratio_filter):

        # Reshape Close data
        close_data = data.pivot(index='Date',
                                columns='SecCode',
                                values='AdjClose')

        train_close = close_data.loc[close_data.index < cut_date]

        # Clean data for training
        train_close = train_close.T.dropna().T

        pairs = self._get_stats_all_pairs(train_close)
        fpairs = self._filter_pairs(pairs, data, max_pairs, same_sector,
                                    vol_ratio_filter)

        # Create daily z-scores
        test_pairs = self._get_test_zscores(close_data, cut_date,
                                            fpairs, z_window)
        return test_pairs, fpairs

    def _filter_pairs(self, pairs, data, max_pairs, same_sector,
                      vol_ratio_filter):
        """
        Function is to score based on incoming stats
        """
        if same_sector:
            # Merge GSECTOR
            gsectors = data.groupby('SecCode')['GSECTOR'].min().reset_index()
            pairs = pairs.merge(gsectors, left_on='Leg1', right_on='SecCode')
            pairs = pairs.merge(gsectors, left_on='Leg2', right_on='SecCode')
            pairs = pairs[pairs.GSECTOR_x == pairs.GSECTOR_y]
            pairs['Sector'] = pairs.GSECTOR_x
            pairs = pairs.drop(['GSECTOR_x', 'GSECTOR_y',
                                'SecCode_x', 'SecCode_y'], axis=1)
        # Vol ratio filter
        pairs = pairs.loc[np.abs(pairs.volratio - 1) < vol_ratio_filter].copy()
        # Rank values
        rank1 = np.argsort(np.argsort(-pairs.corrcoef))
        rank2 = np.argsort(np.argsort(pairs.distances))
        pairs.loc[:, 'score'] = rank1 + rank2
        # Sort
        pairs = pairs.sort_values('score', ascending=True)
        pairs = pairs.iloc[:max_pairs].reset_index(drop=True)
        return pairs

    def _get_stats_all_pairs(self, close_data):
        # Convert to numpy array for calculations
        rets_a = np.array(close_data.pct_change().dropna())
        index_a = close_data / close_data.iloc[0]
        index_a = np.array(index_a)

        # Get matrix of all combos
        X1 = self._get_corr_coef(rets_a)
        X2 = np.apply_along_axis(self._get_corr_moves, 0, rets_a, rets_a)
        X3 = np.apply_along_axis(self._get_vol_ratios, 0, rets_a, rets_a)
        X4 = np.apply_along_axis(self._get_abs_distance, 0, index_a, index_a)

        # Output
        legs = zip(*it.combinations(close_data.columns.values, 2))
        stat_df = pd.DataFrame({'Leg1': legs[0]})
        stat_df['Leg2'] = legs[1]

        # Capture going first down rows, then over columns
        # (Column, Row)
        z1 = list(it.combinations(range(len(close_data.columns)), 2))

        stat_df['corrcoef'] = [X1[z1[i]] for i in range(len(z1))]
        stat_df['corrmoves'] = [X2[z1[i]] for i in range(len(z1))]
        stat_df['volratio'] = [X3[z1[i]] for i in range(len(z1))]
        stat_df['distances'] = [X4[z1[i]] for i in range(len(z1))]

        return stat_df

    @staticmethod
    def _get_abs_distance(x_index, indexes):
        return np.sum(np.abs(x_index[:, None] - indexes), axis=0)

    @staticmethod
    def _get_corr_coef(rets):
        """
        Returns correlation coefficients of first column vs rest
        """
        return np.corrcoef(rets.T)

    @staticmethod
    def _get_corr_moves(x_ret, rets):
        """
        Returns percentage of moves that are the same of first column vs rest
        """
        # Percent ups and downs that match
        Z = (rets >= 0).astype(int) + (x_ret[:, None] >= 0).astype(int)
        return ((Z != 1).sum(axis=0) / float(len(Z)))

    @staticmethod
    def _get_vol_ratios(x_ret, rets):
        """
        Returns return series volatility, first column over rest columns
        """
        x_std = np.array([np.std(x_ret)] * rets.shape[1])
        all_std = np.std(rets, axis=0)
        # Flipped when it is pulled from nXn
        return all_std / x_std

    def _get_test_zscores(self, Close, cut_date, fpairs, window):
        # Create two data frames that represent Leg1 and Leg2
        df_leg1 = Close.loc[:, fpairs.Leg1]
        df_leg2 = Close.loc[:, fpairs.Leg2]
        outdf = self._get_spread_zscores(df_leg1, df_leg2, window)
        # Add correct column names
        outdf.columns = ['{0}~{1}'.format(x, y) for x, y in
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

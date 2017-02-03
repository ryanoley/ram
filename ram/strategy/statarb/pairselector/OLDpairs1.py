import numpy as np
import pandas as pd
import itertools as it

from ram.strategy.statarb.pairselector.base import BasePairSelector

from statsmodels.tsa.stattools import adfuller


class PairsStrategy1(BasePairSelector):

    def get_best_pairs(self, data, cut_date, window=60,
                       abs_corr_filter=0.5, abs_corr_move_filter=0.5,
                       vol_ratio_filter=0.3):
        """
        """
        # Reshape Close data
        close_data = data.pivot(index='Date',
                                columns='SecCode',
                                values='ADJClose')

        train_close = close_data.loc[close_data.index < cut_date]
        pairs = self._get_stats_all_pairs(train_close)
        fpairs = self._filter_pairs(pairs, abs_corr_filter,
                                    abs_corr_move_filter,
                                    vol_ratio_filter)
        # Create daily z-scores
        test_pairs = self._get_test_zscores(close_data, cut_date,
                                            fpairs, window)
        return test_pairs

    def _filter_pairs(self, pairs, abs_corr_filter,
                      abs_corr_move_filter, vol_ratio_filter):
        """
        Function is to score based on incoming stats
        """
        # Filters
        pairs = pairs.loc[np.abs(pairs.volratio - 1) < vol_ratio_filter].copy()
        pairs = pairs.loc[np.abs(pairs.corrcoef) > abs_corr_filter]
        pairs = pairs.loc[np.abs(pairs.corrmoves) > abs_corr_move_filter]
        # Rank values
        rank1 = np.argsort(np.argsort(pairs.corrcoef))
        rank2 = np.argsort(np.argsort(pairs.corrmoves))
        pairs.loc[:, 'score'] = rank1 + rank2
        pairs = pairs.sort_values('score', ascending=False)
        return pairs

    def _get_stats_all_pairs(self, close):
        # Returns needed for some calculations
        rets = close.pct_change().dropna()
        # Convert to numpy array for calculations
        close_a = np.array(close)
        rets_a = np.array(rets)
        # Get matrix of all combos
        X1 = self._get_corr_coef(rets_a)
        X2 = np.apply_along_axis(self._get_corr_moves, 0, rets_a, rets_a)
        X3 = np.apply_along_axis(self._get_vol_ratios, 0, rets_a, rets_a)
        # X4 = np.apply_along_axis(self._get_adf_p_values, 0, close_a, close_a)
        # Output
        legs = zip(*it.combinations(close.columns.values, 2))
        stat_df = pd.DataFrame({'Leg1': legs[0]})
        stat_df['Leg2'] = legs[1]
        # Capture going first down rows, then over columns
        # (Column, Row)
        z1 = list(it.combinations(range(len(close.columns)), 2))
        # Can we speed this up?
        stat_df['corrcoef'] = [X1[z1[i]] for i in range(len(z1))]
        stat_df['corrmoves'] = [X2[z1[i]] for i in range(len(z1))]
        stat_df['volratio'] = [X3[z1[i]] for i in range(len(z1))]
        # stat_df['adf'] = [X4[z1[i]] for i in range(len(z1))]
        return stat_df

    @staticmethod
    def _get_corr_coef(rets):
        """
        Returns correlation coefficients of first column vs rest

        Parameters
        ----------
        rets : df
            Return series
        """
        return np.corrcoef(rets.T)

    @staticmethod
    def _get_corr_moves(x_ret, rets):
        """
        Returns percentage of moves that are the same of first column vs rest

        Parameters
        ----------
        rets : df
            Return series
        """
        # Percent ups and downs that match
        Z = (rets >= 0).astype(int) + (x_ret[:, None] >= 0).astype(int)
        return ((Z != 1).sum(axis=0) / float(len(Z)))

    @staticmethod
    def _get_vol_ratios(x_ret, rets):
        """
        Returns return series volatility, first column over rest columns

        Parameters
        ----------
        rets : df
            Return series
        """
        x_std = np.array([np.std(x_ret)] * rets.shape[1])
        all_std = np.std(rets, axis=0)
        # Flipped when it is pulled from nXn
        return all_std / x_std

    @staticmethod
    def _get_adf_p_values(x_close, close):
        pairs = (np.log(close).T - np.log(x_close))
        return np.apply_along_axis(
            lambda y: adfuller(y, maxlag=1)[1], 1, pairs)

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

import numpy as np
import pandas as pd
import itertools as it


class Pairs:

    def get_best_pairs(self,
                       data,
                       cut_date,
                       z_window,
                       max_pairs):
        train_close = data.loc[data.index < cut_date]
        pairs = self._get_stats_all_pairs(train_close)
        fpairs = self._filter_pairs(pairs, data, max_pairs)
        # Create daily z-scores
        test_rets, test_pairs = self._get_test_zscores(data, cut_date,
                                                       fpairs, z_window)
        return test_rets, test_pairs, fpairs

    def _filter_pairs(self, pairs, data, max_pairs):
        """
        Function is to score based on incoming stats
        """
        # Rank values
        rank1 = np.argsort(np.argsort(-pairs.corrcoef))
        rank2 = np.argsort(np.argsort(pairs.distances))
        pairs.loc[:, 'score'] = rank1 + rank2
        # Sort
        pairs = pairs.sort_values('score', ascending=True)
        pairs = pairs.iloc[:max_pairs].reset_index(drop=True)
        return pairs

    def _get_stats_all_pairs(self, data):
        # Convert to numpy array for calculations
        rets_a = np.array(data)
        index_a = np.array(data.cumsum())

        # Get matrix of all combos
        X1 = self._get_corr_coef(rets_a)
        X2 = np.apply_along_axis(self._get_corr_moves, 0, rets_a, rets_a)
        X3 = np.apply_along_axis(self._get_vol_ratios, 0, rets_a, rets_a)
        X4 = np.apply_along_axis(self._get_abs_distance, 0, index_a, index_a)

        # Output
        legs = zip(*it.combinations(data.columns.values, 2))
        stat_df = pd.DataFrame({'Leg1': legs[0]})
        stat_df['Leg2'] = legs[1]

        # Capture going first down rows, then over columns
        # (Column, Row)
        z1 = list(it.combinations(range(len(data.columns)), 2))

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

    def _get_test_zscores(self, data, cut_date, fpairs, window):
        # Create two data frames that represent Leg1 and Leg2
        df_leg1 = (data.loc[:, fpairs.Leg1].cumsum() + 1) * 100
        df_leg2 = (data.loc[:, fpairs.Leg2].cumsum() + 1) * 100
        outdf = self._get_spread_zscores(df_leg1, df_leg2, window)
        # Get returns
        rets_leg1 = data.loc[:, fpairs.Leg1].copy()
        rets_leg2 = data.loc[:, fpairs.Leg2].copy()
        outdf_rets = rets_leg1 - rets_leg2.values
        # Add correct column names
        outdf.columns = ['{0}_{1}'.format(x, y) for x, y in
                         zip(fpairs.Leg1, fpairs.Leg2)]
        outdf_rets.columns = ['{0}_{1}'.format(x, y) for x, y in
                              zip(fpairs.Leg1, fpairs.Leg2)]
        return outdf_rets.loc[outdf_rets.index >= cut_date], \
            outdf.loc[outdf.index >= cut_date]

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

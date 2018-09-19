import numpy as np
import pandas as pd
import itertools as it


class Pairs:

    def get_best_pairs(self,
                       data,
                       cut_date,
                       z_window=20,
                       max_pairs=1000,
                       close_prices=False):
        """
        Parameters
        ----------
        data : pd.DataFrame
            Should have dates in the index, and time series in the column.
            It can be Closes or Returns.
        cut_date : dt.date/datetime
            The first date of the test period.
        z_window : int
            This function returns z-scores, and z-scores require some window
            over which the standard deviation is calculated.
        max_pairs: int
            Because by creating pairs, there are n choose m pairs, larger
            data frames can have massive numbers of pairs. This gives you the
            ability to restrict the number that are calculated and returned.

        Returns
        -------
        test_rets : pd.DataFrame
            The return series for the top pairs
        test_zscores : pd.DataFrame
            Z-Scores series for top pairs. Aligns with test_rets
        pair_info : pd.DataFrame
            Description stats of the pairs
        """
        if close_prices:
            data = data.pct_change(1).iloc[1:]
        # Create statistics for all pairs from training data
        pair_info = self._get_stats_all_pairs(data.loc[data.index < cut_date])
        # Select top pairs by some scoring mechanism
        pair_info = self._filter_pairs(pair_info, data, max_pairs)
        # Create daily z-scores
        test_rets, test_zscores = self._get_test_zscores(data,
                                                         cut_date,
                                                         pair_info,
                                                         z_window)
        return test_rets, test_zscores, pair_info

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
        """
        Parameters
        ----------
        data : pd.DataFrame
            Return series
        """
        # Output
        output = self._create_output_object(data)

        data = np.array(data)
        # Cumulative sum of return series
        index = np.cumsum(data, axis=0)
        # Get matrix of all combos
        X1 = self._get_corr_coef(data)
        X2 = np.apply_along_axis(self._get_corr_moves, 0, data, data)
        X3 = np.apply_along_axis(self._get_vol_ratios, 0, data, data)
        X4 = np.apply_along_axis(self._get_abs_distance, 0, index, index)

        # Capture going first down rows, then over columns (Column, Row)
        z1 = list(it.combinations(range(data.shape[1]), 2))

        output['corrcoef'] = [X1[z1[i]] for i in range(len(z1))]
        output['corrmoves'] = [X2[z1[i]] for i in range(len(z1))]
        output['volratio'] = [X3[z1[i]] for i in range(len(z1))]
        output['distances'] = [X4[z1[i]] for i in range(len(z1))]

        return output

    def _create_output_object(self, data):
        legs = zip(*it.combinations(data.columns.values, 2))
        output = pd.DataFrame({'Leg1': legs[0]})
        output['Leg2'] = legs[1]
        return output

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
        # NOTE: this relationship is flipped to make sure it works when
        # extracted in _get_stats_all_pairs
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
        outdf.columns = ['{0}~{1}'.format(x, y) for x, y in
                         zip(fpairs.Leg1, fpairs.Leg2)]
        outdf_rets.columns = ['{0}~{1}'.format(x, y) for x, y in
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

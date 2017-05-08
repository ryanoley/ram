import numpy as np
import pandas as pd
import itertools as it


class PairSelector(object):

    def get_iterable_args(self):
        return {'n_pairs': [500]}

    def rank_pairs(self, data, n_pairs):
        """
        pair_test_flag is a placeholder if you ever want to do an A/B
        test with the ranking of pairs. HOWEVER, each unique hyper
        parameter setting for PairSelector creates a copy of the data
        and should be used sparingly to avoid a memory explosion.
        """
        # Reshape Close data
        close_data = data.pivot(index='Date',
                                columns='SecCode',
                                values='AdjClose')
        cut_date = data.Date[~data.TestFlag].max()
        train_close = close_data.loc[close_data.index <= cut_date]
        train_close = train_close.T.dropna().T
        pairs = self._get_stats_all_pairs(train_close)
        scored_pairs = self._score_pairs(pairs)
        return scored_pairs.iloc[:n_pairs]

    def _get_stats_all_pairs(self, close_data):
        # Convert to numpy array for calculations
        rets_a = np.array(close_data.pct_change().dropna())
        index_a = np.array(close_data / close_data.iloc[0])
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

    def _score_pairs(self, pairs):
        """
        Function is to score based on incoming stats
        """
        # Rank values
        rank1 = np.argsort(np.argsort(-pairs.corrcoef))
        rank2 = np.argsort(np.argsort(pairs.distances))
        pairs.loc[:, 'score'] = rank1 + rank2
        # Sort
        return pairs.sort_values('score').reset_index(drop=True)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

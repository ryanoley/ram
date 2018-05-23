import numpy as np
import pandas as pd
import itertools as it
import datetime as dt

from ram.analysis.model_selection.model_selection import ModelSelection


class PairSearch(ModelSelection):

    def get_implementation_name(self):
        return 'PairsSearch'

    def get_top_models(self, time_index, train_data):

        pairs = self._get_stats_all_pairs(train_data)

        # Rank Returns
        total_rets = train_data.sum().reset_index()
        total_rets['rank'] = np.argsort(np.argsort(total_rets[0]))

        total_rets.columns = ['Leg1', 'Ret1', 'Rank1']
        pairs = pairs.merge(total_rets)
        total_rets.columns = ['Leg2', 'Ret2', 'Rank2']
        pairs = pairs.merge(total_rets)

        # Only select from best returns
        top_rank = int(pairs.Rank1.max() * 0.8)
        pairs2 = pairs[(pairs.Rank1 > top_rank) &
                       (pairs.Rank2 > top_rank)].copy()

        best_inds = []
        pairs2 = pairs2.sort_values('volratio')
        tops, bottoms = self._get_top_bottom_seccodes(pairs2)
        best_inds.append(tops)
        best_inds.append(bottoms)

        pairs2 = pairs2.sort_values('distances')
        tops, bottoms = self._get_top_bottom_seccodes(pairs2)
        best_inds.append(tops)
        best_inds.append(bottoms)

        pairs2 = pairs2.sort_values('corrmoves')
        tops, bottoms = self._get_top_bottom_seccodes(pairs2)
        best_inds.append(tops)
        best_inds.append(bottoms)

        pairs2 = pairs2.sort_values('corrcoef')
        tops, bottoms = self._get_top_bottom_seccodes(pairs2)
        best_inds.append(tops)
        best_inds.append(bottoms)

        scores = [0] * len(best_inds)
        return best_inds, scores

    ###########################################################################

    def _get_top_bottom_seccodes(self, pairs, n_seccodes=10):
        tops = []
        bottoms = []
        for i in range(len(pairs)):
            if pairs.Leg1.iloc[i] not in tops:
                tops.append(pairs.Leg1.iloc[i])
            if pairs.Leg2.iloc[i] not in tops:
                tops.append(pairs.Leg2.iloc[i])
            if len(tops) > n_seccodes:
                break
        for i in range(len(pairs))[::-1]:
            if pairs.Leg1.iloc[i] not in bottoms:
                bottoms.append(pairs.Leg1.iloc[i])
            if pairs.Leg2.iloc[i] not in bottoms:
                bottoms.append(pairs.Leg2.iloc[i])
            if len(bottoms) > n_seccodes:
                break
        return tops, bottoms

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_stats_all_pairs(self, data):
        data.columns = range(data.shape[1])
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

    # def _score_pairs(self, pairs):
    #     """
    #     Function is to score based on incoming stats
    #     """
    #     # Rank values
    #     rank1 = np.argsort(np.argsort(-pairs.corrcoef))
    #     rank2 = np.argsort(np.argsort(pairs.distances))
    #     pairs.loc[:, 'score'] = rank1 + rank2
    #     # Sort
    #     return pairs.sort_values('score').reset_index(drop=True)

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

import numpy as np
import pandas as pd
import itertools as it

from ram.strategy.statarb.pairselector.base import BasePairSelector


class PairsStrategy2(BasePairSelector):

    def __init__(self):
        pass

    def get_feature_names(self):
        return ['AdjClose', 'AvgDolVol', 'GSECTOR']

    def get_best_pairs(self, data, cut_date, n_per_side=2, max_pairs=1000,
                       z_window=20, **kwargs):

        # Reshape Close data
        close_data = data.pivot(index='Date',
                                columns='SecCode',
                                values='AdjClose')

        # Get clean ids from training
        clean_ids = self._get_clean_ids(close_data, cut_date)
        close_data = close_data.loc[:, clean_ids]

        sectors = self._classify_sectors(data, clean_ids)

        # All indexes are created on
        ret_data = close_data.pct_change().iloc[1:]

        # Output objects
        test_z_scores = pd.DataFrame(index=ret_data.loc[cut_date:].index)
        port_scores = pd.DataFrame()

        for sec, seccodes in sectors.iteritems():

            if len(seccodes) < (2 * n_per_side):
                # Happens with low univ size counts
                continue

            # Get data for the Sector
            sec_ret_data = ret_data.loc[:, seccodes]

            # Get combinations of random columns
            combs1, combs2 = self._generate_random_cols(
                len(seccodes), n_per_side)

            # Due to size of combinations, this iterates in chunks of 20k
            chunksize = 10000
            iter_count = combs1.shape[0] / chunksize + 1
            for i in range(iter_count + 1):
                start_i = i * chunksize
                end_i = (i+1) * chunksize
                comb_indexes = self._get_comb_indexes(sec_ret_data,
                                                      combs1[start_i:end_i],
                                                      combs2[start_i:end_i])

                # Get "scores" for these column combinations
                scores = self._get_comb_scores(comb_indexes, cut_date)

                # Get top values assuming all top pairs come from just this sector
                scores_sorted = scores.sort_values()[:max_pairs/iter_count]

                # Get Test Z Scores of these indexes
                z_scores = self._get_spread_zscores(
                    comb_indexes.loc[:, scores_sorted.index], z_window, cut_date)

                port_scores = port_scores.append(pd.DataFrame({
                    'Scores': scores_sorted,
                    'Sector': sec,
                }))

                test_z_scores = test_z_scores.join(z_scores)

        # Final sort and selection
        port_scores = port_scores.sort_values('Scores')[:max_pairs]

        return test_z_scores.loc[:, port_scores.index], port_scores

    @staticmethod
    def _get_clean_ids(close_data, cut_date):
        # CLEAN: Remove columns that have any missing training data
        return close_data.loc[close_data.index < cut_date] \
            .T.dropna().T.columns.tolist()

    ###########################################################################

    def _get_comb_indexes(self, sec_data, combs1, combs2):
        """
        Each day the returns for each Side (comb array) are averaged
        as if they were equal-weighted portfolios that are rebalanced daily.

        These returns are cumsum'd to create and index, and then the
        opposite side is subtracted from it.
        """
        # Ensure proper type
        sec_dataA = np.array(sec_data.T)
        combs1 = np.array(combs1)
        combs2 = np.array(combs2)
        comb_indexes = pd.DataFrame(
            (np.cumsum(np.mean(sec_dataA[combs1], axis=1), axis=1) - \
             np.cumsum(np.mean(sec_dataA[combs2], axis=1), axis=1)).T)
        comb_indexes.index = sec_data.index
        comb_indexes.columns = self._concatenate_seccodes(
            sec_data.columns.values, combs1, combs2)
        return comb_indexes

    def _get_comb_scores(self, comb_indexes, cut_date):
        return comb_indexes.loc[comb_indexes.index < cut_date].abs().sum()

    ###########################################################################

    def _generate_random_cols(self, n_choices, n_per_side):
        """
        Generates random combinations of columns in two sets. A single
        example would look like this with two per side: [1, 2] and [3, 4]

        The complex function assures uniqueness of elements, so the following
        examples would be filtered out:
        * [1, 1]  [89, 93]      -- Same col on same side
        * [1, 2]  [1, 6]        -- Same col on opposite side
        * [[1, 2], [7, 9]]  [[3, 5], [1, 2]]   -- Same combination on opposite
                                                  side
        """
        combs = np.random.randint(0, high=n_choices,
                                  size=(400000, n_per_side * 2))
        combs1 = combs[:, :n_per_side]
        combs2 = combs[:, n_per_side:]
        return self._filter_combs(combs1, combs2)

    @staticmethod
    def _filter_combs(combs1, combs2):
        """
        Separated for testing.
        """
        combs1 = np.sort(combs1, axis=1)
        combs2 = np.sort(combs2, axis=1)
        # Drop repeats in same row on each side
        inds1 = (np.sum(np.diff(combs1, axis=1) == 0, axis=1) == 0)
        inds2 = (np.sum(np.diff(combs2, axis=1) == 0, axis=1) == 0)
        combs1 = combs1[inds1 & inds2]
        combs2 = combs2[inds1 & inds2]

        # Drop repeats in same row for opposite sides
        combs = np.hstack((combs1, combs2))
        combs = np.sort(combs, axis=1)
        inds = (np.sum(np.diff(combs, axis=1) == 0, axis=1) == 0)
        combs1 = combs1[inds]
        combs2 = combs2[inds]

        # Ensure same ports aren't flipped
        combs1a = np.where((combs1[:, 0] < combs2[:, 0])[:, np.newaxis],
                            combs1, combs2)
        combs2a = np.where((combs1[:, 0] >= combs2[:, 0])[:, np.newaxis],
                            combs1, combs2)

        # Drop repeat rows
        combs = np.hstack((combs1a, combs2a))
        combs = combs[np.lexsort([combs[:, i] for i in
                                  range(combs.shape[1]-1, -1, -1)])]
        inds = np.append(True, np.sum(np.diff(combs, axis=0) != 0, axis=1) != 0)
        combs = combs[inds]
        # Split again
        n_cols = combs1.shape[1]

        return combs[:, :n_cols], combs[:, n_cols:]

    @staticmethod
    def _concatenate_seccodes(seccodes, combs1, combs2):
        z1 = np.array(seccodes)[combs1]
        z1 = ['_'.join(x) for x in z1]
        z2 = np.array(seccodes)[combs2]
        z2 = ['_'.join(x) for x in z2]
        return ['~'.join(x) for x in zip(z1, z2)]

    ###########################################################################

    @staticmethod
    def _classify_sectors(data, clean_ids):
        # Classify seccodes by Sector
        tmp = data[['SecCode','GSECTOR']].drop_duplicates()
        tmp = tmp[tmp.SecCode.isin(clean_ids)]
        sectors = {}
        for sc in np.unique(tmp.GSECTOR):
            sectors[sc] = tmp[tmp.GSECTOR == sc].SecCode.tolist()
        return sectors

    ###########################################################################

    def _get_spread_zscores(self, comb_indexes, window, cut_date):
        """
        Simple normalization
        """
        ma, std = self._get_moving_avg_std(comb_indexes, window)
        return ((comb_indexes - ma) / std).loc[cut_date:]

    @staticmethod
    def _get_moving_avg_std(X, window):
        """
        Optimized calculation of rolling mean and standard deviation.
        """
        ma_df = X.rolling(window=window).mean()
        std_df = X.rolling(window=window).std()
        return ma_df, std_df

import numpy as np
import pandas as pd
import itertools as it

from ram.strategy.statarb.pairselector.base import BasePairSelector


class PairsStrategy2(BasePairSelector):

    def __init__(self, secs_per_side=2):
        self.secs_per_side = secs_per_side

    def get_feature_names(self):
        return ['AdjClose', 'AvgDolVol', 'GSECTOR']

    def get_best_pairs(self, data, cut_date, n_per_side=2, max_pairs=1000,
                       z_window=20, **kwargs):
        # Reshape Close data
        close_data = data.pivot(index='Date',
                                columns='SecCode',
                                values='AdjClose')

        train_close = close_data.loc[close_data.index < cut_date]

        # Clean data for training
        train_close = train_close.T.dropna().T
        train_close = train_close / train_close.iloc[0]

        # Classify seccodes by Sector
        tmp = data[['SecCode','GSECTOR']].drop_duplicates()
        tmp = tmp[tmp.SecCode.isin(train_close.columns)]
        sectors = {}
        for sc in np.unique(tmp.GSECTOR):
            sectors[sc] = tmp[tmp.GSECTOR == sc].SecCode.tolist()

        # Generate random combos
        port_scores, columns = self._get_sector_portfolios(
            train_close, sectors, n_per_side, max_pairs)

        # Create daily z-scores
        test_pairs = self._get_test_zscores(close_data, cut_date,
                                            columns, port_scores, z_window)
        return test_pairs, fpairs

    def _get_sector_portfolios(self, data, sectors, n_per_side, max_pairs):
        ret_data = data.pct_change().dropna()
        comb_scores = pd.DataFrame()
        comb_names = np.array([['a','a','a','a']])
        for sec, seccodes in sectors.iteritems():
            # Get data
            sec_data = np.array(ret_data.loc[:, seccodes]).T
            # Get combinations of random columns
            combs1, combs2 = self._generate_random_cols(len(seccodes),
                                                        n_per_side)
            sum_comb_rets = self._get_comb_scores(sec_data, combs1, combs2)

            cn = self._concatenate_seccodes(seccodes, combs1, combs2)

            comb_scores = comb_scores.append(pd.DataFrame({
                'Scores': sum_comb_rets,
                'Sector': sec
            }))
            comb_names = np.vstack((comb_names, cn))

        comb_names = comb_names[1:]

        inds = np.argsort(comb_scores.Scores).values

        comb_scores = comb_scores.iloc[inds][:max_pairs]
        comb_names = comb_names[inds][:max_pairs]

        return comb_scores, comb_names

    def _get_comb_scores(self, sec_data, combs1, combs2):
        # Ensure proper type
        sec_data = np.array(sec_data)
        combs1 = np.array(combs1)
        combs2 = np.array(combs2)
        comb_rets = \
            np.cumsum(np.mean(sec_data[combs1], axis=1), axis=1) - \
            np.cumsum(np.mean(sec_data[combs2], axis=1), axis=1)
        return np.sum(np.abs(comb_rets), axis=1)

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
                                  size=(200000, n_per_side * 2))
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
        z2 = np.array(seccodes)[combs2]
        return np.hstack((z1, z2))

    ###########################################################################

    def _get_test_zscores(self, Close, cut_date, columns, fpairs, window):
        import pdb; pdb.set_trace()

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

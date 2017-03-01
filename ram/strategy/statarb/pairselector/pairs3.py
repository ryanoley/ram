import numpy as np
import pandas as pd
import itertools as it

from ram.strategy.statarb.pairselector.base import BasePairSelector
from ram.strategy.statarb.pairselector.pairs1 import PairsStrategy1

from sklearn.ensemble import RandomForestClassifier


class PairsStrategy3(BasePairSelector):

    features = ['LAG1_RANK_PRMA10_AdjClose', 'LAG1_RANK_PRMA20_AdjClose',
                'LAG1_RANK_DISCOUNT63_AdjClose', 'LAG1_RANK_VOL20_AdjClose',
                'LAG1_RANK_BOLL20_AdjClose']

    def get_iterable_args(self):
        return {'max_pairs': [3000],
                'same_sector': [False],
                'vol_ratio_filter': [0.3]}

    def get_feature_names(self):
        return self.features + ['AdjClose', 'AvgDolVol', 'GSECTOR']

    def get_best_pairs(self, data, cut_date, max_pairs,
                       same_sector, vol_ratio_filter):

        # Reshape Close data
        close_data = data.pivot(index='Date',
                                columns='SecCode',
                                values='AdjClose')

        train_close = close_data.loc[close_data.index < cut_date]

        # Clean data for training
        train_close = train_close.T.dropna().T

        pairs = PairsStrategy1()._get_stats_all_pairs(train_close)
        fpairs = PairsStrategy1()._filter_pairs(
            pairs, data, max_pairs, same_sector, vol_ratio_filter)

        # Create daily z-scores
        zscores = self._get_zscores(close_data, fpairs, z_window)

        reg_train_data = merge_pairs(data[data.Date < cut_date],
                                     fpairs, self.features)
        reg_train_data = reg_train_data.merge(zscores).dropna()

        reg_test_data = merge_pairs(data[data.Date >= cut_date],
                                    fpairs, self.features)
        reg_test_data = reg_test_data.merge(zscores)
        # Train a model
        cl = RandomForestClassifier(n_estimators=25, min_samples_split=200)
        cl.fit(
            X=reg_train_data.drop(['Date', 'Pair', 'Response1','Response2'], axis=1),
            y=reg_train_data[['Response1', 'Response2']])
        ests = cl.predict_proba(
            reg_test_data.drop(['Date', 'Pair', 'Response1', 'Response2'], axis=1))
        reg_test_data['score'] = ests[0][:, 1] - ests[1][:, 1]
        scores = reg_test_data.pivot(index='Date',
                                     columns='Pair',
                                     values='score')
        # FLIP TO GET SCORES CORRET
        return -1 * scores, fpairs

    def _get_zscores(self, Close, fpairs, window):
        for window in [10, 20, 30]:
            # Create two data frames that represent Leg1 and Leg2
            df_leg1 = Close.loc[:, fpairs.Leg1]
            df_leg2 = Close.loc[:, fpairs.Leg2]
            zscores = self._get_spread_zscores(df_leg1, df_leg2, window)
            # Add correct column names
            zscores.columns = ['{0}~{1}'.format(x, y) for x, y in zip(fpairs.Leg1, fpairs.Leg2)]
            zscores = zscores.unstack().reset_index()
            zscores.columns = ['Pair', 'Date', 'ZScore{}'.format(window)]
            if window == 10:
                outdf = zscores
            else:
                outdf = outdf.merge(zscores)
        return outdf

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


def merge_pairs(df, fpairs, features):
    df = df.sort_values(['SecCode', 'Date']).copy()
    df['ForPrice'] = df.AdjClose.shift(-2)
    df.loc[df.SecCode != df.SecCode.shift(-2), 'ForPrice'] = np.nan
    df['Ret'] = df.ForPrice / df.AdjClose - 1
    df2 = df.set_index('SecCode')
    counts = df.SecCode.value_counts()
    side1 = df2.loc[fpairs.Leg1].reset_index()
    side1.loc[:, 'pair_num'] = np.repeat(range(len(fpairs)), counts[fpairs.Leg1])
    side2 = df2.loc[fpairs.Leg2].reset_index()
    side2.loc[:, 'pair_num'] = np.repeat(range(len(fpairs)), counts[fpairs.Leg2])
    out = side1.merge(side2, on=['Date', 'pair_num'])
    # Make response variable
    out['PairRet'] = out.Ret_x - out.Ret_y
    return_inflection = out.groupby('Date')['PairRet'].quantile(.70)
    return_inflection = return_inflection.reset_index()
    return_inflection.columns = ['Date', 'inf_point']
    out = out.merge(return_inflection)
    out['Response1'] = out.PairRet >= out.inf_point
    out = out.drop('inf_point', axis=1)
    return_inflection = out.groupby('Date')['PairRet'].quantile(.30)
    return_inflection = return_inflection.reset_index()
    return_inflection.columns = ['Date', 'inf_point']
    out = out.merge(return_inflection)
    out['Response2'] = out.PairRet <= out.inf_point
    # Format output
    out['Pair'] = ['{0}~{1}'.format(x, y) for x, y in zip(out.SecCode_x,
                                                          out.SecCode_y)]
    out_features = ['Date', 'Pair', 'Response1', 'Response2']
    out_features += ['{}_x'.format(i) for i in features]
    out_features += ['{}_y'.format(i) for i in features]
    return out.loc[:, out_features]

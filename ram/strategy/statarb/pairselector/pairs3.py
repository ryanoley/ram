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
        scored_pairs = PairsStrategy1()._filter_pairs(
            pairs, data, max_pairs, same_sector, vol_ratio_filter)

        # Create daily z-scores for all data
        zscores = _get_zscores(close_data, scored_pairs)

        # Make data
        feature_data = _make_simulation_data(data, scored_pairs, self.features)
        feature_data = feature_data.merge(zscores).dropna()

        # TODO: NEED TO TRIM TWO DAYS OFF TRAIN DATA
        # Separate training from test data
        train_data = feature_data[feature_data.Date < cut_date]
        test_data = feature_data[feature_data.Date >= cut_date]

        features = _extract_features(feature_data)

        # Train a model
        cl = RandomForestClassifier(n_estimators=25, min_samples_split=200)
        cl.fit(X=train_data[features],
               y=train_data[['Response1', 'Response2']])
        ests = cl.predict_proba(test_data[features])
        test_data['score'] = ests[0][:, 1] - ests[1][:, 1]
        scores = test_data.pivot(index='Date', columns='Pair', values='score')
        return -1 * scores, scored_pairs


###############################################################################

def _get_zscores(close_prices, scored_pairs, windows=[10, 20, 30]):
    """
    Creates zscores that can be merged onto data files for the pair
    """
    close_leg1 = close_prices.loc[:, scored_pairs.Leg1]
    close_leg2 = close_prices.loc[:, scored_pairs.Leg2]
    columns = ['{0}~{1}'.format(x, y) for x, y in
               zip(scored_pairs.Leg1, scored_pairs.Leg2)]
    for window in windows:
        zscores = _get_spread_zscores(close_leg1, close_leg2, window)
        # Add correct column names
        zscores.columns = columns
        zscores = zscores.unstack().reset_index()
        zscores.columns = ['Pair', 'Date', 'ZScore{}'.format(window)]
        if window == windows[0]:
            outdf = zscores
        else:
            outdf = outdf.merge(zscores)
    return outdf


def _get_spread_zscores(close1, close2, window):
    """
    Simple normalization
    """
    spreads = np.subtract(np.log(close1), np.log(close2))
    ma = spreads.rolling(window=window).mean()
    std = spreads.rolling(window=window).std()
    return (spreads - ma) / std


def _extract_features(feature_data):
    features = feature_data.columns.tolist()
    features.remove('Date')
    features.remove('Pair')
    features.remove('Response1')
    features.remove('Response2')
    return features

###############################################################################

def _make_simulation_data(data, scored_pairs, features):
    """
    Formats the data appropriately. Below this function are all the
    helper functions that are individually tested.
    """
    scored_pairs = _clean_scored_pairs_df(scored_pairs, data)
    data = _make_return_column(data)
    feature_data = _match_pair_feature_data(data, scored_pairs, features)
    feature_data = _make_responses(feature_data)
    return feature_data


def _clean_scored_pairs_df(scored_pairs, data):
    """
    Filter scored_pairs for missing seccodes from data. Can happen because
    of stocks that have stopped trading
    """
    unique_seccodes = data.SecCode.unique()
    scored_pairs = scored_pairs.loc[scored_pairs.Leg1.isin(unique_seccodes)]
    scored_pairs = scored_pairs.loc[scored_pairs.Leg2.isin(unique_seccodes)]
    return scored_pairs


def _make_return_column(data):
    # Sort data to get appropriate Forward prices for return calculation
    data = data.sort_values(['SecCode', 'Date']).copy()
    data['ForPrice'] = data.AdjClose.shift(-2)
    data.loc[data.SecCode != data.SecCode.shift(-2), 'ForPrice'] = np.nan
    data['Ret'] = data.ForPrice / data.AdjClose - 1
    return data.drop('ForPrice', axis=1)


def _match_pair_feature_data(data, scored_pairs, features):
    """
    Cast new dataframes that have get all daily data for each seccode
    in order from fpairs
    """
    counts = data.SecCode.value_counts()
    data = data.set_index('SecCode')
    side1 = data.loc[scored_pairs.Leg1].reset_index()
    side1.loc[:, 'pair_num'] = np.repeat(range(len(scored_pairs)), counts[scored_pairs.Leg1])
    side2 = data.loc[scored_pairs.Leg2].reset_index()
    side2.loc[:, 'pair_num'] = np.repeat(range(len(scored_pairs)), counts[scored_pairs.Leg2])
    feature_data = side1.merge(side2, on=['Date', 'pair_num'])
    # Format output
    feature_data['Pair'] = ['{0}~{1}'.format(x, y) for x, y
                            in zip(feature_data.SecCode_x,
                                   feature_data.SecCode_y)]
    out_features = ['Date', 'Pair', 'Ret_x', 'Ret_y']
    out_features += ['{}_x'.format(i) for i in features]
    out_features += ['{}_y'.format(i) for i in features]
    return feature_data.loc[:, out_features]


def _make_responses(feature_data, perc=0.3):
    fd = feature_data.copy()
    fd['PairRet'] = fd.Ret_x - fd.Ret_y
    # Get return inflections by quantile
    return_inflection = fd.groupby('Date')['PairRet'].quantile(1-perc)
    return_inflection = return_inflection.reset_index()
    return_inflection.columns = ['Date', 'inf_point']
    fd = fd.merge(return_inflection)
    fd['Response1'] = fd.PairRet >= fd.inf_point
    fd = fd.drop('inf_point', axis=1)
    # Second response
    return_inflection = fd.groupby('Date')['PairRet'].quantile(perc)
    return_inflection = return_inflection.reset_index()
    return_inflection.columns = ['Date', 'inf_point']
    fd = fd.merge(return_inflection)
    fd['Response2'] = fd.PairRet <= fd.inf_point
    return fd.drop(['inf_point', 'PairRet', 'Ret_x', 'Ret_y'], axis=1)

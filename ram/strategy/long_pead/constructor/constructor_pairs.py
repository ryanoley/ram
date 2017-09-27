import numba
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.utils import make_variable_dict

from ram.strategy.long_pead.constructor.portfolio import Portfolio
from ram.strategy.long_pead.constructor.base_constructor import Constructor


class PortfolioConstructorPairs(Constructor):

    def get_args(self):
        return {
            'logistic_spread': [0.1],
            'pair_min_offsets': [1, 3, 5],
            'pair_max_offsets': [10, 100]
        }

    def get_position_sizes(self, date, scores,
                           logistic_spread,
                           pair_min_offsets,
                           pair_max_offsets):
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.

        The weighting scheme takes on the shape of a sigmoid function,
        and the shape of the sigmoid is modulated by the hyperparameter
        logistic spread.
        """
        # Capture all seccodes for output object. Bad SecCodes will be filtered
        # during construction phase.
        all_seccodes = scores.keys()
        # Format and Retrieve Scores and ZScores
        scores = _format_scores_dict(scores)
        zscores = _extract_zscore_data(self.data_container, date)
        scores = _merge_scores_zscores_data(scores, zscores)
        # Get scores
        scores = _select_port_and_offsets(scores, logistic_spread,
                                          pair_min_offsets,
                                          pair_max_offsets)

        scores.pos_size *= self.booksize
        scores = scores.merge(pd.DataFrame({'SecCode': all_seccodes}),
                              how='outer').fillna(0)
        scores = scores.set_index('SecCode')
        return scores.pos_size.to_dict()


def _select_port_and_offsets(data,
                             logistic_spread,
                             pair_min_offsets,
                             pair_max_offsets):

    # Get approximate side to match correct pairs
    sides = data[['SecCode', 'Signal']].drop_duplicates()
    sides = sides.sort_values('Signal')
    sides['long'] = sides.Signal > sides.Signal.median()
    data = data.merge(sides)
    temp1 = data[(~data.long) & (data.zscore > 0)]
    temp2 = data[(data.long) & (data.zscore < 0)]
    data = temp1.append(temp2)

    # Counts filter
    counts = data.groupby('SecCode')['Signal'].count().reset_index()
    counts.columns = ['SecCode', 'counts']
    data = data.merge(counts)
    data = data[data.counts >= pair_min_offsets]

    # Max number
    data = _zscore_rank(data)
    data = data[data.zscore_rank <= pair_max_offsets]

    # Get proper sizing given remaing names
    ranked_weights = _get_weighting(
        data[['SecCode', 'Signal']].drop_duplicates(),
        'Signal', logistic_spread)
    data = data.merge(ranked_weights)

    # Get norm factor
    norm_factor = data.groupby('SecCode')['zscore'].sum().reset_index()
    norm_factor.columns = ['SecCode', 'norm_factor']
    data = data.merge(norm_factor)
    data['offset_signal'] = data.zscore / data.norm_factor * \
        data.Weighted_Signal * -1

    offset_size = data.groupby('OffsetSecCode')['offset_signal'].sum()
    main_size = data.groupby('SecCode')['Weighted_Signal'].max()
    out = main_size.add(offset_size, fill_value=0).reset_index()
    out.columns = ['SecCode', 'pos_size']
    # Scale to get everything to one
    out.pos_size = out.pos_size * (1 / out.pos_size.abs().sum())
    return out


def _zscore_rank(data):
    data['absZscore'] = data.zscore.abs() * -1
    data = data.sort_values(['SecCode', 'absZscore'])
    ids = data.SecCode.astype('category').cat.codes.values
    ranks = np.zeros(data.shape[0])
    _zscore_rank_numba(ranks, ids)
    data['zscore_rank'] = ranks
    return data


@numba.jit(nopython=True)
def _zscore_rank_numba(ranks, seccodes):
    seccode = seccodes[0]
    ranks[0] = 1
    rank = 1
    for i in xrange(1, len(ranks)):
        if seccodes[i] == seccode:
            rank += 1
        else:
            seccode = seccodes[i]
            rank = 1
        ranks[i] = rank


def _extract_zscore_data(data_container, date):
    zscores = pd.DataFrame({'zscore': data_container.zscores.loc[date]})
    zscores = zscores.reset_index()
    zscores.columns = ['pair', 'zscore']
    zscores = zscores.merge(data_container.zscores_pair_info)
    return zscores


def _merge_scores_zscores_data(scores, zscores):
    scores.columns = ['Leg1', 'Score1']
    zscores = zscores.merge(scores)
    scores.columns = ['Leg2', 'Score2']
    data = zscores.merge(scores)
    data['SecCode'] = data.Leg1
    data['OffsetSecCode'] = data.Leg2
    data['Signal'] = data.Score1
    data['SignalOffset'] = data.Score2
    return data[['SecCode', 'OffsetSecCode', 'Signal', 'SignalOffset',
                 'distances', 'zscore']].reset_index(drop=True)


def _format_scores_dict(scores):
    scores = pd.Series(scores).to_frame().reset_index()
    scores.columns = ['SecCode', 'RegScore']
    scores = scores[scores.RegScore.notnull()]
    scores = scores.sort_values('RegScore')
    return scores.reset_index(drop=True)


def _get_weighting(data, rank_column, logistic_spread):
    data = data.copy()
    data = data.sort_values(rank_column)

    def logistic_weight(k):
        return 2 / (1 + np.exp(-k)) - 1
    n_good = (~data[rank_column].isnull()).sum()
    n_bad = data[rank_column].isnull().sum()
    new_col = 'Weighted_{}'.format(rank_column)
    data[new_col] = [
        logistic_weight(x) for x in np.linspace(
            -logistic_spread, logistic_spread, n_good)] + [0] * n_bad
    data[new_col] = data[new_col] / data[new_col].abs().sum()
    return data.reset_index(drop=True)

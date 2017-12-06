import numba
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.statarb.utils import make_variable_dict

from ram.strategy.statarb.abstract.portfolio_constructor import \
    BasePortfolioConstructor


class PortfolioConstructorPairs(BasePortfolioConstructor):

    def get_args(self):
        return {
            'params': [
                # ORIGINAL Implementation
                {'type': 'basic'},

                # # TREES - LONG ONLY
                {'type': 'tree_model_long', 'pair_offsets': 1,
                 'signal_thresh_perc': 50},
                {'type': 'tree_model_long', 'pair_offsets': 3,
                 'signal_thresh_perc': 50},
                {'type': 'tree_model_long', 'pair_offsets': 5,
                 'signal_thresh_perc': 50},

                {'type': 'tree_model_long', 'pair_offsets': 1,
                 'signal_thresh_perc': 70},
                {'type': 'tree_model_long', 'pair_offsets': 3,
                 'signal_thresh_perc': 70},
                {'type': 'tree_model_long', 'pair_offsets': 5,
                 'signal_thresh_perc': 70},

                # TREES
                {'type': 'tree_model', 'pair_offsets': 1,
                 'signal_thresh_perc': 50},
                {'type': 'tree_model', 'pair_offsets': 3,
                 'signal_thresh_perc': 50},
                {'type': 'tree_model', 'pair_offsets': 5,
                 'signal_thresh_perc': 50},

                {'type': 'tree_model', 'pair_offsets': 1,
                 'signal_thresh_perc': 70},
                {'type': 'tree_model', 'pair_offsets': 3,
                 'signal_thresh_perc': 70},
                {'type': 'tree_model', 'pair_offsets': 5,
                 'signal_thresh_perc': 70},
            ]
        }

    def set_args(self, params):
        self._params = params

    def set_signals_constructor_data(self, signals, data):
        self._signals = signals.copy()
        # Clean zscores and pair info SecCodes that aren't
        # in signals (test_data)
        unique_sec_codes = signals.SecCode.unique()

        pair_info = data['pair_info']
        pair_info = pair_info[pair_info.PrimarySecCode.isin(unique_sec_codes)]
        pair_info = pair_info[pair_info.OffsetSecCode.isin(unique_sec_codes)]

        zscores = data['zscores']
        zscores = zscores[pair_info.pair]

        zscores = _merge_zscores_pair_info(zscores, pair_info)
        zscores = _merge_zscores_signals(zscores, signals, pair_info)
        zscores = _final_format(zscores)
        self._zscores = zscores
        if 'pricing' in data:
            self._pricing = data['pricing']

    def get_day_position_sizes(self, date, scores):
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
        clean_scores = {x: y for x, y in scores.iteritems() if ~np.isnan(y)}

        zscores = self._zscores.loc[date]

        # Only keep zscores that have signals, because nans represent
        # stocks that have been filtered
        zscores = zscores[
            (zscores.SecCode.isin(clean_scores.keys())) |
            (zscores.OffsetSecCode.isin(clean_scores.keys()))]

        # Get relative position sizes
        scores = _select_port_and_offsets(clean_scores, zscores, self._params)

        # Scale to dollar value
        for key in scores.keys():
            scores[key] *= self.booksize
        # Add in missing scores, and return dictionary
        missing_codes = list(set(all_seccodes) -
                             set(scores.keys()))
        scores2 = dict(zip(missing_codes, [0]*len(missing_codes)))
        scores.update(scores2)

        return scores


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _select_port_and_offsets(scores, data, params):
    if params['type'] == 'basic':
        return _basic(scores)

    elif params['type'] == 'tree_model_long':
        return _tree_model(scores, data, params, True)

    elif params['type'] == 'tree_model':
        return _tree_model(scores, data, params)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _tree_model(scores, data, params, long_only_flag=False):
    # Get approximate side to match correct pairs
    signals = pd.Series(scores).reset_index()
    signals.columns = ['SecCode', 'Signal']

    # Get Long Values from model
    thresh = np.percentile(signals.Signal, params['signal_thresh_perc'])
    longs = signals[signals.Signal > thresh]
    longs = data[data.SecCode.isin(longs.SecCode.values)].copy()
    longs = longs[longs.zscore < 0]

    longs = _tree_model_get_pos_sizes(longs, params['pair_offsets'])

    if long_only_flag:
        return _tree_model_aggregate_pos_sizes(longs)

    # Get Short values from model
    thresh = np.percentile(signals.Signal, 100 - params['signal_thresh_perc'])
    shorts = signals[signals.Signal < thresh]
    shorts = data[data.SecCode.isin(shorts.SecCode.values)].copy()
    shorts = shorts[shorts.zscore > 0]

    shorts = shorts.iloc[::-1].copy()  # Flip for ranking function
    shorts.Signal *= -1  # Flip signal to get short

    shorts = _tree_model_get_pos_sizes(shorts, params['pair_offsets'])
    # FLIP position sizes for shorts
    shorts.pos_size *= -1
    shorts.offset_size *= -1

    positions = longs.append(shorts)

    return _tree_model_aggregate_pos_sizes(positions)


def _tree_model_get_pos_sizes(data, pair_offsets):
    """
    * Input should be sorted by SecCode, ZScore
    * Rank is applied to zscore per seccode
    * Top ranked rows are selected
    * Weighting is dependent upon SecCode signal
    * If pair_offsets > 1, then multiple SecCodes per row, but unique
        offsets
    """
    ranks, counts = _zscore_rank(data)
    data = data[(ranks <= pair_offsets) &
                (counts >= pair_offsets)].copy()
    data['pos_size'] = _get_weighting(data.Signal,
                                      long_only_flag=True,
                                      logistic_spread=0.1)
    data['offset_size'] = data.pos_size * -1
    return data


def _tree_model_aggregate_pos_sizes(data):
    """
    Stacks data and sums up over unique seccodes
    """
    output = {}
    total_sum = 0.0
    for s, v in zip(data.SecCode, data.pos_size):
        if s not in output:
            output[s] = 0
        output[s] += v
        total_sum += abs(v)
    for s, v in zip(data.OffsetSecCode, data.offset_size):
        if s not in output:
            output[s] = 0
        output[s] += v
        total_sum += abs(v)
    # Normalize everything
    for key in output.keys():
        output[key] /= total_sum
    assert sum(output.values()) < 0.001
    return output


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _basic(scores):
    signals = pd.Series(scores).reset_index()
    signals.columns = ['SecCode', 'Signal']

    signals['pos_size'] = _get_weighting(signals.Signal,
                                         logistic_spread=0.1)
    return dict(zip(signals.SecCode, signals.pos_size))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _merge_zscores_pair_info(zscores, pair_info):
    # Create dataframe with pair/date in index, with zscore and signals
    pair_info.columns = ['SecCode', 'OffsetSecCode',
                         'distances', 'distance_rank', 'pair']
    assert np.all(pair_info.pair.values == zscores.columns.values)
    zscores = zscores.unstack()
    zscores.index.names = ['pair', 'Date']
    zscores.name = 'zscore'
    zscores = pair_info.set_index('pair').join(zscores)
    return zscores


def _merge_zscores_signals(zscores, signals, pair_info):
    # Create data frame that has Signals/Offset signals, according to `pair`
    signals2 = signals.pivot(index='Date', columns='SecCode', values='preds')
    # Regular signals
    signals3 = signals2[pair_info.SecCode]
    signals3.columns = columns = [
        '{0}~{1}'.format(x, y) for x, y in zip(pair_info.SecCode,
                                               pair_info.OffsetSecCode)]
    signals3 = signals3.unstack()
    signals3.name = 'Signal'
    # Offset signals
    signals4 = signals2[pair_info.OffsetSecCode]
    signals4.columns = columns = [
        '{0}~{1}'.format(x, y) for x, y in zip(pair_info.SecCode,
                                               pair_info.OffsetSecCode)]
    signals4 = signals4.unstack()
    signals4.name = 'OffsetSignal'
    # Sanity check: Assert dates/pairs indexes are the same
    assert np.all(signals3.index == zscores.index)
    assert np.all(signals4.index == zscores.index)
    # Put into columns
    zscores['Signal'] = signals3
    zscores['OffsetSignal'] = signals4
    return zscores


def _final_format(zscores):
    zscores = zscores.reset_index()
    # Sorted for downstream analysis
    zscores = zscores.sort_values(['Date', 'SecCode', 'zscore'])
    zscores['cindex'] = range(len(zscores))
    return zscores.set_index(['Date', 'cindex'])


def _zscore_rank(data):
    """
    Assumes this is just one day's data
    """
    # Check columns
    d_seccode = (data.SecCode != data.SecCode.shift(1)).values
    d_seccode[0] = False
    ranks = np.zeros(len(data))
    counts = np.zeros(len(data))
    _zscore_rank_numba(d_seccode, ranks, counts)
    return ranks, counts


@numba.jit(nopython=True)
def _zscore_rank_numba(d_seccode, ranks, counts):
    ranks[0] = 1
    rank = 1
    for i in xrange(1, len(ranks)):
        if d_seccode[i]:
            for j in xrange(i-rank, i):
                counts[j] = rank
            rank = 1
        else:
            rank += 1
        ranks[i] = rank
    for j in xrange(i-rank, i+1):
            counts[j] = rank


def _format_scores_dict(scores):
    scores = pd.Series(scores).to_frame().reset_index()
    scores.columns = ['SecCode', 'RegScore']
    scores = scores[scores.RegScore.notnull()]
    scores = scores.sort_values('RegScore')
    return scores.reset_index(drop=True)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _get_weighting(x, long_only_flag=False, logistic_spread=0.01):
    """
    Logistic weighting based on ranks.

    If Long only, uses logistic shape for range above zero, else
    both positive and negative.

    If long only, entire portfolio has sum of 1. If not, sum of 0.
    """
    if not isinstance(x, pd.Series):
        x = pd.Series(x)
    # Get rank values, fill NAN with zero
    ranks = x.rank(method='min').fillna(0).astype(int).values
    count = ranks.max()

    # Create array of scores - 0 position for nan values get 0
    def logistic_weight(k):
        return 2 / (1 + np.exp(-k)) - 1
    if long_only_flag:
        scores = np.array([0] +
                          [logistic_weight(x) for x in np.linspace(
                               0, logistic_spread*2, count)])
    else:
        scores = np.array([0] +
                          [logistic_weight(x) for x in np.linspace(
                                -logistic_spread, logistic_spread, count)])
    # Grab scores
    output = scores[ranks]
    return output / np.abs(output).sum()

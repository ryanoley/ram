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

                {'type': 'tree_model_long', 'pair_offsets': 1,
                 'signal_thresh_perc': 70},
                {'type': 'tree_model_long', 'pair_offsets': 3,
                 'signal_thresh_perc': 70},

                # TREES
                {'type': 'tree_model', 'pair_offsets': 3},
                {'type': 'tree_model', 'pair_offsets': 7},

                # # ZSCORES
                {'type': 'zscore_model', 'z_thresh': 0.8, 'n_per_side': 3},
                {'type': 'zscore_model', 'z_thresh': 0.8, 'n_per_side': 5},
                {'type': 'zscore_model', 'z_thresh': 1.2, 'n_per_side': 3},
                {'type': 'zscore_model', 'z_thresh': 1.2, 'n_per_side': 5},

            ]
        }

    def set_args(self, params):
        self._params = params

    def set_signals_constructor_data(self, signals, data):
        self._signals = signals.copy()
        zscores = _merge_zscores_pair_info(data['zscores'],
                                           data['pair_info'])
        zscores = _merge_zscores_signals(zscores, signals, data['pair_info'])
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

        scores.pos_size *= self.booksize
        # Add in missing scores, and return dictionary
        missing_codes = list(set(all_seccodes) -
                             set(scores.SecCode.unique().tolist()))
        scores = dict(zip(scores.SecCode, scores.pos_size))
        scores2 = dict(zip(missing_codes, [0]*len(missing_codes)))
        scores.update(scores2)

        return scores


def _select_port_and_offsets(scores, data, params):

    if params['type'] == 'basic':
        signals = pd.Series(scores).reset_index()
        signals.columns = ['SecCode', 'Signal']

        signals['pos_size'] = _get_weighting(signals.Signal,
                                             logistic_spread=0.1)
        return signals

    elif params['type'] == 'tree_model_long':
        """
        Idea is to go long the stocks the tree model says go long,
        and offset with best X pairs ranked by z-score
        """
        signals = pd.Series(scores).reset_index()
        signals.columns = ['SecCode', 'Signal']

        # Get long signals
        thresh = np.percentile(signals.Signal, params['signal_thresh_perc'])
        signals = signals[signals.Signal > thresh]

        # Filter for negative zscores, which means go long SecCode
        data = data[data.zscore < 0]
        data = data.merge(signals)

        # Rank and count available offsetting securities
        ranks, counts = _zscore_rank(data)

        data = data[(ranks <= params['pair_offsets']) &
                    (counts >= params['pair_offsets'])].copy()

        data['pos_size'] = _get_weighting(data.Signal,
                                          long_only_flag=True,
                                          logistic_spread=0.1)
        data['short_size'] = -1 * data.pos_size / params['pair_offsets']

        output = data[['SecCode', 'pos_size']]
        output2 = data[['OffsetSecCode', 'short_size']]
        output2.columns = ['SecCode', 'pos_size']
        positions = output.append(output2).groupby('SecCode')['pos_size'].sum()
        positions = positions.reset_index()
        positions.pos_size = positions.pos_size * \
            (1 / positions.pos_size.abs().sum())

        return positions

    elif params['type'] == 'tree_model':

        # Get approximate side to match correct pairs
        signals = pd.Series(scores).reset_index()
        signals.columns = ['SecCode', 'Signal']
        signals['long'] = signals.Signal > signals.Signal.median()

        data = data.merge(signals)

        shorts = data[(~data.long) & (data.zscore > 0)].iloc[::-1]
        ranks, counts = _zscore_rank(shorts)

        shorts = shorts[(ranks <= params['pair_offsets']) &
                        (counts >= params['pair_offsets'])].copy()

        shorts['pos_size'] = _get_weighting(shorts.Signal,
                                            long_only_flag=True,
                                            logistic_spread=0.1) * -1
        shorts['offset_size'] = shorts.pos_size * -1 / params['pair_offsets']

        longs = data[(data.long) & (data.zscore < 0)]
        ranks, counts = _zscore_rank(longs)

        longs = longs[(ranks <= params['pair_offsets']) &
                      (counts >= params['pair_offsets'])].copy()

        longs['pos_size'] = _get_weighting(longs.Signal,
                                           long_only_flag=True,
                                           logistic_spread=0.1)
        longs['offset_size'] = longs.pos_size * -1 / params['pair_offsets']

        data = longs.append(shorts)

        output = data[['SecCode', 'pos_size']]
        output2 = data[['OffsetSecCode', 'offset_size']]
        output2.columns = ['SecCode', 'pos_size']
        positions = output.append(output2).groupby('SecCode')['pos_size'].sum()
        positions = positions.reset_index()
        positions.pos_size = positions.pos_size * \
            (1 / positions.pos_size.abs().sum())

        return positions

    elif params['type'] == 'zscore_model':
        # Takes position in OffsetSecCode

        # SHORTS
        shorts = data[data.zscore < -params['z_thresh']]
        ranks, counts = _zscore_rank(shorts)

        shorts = shorts[(ranks <= params['n_per_side']) &
                        (counts >= params['n_per_side'])].copy()
        # Count
        shorts['pos_size'] = -1

        # LONGS
        longs = data[data.zscore > params['z_thresh']].loc[::-1]
        ranks, counts = _zscore_rank(longs)

        longs = longs[(ranks <= params['n_per_side']) &
                      (counts >= params['n_per_side'])].copy()
        # Count
        longs['pos_size'] = 1

        # Weight with logistic function
        positions = longs.append(shorts)[['OffsetSecCode', 'pos_size']]
        positions.columns = ['SecCode', 'pos_size']
        positions = positions.groupby('SecCode')['pos_size'].sum()
        positions = positions.reset_index()
        positions['pos_size'] = _get_weighting(positions.pos_size,
                                               long_only_flag=False,
                                               logistic_spread=0.1)
        positions.pos_size = positions.pos_size * \
            (1 / positions.pos_size.abs().sum())

        return positions


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

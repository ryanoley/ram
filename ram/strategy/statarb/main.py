import itertools
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy

from ram.strategy.statarb.pairselector.pairs1 import PairsStrategy1
from ram.strategy.statarb.pairselector.pairs2 import PairsStrategy2
from ram.strategy.statarb.constructor.constructor import PortfolioConstructor


class StatArbStrategy(Strategy):

    def __init__(self, **args):
        super(StatArbStrategy, self).__init__()
        self.pairselector = PairsStrategy2()
        self.constructor = PortfolioConstructor()
        self.univ_size = 500

    def get_iter_index(self):
        return range(len(self._get_date_iterator()))

    def run_index(self, index):

        t_start, cut_date, t_end = self._get_date_iterator()[index]

        if t_start < dt.datetime(2010, 1, 1):
            out = {}
            out['data'] = pd.DataFrame({}, index=pd.DatetimeIndex([0]))
            out['meta'] = {}
            return out

        data = self._get_data(t_start, cut_date, t_end,
                              univ_size=self.univ_size)

        args1 = {'z_window': 30, 'max_pairs': 1000}
        scores, pair_info = self.pairselector.get_best_pairs(
            data, cut_date, **args1)

        args2 = {'n_pairs': 100, 'max_pos_prop': 0.05}
        results = self.constructor.get_daily_pl(
            scores, data, pair_info, **args2)

        return {'meta': {}, 'data': results}

    def _get_date_iterator(self):
        """
        Bookend dates for start training, start test (~eval date)
        and end test sets.

        Training data has one year's data, test is one quarter.

        NOTE:   Could this be put into the data handler at some point?
        """
        all_dates = self.datahandler.get_all_dates()
        # BUG: for some reason the filter doesn't work for earliest dates
        if len(all_dates) > 326:
            all_dates = all_dates[326:]
        # Generate first dates of quarter
        qtrs = np.array([(d.month-1)/3 + 1 for d in all_dates])
        inds = np.append([True], np.diff(qtrs) != 0)
        # Add in final date from all dates available.
        inds[-1] = True
        quarter_dates = all_dates[inds]
        # Get train start, test start, and final quarter date
        iterable = zip(quarter_dates[:-5],
                       quarter_dates[4:-1],
                       quarter_dates[5:])
        return iterable

    def _get_data(self, start_date, filter_date, end_date, univ_size):
        """
        Makes the appropriate period's data.
        """
        # Adjust date by one day for filter date
        adj_filter_date = filter_date - dt.timedelta(days=1)

        filter_args = {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 and GSECTOR not in (55) ' +
                'and Close_ > 15',
            'univ_size': univ_size}

        features = list(set(self.pairselector.get_feature_names() +
                            self.constructor.get_feature_names()))

        data = self.datahandler.get_filtered_univ_data(
            features=features,
            start_date=start_date,
            end_date=end_date,
            filter_date=adj_filter_date,
            filter_args=filter_args)

        data = data.drop_duplicates()
        data.SecCode = data.SecCode.astype(str)

        return data

    @staticmethod
    def _make_iter(variants):
        return [{x: y for x, y in zip(variants.keys(), vals)}
            for vals in itertools.product(*variants.values())]


if __name__ == '__main__':

    strategy = StatArbStrategy()
    strategy.start()

    from gearbox import to_desk
    to_desk(strategy.results, 'statarb_run')

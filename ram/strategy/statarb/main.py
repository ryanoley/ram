import json
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

        data = self._get_data(t_start, cut_date, t_end,
                              univ_size=self.univ_size)

        args1 = make_arg_iter({
            'z_window': [30, 40, 50, 60, 100, 140],
            'max_pairs': [1000, 1500]
        })

        args2 = make_arg_iter({
            'n_pairs': [80, 120, 180],
            'max_pos_prop': [0.05, 0.10],
            'pos_perc_deviation': [0.3, 0.5, 0.7],
        })

        ind = 0
        output_results = pd.DataFrame()
        output_params = {}

        for a1 in args1:
            scores, pair_info = self.pairselector.get_best_pairs(
                data, cut_date, **a1)

            for a2 in args2:
                results = self.constructor.get_daily_pl(
                    scores, data, pair_info, **a2)
                results.columns = [ind]
                output_results = output_results.join(results, how='outer')
                ind += 1

                temp_params = {}
                temp_params.update(a1)
                temp_params.update(a2)

                output_params[ind] = temp_params

        # write
        with open('C:\Users\Mitchell\Desktop\stat_arb_params.json', 'w') as outfile:
            json.dump(output_params, outfile)

        return output_results

    def _get_date_iterator(self):
        """
        Bookend dates for start training, start test (~eval date)
        and end test sets.
        """
        all_dates = self.datahandler.get_all_dates()
        all_dates = all_dates[all_dates >= dt.datetime(2002, 12, 1)]
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


def make_arg_iter(variants):
    return [{x: y for x, y in zip(variants.keys(), vals)}
        for vals in itertools.product(*variants.values())]


if __name__ == '__main__':

    strategy = StatArbStrategy()
    strategy.start()

    from gearbox import to_desk
    to_desk(strategy.results, 'statarb_run')

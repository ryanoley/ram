import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy
from ram.data.dh_mongo import DataHandlerMongoDb
from ram.utils.statistics import create_strategy_report

from ram.strategy.statarb.pairselector.pairs1 import PairsStrategy1
from ram.strategy.statarb.constructor.constructor import PortfolioConstructor


class StatArbStrategy(Strategy):

    def __init__(self):
        self.datahandler = DataHandlerMongoDb()
        self.pair = PairsStrategy1()
        self.portfolio = PortfolioConstructor()

    def start(self):

        dates = self._get_date_iterator()

        for t_start, cut_date, t_end in dates:

            data = self.datahandler.get_filtered_univ_data(
                univ_size=100,
                features=['Close'],
                start_date=t_start,
                filter_date=cut_date,
                end_date=t_end,
                filter_column='AvgDolVol',
                filter_bad_ids=True)

            test_z_scores = self.pair.get_best_pairs(data, cut_date, window=60)

            plexp = self.portfolio.get_daily_pl(pairs)

            self._collect_output(plexp)

    def get_results(self):
        pass

    def start_live(self):
        pass

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


if __name__ == '__main__':

    import pdb; pdb.set_trace()
    strategy = StatArbStrategy()
    strategy.start()

    path = '/Users/mitchellsuter/Desktop/'
    name = 'StatArbStrategy'
    create_strategy_report(strategy.get_results(), name, path)

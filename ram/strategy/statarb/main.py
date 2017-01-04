import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy

from ram.strategy.statarb.pairselector.pairs1 import PairsStrategy1
from ram.strategy.statarb.pairselector.pairs2 import PairsStrategy2
from ram.strategy.statarb.constructor.constructor import PortfolioConstructor


class StatArbStrategy(Strategy):

    def __init__(self, pairselector):
        super(StatArbStrategy, self).__init__()
        if pairselector == 'pairs1':
            self.pairselector = PairsStrategy1()
        else:
            self.pairselector = PairsStrategy2()

    def get_iter_index(self):
        return range(len(self._get_date_iterator()))

    def run_index(self, index):

        t_start, cut_date, t_end = self._get_date_iterator()[index]

        self.constructor = PortfolioConstructor()

        data, trade_data = self._get_data(
            t_start, cut_date, t_end, univ_size=100)

        z_scores = self.pairselector.get_best_pairs(
            data, cut_date, window=20)

        return self.constructor.get_daily_pl(z_scores, trade_data)

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

        data = self.datahandler.get_filtered_univ_data(
            univ_size=univ_size,
            features=['RClose', 'Close',
                      'RCashDividend', 'SplitFactor'],
            start_date=start_date,
            end_date=end_date,
            filter_date=adj_filter_date)
        # Adjustments to data
        data['SplitMultiplier'] = data.SplitFactor.pct_change().fillna(0) + 1
        data = data.rename(columns={'RCashDividend': 'Dividend',
                                    'RClose': 'Close',
                                    'Close': 'ADJClose'})
        # Adjustment for naming conventions
        data.SecCode = data.SecCode.astype(str)

        # Trading data - After filter date
        trade_data = data[['Date', 'SecCode', 'Close',
                           'SplitMultiplier', 'Dividend']].copy()
        trade_data.Dividend = trade_data.Dividend.fillna(0)
        trade_data = trade_data[trade_data.Date >= filter_date]

        data = data[['SecCode', 'Date', 'ADJClose']]

        return data, trade_data


if __name__ == '__main__':

    strategy = StatArbStrategy('pairs2')
    strategy.start()

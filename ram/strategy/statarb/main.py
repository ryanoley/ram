import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy
from ram.data.dh_sql import DataHandlerSQL
from ram.utils.statistics import create_strategy_report

from ram.strategy.statarb.pairselector.pairs1 import PairsStrategy1
from ram.strategy.statarb.constructor.constructor import PortfolioConstructor


class StatArbStrategy(Strategy):

    def __init__(self):
        self.datahandler = DataHandlerSQL()
        self.pairselector = PairsStrategy1()
        self.constructor = PortfolioConstructor()

    def start(self):

        dates = self._get_date_iterator()

        output = pd.DataFrame(columns=['Ret'])

        for t_start, cut_date, t_end in dates:

            data, trade_data = self._get_data(
                t_start, cut_date, t_end, univ_size=100)

            z_scores = self.pairselector.get_best_pairs(
                data, cut_date, window=20)

            plexp = self.constructor.get_daily_pl(z_scores, trade_data)

            output = output.add(plexp, fill_value=0)

        self.results = output

    def get_results(self):
        return self.results

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

    def _get_data(self, start_date, filter_date, end_date, univ_size):
        """
        Makes the appropriate period's data.
        """
        # Adjust date by one day for filter date
        adj_filter_date = filter_date - dt.timedelta(days=1)

        data = self.datahandler.get_filtered_univ_data(
            univ_size=univ_size,
            features=['Close_', 'ADJClose_', 'CashDividend', 'SplitFactor'],
            start_date=start_date,
            end_date=end_date,
            filter_date=adj_filter_date)
        # Adjustments to data
        data['SplitMultiplier'] = data.SplitFactor.pct_change().fillna(0) + 1
        data = data.rename(columns={'CashDividend': 'Dividend',
                                    'Close_': 'Close',
                                    'ADJClose_': 'ADJClose'})
        # Adjustment for naming conventions
        data.ID = data.ID.astype(str)

        # Trading data - After filter date
        trade_data = data[['Date', 'ID', 'Close',
                           'SplitMultiplier', 'Dividend']].copy()
        trade_data.Dividend = trade_data.Dividend.fillna(0)
        trade_data = trade_data[trade_data.Date >= filter_date]

        data = data[['ID', 'Date', 'ADJClose']]

        return data, trade_data


if __name__ == '__main__':

    strategy = StatArbStrategy()
    strategy.start()
    path = '/Users/mitchellsuter/Desktop/'
    name = 'StatArbStrategy'
    create_strategy_report(strategy.get_results(), name, path)

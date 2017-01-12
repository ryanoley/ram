import os
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy

from sklearn.linear_model import LinearRegression
from ram.utils.statistics import create_strategy_report


class BirdsStrategy(Strategy):

    def get_iter_index(self):
        return range(len(self._get_date_iterator()))

    def run_index(self, index):

        t_start, cut_date, t_end = self._get_date_iterator()[index]

        train_data, test_data, features, response_labels, ret_labels = \
            self._get_data(t_start, cut_date, t_end, univ_size=500)

        cl = LinearRegression()

        cl.fit(X=train_data[features], y=train_data[response_labels])

        preds = pd.DataFrame(columns=response_labels, index=test_data.index)
        preds[:] = cl.predict(test_data[features])
        preds['Date'] = test_data.Date

        pred_thresh = np.arange(.5, .55, .01)

        uniq_dates = np.unique(test_data.Date)

        col_labels = []
        for p in pred_thresh:
            for r in ret_labels:
                col_labels.append('{0}_{1}'.format(r, p))

        outdata = pd.DataFrame(columns=col_labels, index=uniq_dates)

        univ_proportion = 0.10

        for pt in pred_thresh:
            for yl, rl in zip(response_labels, ret_labels):
                for d in uniq_dates:

                    ests = preds.loc[preds.Date == d, yl]
                    rets = test_data.loc[test_data.Date == d, rl]

                    pos_per_side = int(len(ests) * univ_proportion)

                    if (ests > pt).sum() > pos_per_side:
                        ## Will take everything and scale accordingly
                        retL = rets[ests > pt].mean()
                        posL = pos_per_side
                    else:
                        ## Max position size enforced
                        retL = (rets[ests > pt]).sum() / pos_per_side
                        posL = (ests > pt).sum()

                    if (ests < (1-pt)).sum() > pos_per_side:
                        retS = rets[ests < (1-pt)].mean()
                        posS = pos_per_side
                    else:
                        ## Max position size enforced
                        retL = (rets[ests < (1-pt)]).sum() / pos_per_side
                        posS = (ests < (1-pt)).sum()

                    col = '{0}_{1}'.format(rl, pt)

                    outdata.loc[d, col] = (posS * retL - posL * retS) / pos_per_side

        return outdata

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
        n_days_forward = 4

        # Adjust date by one day for filter date
        adj_filter_date = filter_date - dt.timedelta(days=1)

        features1 = ['AvgDolVol', 'MarketCap', 'LAG62_MarketCap', 'Close']
        features1 += ['LEAD{0}_Close'.format(x) for x in range(1, n_days_forward)]

        features2 = ['PRMA{0}_Close'.format(x) for x in [10, 20, 30, 40, 50]]
        features2 += ['VOL{0}_Close'.format(x) for x in [10, 20, 30, 40, 50]]
        features2 += ['DISCOUNT{0}_Close'.format(x) for x in [63, 126, 252]]
        features2 += ['MFI{0}_Close'.format(x) for x in [20, 40, 60]]

        data = self.datahandler.get_filtered_univ_data(
            univ_size=univ_size,
            features=features1 + features2,
            start_date=start_date,
            end_date=end_date,
            filter_date=adj_filter_date)

        data['MarketCapGrowth'] = data.MarketCap / data.LAG62_MarketCap - 1
        data = data.drop('MarketCap', axis=1)
        data = data.drop('LAG62_MarketCap', axis=1)

        # Make returns
        ret_labels = []
        for i in range(1, n_days_forward):
            data['Ret{0}'.format(i)] = data['LEAD{0}_Close'.format(i)] / \
                data.Close - 1
            data = data.drop('LEAD{0}_Close'.format(i), axis=1)
            ret_labels.append('Ret{0}'.format(i))

        data = data.dropna()

        trade_data = data[data.Date >= filter_date].copy()

        # Drop days when creating training data
        fdates = np.unique(list(set(data.Date) - set(trade_data.Date)))
        data = data.loc[data.Date.isin(fdates[:-20])]
        data = data.reset_index(drop=True)

        response_labels = []
        # Top 50% returns get 1s
        for i in range(1, n_days_forward):
            data['y{0}'.format(i)] = 0
            response_labels.append('y{0}'.format(i))
            for d in np.unique(data.Date):
                rets = data.loc[data.Date == d, 'Ret{0}'.format(i)]
                cut_n = len(rets) / 2
                data.loc[rets.index, 'y{0}'.format(i)] = rets.rank() > cut_n

        return data, trade_data, features2, response_labels, ret_labels


if __name__ == '__main__':

    strategy = BirdsStrategy()

    strategy.start()

    results = strategy.results

    OUTDIR = os.path.join(os.getenv('DATA'), 'ram', 'strategy_output')
    # Write results
    results.to_csv(os.path.join(OUTDIR, 'BirdsStrategy2_returns.csv'))

import os
import sys
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.base import Strategy

from sklearn import preprocessing
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import BaggingClassifier

from sklearn.ensemble import RandomForestClassifier

from ram.utils.statistics import create_strategy_report


class BirdsStrategy(Strategy):

    def get_iter_index(self):
        return range(len(self._get_date_iterator()))

    def run_index(self, index):

        t_start, test_start_date, t_end = self._get_date_iterator()[index]

        if index < 30:
            return pd.DataFrame(index=[t_start])

        import pdb; pdb.set_trace()
        train_data, test_data, features = \
            self._get_data(t_start, test_start_date, t_end, univ_size=500)

        # Long and short estimates
        train_preds = pd.DataFrame(index=train_data.index)
        train_preds = (train_preds.copy(), train_preds.copy())
        test_preds = pd.DataFrame(index=test_data.index)
        test_preds = (test_preds.copy(), test_preds.copy())

        train_preds, test_preds = self._logistic_regression(
            train_preds, train_data, test_preds, test_data, features)

        train_preds, test_preds = self._random_forest(
            train_preds, train_data, test_preds, test_data, features)

        weights = [1, 1]
        n_pos = 50

        daily_pl_train = self._daily_pl(weights, n_pos, train_preds, train_data)
        daily_pl_test = self._daily_pl(weights, n_pos, test_preds, test_data)
        sys.exit()
        return daily_pl_test

    def _daily_pl(self, weights, n_pos, preds, data):

        univ_size = data.groupby('Date')['ReturnDay1'].count().mode()[0]
        perc = n_pos / float(univ_size)

        data['long_pred'] = preds[0].multiply(weights).sum(axis=1)
        data['short_pred'] = preds[1].multiply(weights).sum(axis=1)

        longs = data.groupby('Date')['long_pred'].quantile(1-perc).reset_index()
        longs.columns = ['Date', 'long_thresh']
        shorts = data.groupby('Date')['short_pred'].quantile(1-perc).reset_index()
        shorts.columns = ['Date', 'short_thresh']
        data = data.merge(longs).merge(shorts)

        longs = data[data.long_pred >= data.long_thresh]
        shorts = data[data.short_pred >= data.short_thresh]

        day1 = longs.groupby('T_1')['ReturnDay1'].mean() - shorts.groupby('T_1')['ReturnDay1'].mean()
        day2 = longs.groupby('T_2')['ReturnDay2'].mean() - shorts.groupby('T_2')['ReturnDay2'].mean()
        day3 = longs.groupby('T_3')['ReturnDay3'].mean() - shorts.groupby('T_3')['ReturnDay3'].mean()

        return pd.DataFrame(day1).join(day2, how='outer').join(
            day3, how='outer').fillna(0).mean(axis=1)

    ###########################################################################

    def _logistic_regression(self, train_preds, train_data,
                             test_preds, test_data, features):

        cl = BaggingClassifier(LogisticRegression(), n_jobs=4,
                               max_samples=0.8, max_features=.8,
                               random_state=123)
        cl.fit(X=train_data[features], y=train_data.Signal)

        short_ind = np.where(cl.classes_ == -1)[0][0]
        long_ind = np.where(cl.classes_ == 1)[0][0]

        train_p = cl.predict_proba(train_data[features])
        test_p = cl.predict_proba(test_data[features])

        train_preds[0]['Log_01'] = train_p[:, long_ind]
        train_preds[1]['Log_01'] = train_p[:, short_ind]

        test_preds[0]['Log_01'] = test_p[:, long_ind]
        test_preds[1]['Log_01'] = test_p[:, short_ind]

        return train_preds, test_preds

    def _random_forest(self, train_preds, train_data,
                       test_preds, test_data, features):

        cl = RandomForestClassifier(n_estimators=10, min_samples_leaf=100,
                                    n_jobs=4, random_state=123)
        cl.fit(X=train_data[features], y=train_data.Signal)

        short_ind = np.where(cl.classes_ == -1)[0][0]
        long_ind = np.where(cl.classes_ == 1)[0][0]

        train_p = cl.predict_proba(train_data[features])
        test_p = cl.predict_proba(test_data[features])

        train_preds[0]['Forest_01'] = train_p[:, long_ind]
        train_preds[1]['Forest_01'] = train_p[:, short_ind]

        test_preds[0]['Forest_01'] = test_p[:, long_ind]
        test_preds[1]['Forest_01'] = test_p[:, short_ind]

        return train_preds, test_preds

    ###########################################################################

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

    def _get_data(self, start_date, start_test_date, end_date, univ_size):

        #######################################################################
        #######################################################################
        ## Features

        # ASSUMING THREE DAY RETURN

        basic_features = ['Close', 'LEAD1_Close', 'LEAD2_Close', 'LEAD3_Close']

        # Features for training
        train_features = ['PRMA{0}_Close'.format(x) for x in [10, 20, 30, 40, 50]]
        train_features += ['VOL{0}_Close'.format(x) for x in [10, 20, 30, 40, 50]]
        train_features += ['DISCOUNT{0}_Close'.format(x) for x in [63, 126, 252]]
        train_features += ['MFI{0}_Close'.format(x) for x in [20, 40, 60]]

        #######################################################################
        #######################################################################

        # Adjust date by one day for filter date
        adj_filter_date = start_test_date - dt.timedelta(days=1)
        adj_end_date = end_date + dt.timedelta(days=6)

        data = self.datahandler.get_filtered_univ_data(
            univ_size=univ_size,
            features=basic_features + train_features,
            start_date=start_date,
            end_date=adj_end_date,
            filter_date=adj_filter_date)

        data = data.dropna()

        # Returns
        data['Return'] = data.LEAD3_Close / data.Close - 1
        data['ReturnDay1'] = data.LEAD1_Close / data.Close - 1
        data['ReturnDay2'] = (data.ReturnDay1 + 1) * data.LEAD2_Close / data.LEAD1_Close - 1
        data['ReturnDay3'] = (data.ReturnDay1 + 1) * (data.ReturnDay2 + 1) * data.LEAD3_Close / data.LEAD2_Close - 1
        data = data.drop('Close', axis=1)
        data = data.drop('LEAD1_Close', axis=1)
        data = data.drop('LEAD2_Close', axis=1)
        data = data.drop('LEAD3_Close', axis=1)

        # Date map
        date_map = pd.DataFrame({'Date': data.Date.unique()})
        date_map['T_1'] = date_map.Date.shift(-1)
        date_map['T_2'] = date_map.Date.shift(-2)
        date_map['T_3'] = date_map.Date.shift(-3)

        # Train/Test data - drop overlapping days from training, assuming
        #  3 day return
        actual_start_date = data.Date[data.Date >= start_test_date].min()
        last_train_date = data.Date[data.Date < actual_start_date].unique()[-4]
        train_data = data[data.Date <= last_train_date].copy()
        test_data = data[data.Date >= start_test_date].copy()

        # Scale data for other downstream algos
        scaler = preprocessing.StandardScaler().fit(
            train_data.loc[:, train_features])

        train_data.loc[:, train_features] = scaler.transform(
            train_data.loc[:, train_features])
        test_data.loc[:, train_features] = scaler.transform(
            test_data.loc[:, train_features])

        # Create binaries for classification
        shorts = train_data.groupby('Date')['Return'].quantile(.25).reset_index()
        shorts.columns = ['Date', 'ShortThresh']
        longs = train_data.groupby('Date')['Return'].quantile(.75).reset_index()
        longs.columns = ['Date', 'LongThresh']
        train_data = train_data.merge(longs).merge(shorts)
        train_data['Signal'] = np.where(
            train_data.Return >= train_data.LongThresh, 1,
            np.where(train_data.Return <= train_data.ShortThresh, -1, 0))
        train_data = train_data.drop(['ShortThresh', 'LongThresh'], axis=1)

        # Adjust test data
        test_data = test_data[test_data < end_date]
        test_data = test_data.merge(date_map)
        train_data = train_data.merge(date_map)

        return train_data, test_data, train_features


if __name__ == '__main__':

    strategy = BirdsStrategy()

    strategy.start()

    results = strategy.results

    OUTDIR = os.path.join(os.getenv('DATA'), 'ram', 'strategy_output')
    # Write results
    results.to_csv(os.path.join(OUTDIR, 'BirdsStrategy2_returns.csv'))

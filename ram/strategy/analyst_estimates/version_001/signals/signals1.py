import numpy as np

from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor

from ram.strategy.analyst_estimates import config

NJOBS = config.SKLEARN_NJOBS


class SignalModel1(object):

    def __init__(self):
        self.preds_data = {}

    def get_args(self):
        return {
            'max_features': [0.80],
            'n_estimators': [125],
            'min_samples_leaf': [25],
            'drop_ibes': [False]
        }

    def rf_signals(self, data_container, max_features, min_samples_leaf,
                   n_estimators, drop_ibes, seed=None):

        features = data_container.features
        if drop_ibes:
            features = list(set(features) -
                            set(['prtgt_est_change', 'prtgt_discount',
                                 'prtgt_disc_change','anr_rec_change',
                                 'RECMEAN']))

        train_data = data_container.train_data
        test_data = data_container.test_data

        for e in range(1, data_container._entry_window):
            e_train = train_data[train_data['T'] == e].copy()
            e_test = test_data[test_data['T'] == e].copy()

            clf = RandomForestRegressor(n_estimators=n_estimators,
                                       min_samples_leaf=min_samples_leaf,
                                       max_features=max_features,
                                       n_jobs=NJOBS)
            if seed is not None:
                clf.random_state = seed
            clf.fit(X=e_train[features], y=e_train['Response'])

            preds = clf.predict(e_test[features])
            e_test['preds'] = preds
            self.preds_data[e + 1] = e_test[['SecCode', 'Date', 'preds']]
        return

    def lr_signals(self, data_container):

        train_data = data_container.train_data
        test_data = data_container.test_data
        features = data_container.features

        lr = LinearRegression()

        lr.fit(X=train_data[features], y=train_data['Response'])
        test_data['preds'] = lr.predict(test_data[features])
        self.preds_data = test_data[['SecCode', 'Date', 'preds']].copy()
        return

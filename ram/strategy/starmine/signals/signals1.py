import numpy as np

from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression

from ram import config

NJOBS = config.SKLEARN_NJOBS


class SignalModel1(object):

    def __init__(self):
        pass

    def get_args(self):
        return {
            'max_features': [0.75],
            'n_estimators': [100],
            'min_samples_leaf': [25]
        }


    def rf_signals2(self, data_container, max_features,
                   min_samples_leaf, n_estimators):

        train_data = data_container.train_data
        test_data = data_container.test_data
        features = data_container.features

        clf = RandomForestClassifier(n_estimators=n_estimators,
                                     min_samples_leaf=min_samples_leaf,
                                     max_features=max_features,
                                     n_jobs=NJOBS)

        clf.fit(X=train_data[features], y=train_data['Response'])
        preds = clf.predict_proba(test_data[features])
        test_data.loc[:, 'preds'] = _get_preds(clf, preds)
        self.preds_data = test_data[['SecCode', 'Date', 'preds']].copy()
        return


    def rf_signals(self, data_container, max_features,
                   min_samples_leaf, n_estimators):

        train_data = data_container.train_data
        test_data = data_container.test_data
        features = data_container.features

        clf = RandomForestRegressor(n_estimators=n_estimators,
                                   min_samples_leaf=min_samples_leaf,
                                   max_features=max_features,
                                   n_jobs=NJOBS)

        clf.fit(X=train_data[features],
                y=train_data['Response'])

        preds = clf.predict(test_data[features])
        test_data['preds'] = preds
        self.preds_data = test_data[['SecCode', 'Date', 'preds']].copy()
        return


    def lr_signals(self, data_container):

        train_data = data_container.train_data
        test_data = data_container.test_data
        features = data_container.features

        lr = LinearRegression()

        lr.fit(X=train_data[features],
                y=train_data['Response'])
        test_data['preds'] = lr.predict(test_data[features])
        self.preds_data = test_data[['SecCode', 'Date', 'preds']].copy()
        return



def _get_preds(classifier, preds):
    if -1 in classifier.classes_:
        short_ind = np.where(classifier.classes_ == -1)[0][0]
        long_ind = np.where(classifier.classes_ == 1)[0][0]
        return preds[:, long_ind] - preds[:, short_ind]
    else:
        long_ind = np.where(classifier.classes_ == 1)[0][0]
        return preds[:, long_ind]
    

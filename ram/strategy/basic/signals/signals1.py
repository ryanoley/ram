import numpy as np

from sklearn.ensemble import ExtraTreesClassifier

from ram import config


class SignalModel1(object):

    def __init__(self, njobs=config.SKLEARN_NJOBS):
        self.NJOBS = njobs

    def get_args(self):
        return {
            'tree_params': [
                {'min_samples_leaf': 20,
                 'n_estimators': 100,
                 'max_features': 0.5,
                },
                {'min_samples_leaf': 20,
                 'n_estimators': 100,
                 'max_features': 0.8,
                }
            ],
            'drop_extremes': [True],
            'drop_market_variables': [True]
        }

    def generate_signals(self,
                         data_container,
                         tree_params,
                         drop_extremes,
                         drop_market_variables):

        train_data = data_container.train_data
        test_data = data_container.test_data
        features = data_container.features

        if drop_extremes:
            features = [x for x in features if x.find('extreme') == -1]
        if drop_market_variables:
            features = [x for x in features if x.find('Mkt_') == -1]

        clf = ExtraTreesClassifier(n_jobs=self.NJOBS, **tree_params)

        clf.fit(X=train_data[features],
                y=train_data['Response'])

        # Get indexes of long and short sides
        short_ind = np.where(clf.classes_ == -1)[0][0]
        long_ind = np.where(clf.classes_ == 1)[0][0]

        # Get test predictions to create portfolios on:
        #    Long Prediction - Short Prediction
        preds = clf.predict_proba(test_data[features])

        test_data.loc[:, 'preds'] = \
            preds[:, long_ind] - preds[:, short_ind]
        self.preds_data = test_data[['SecCode', 'Date', 'preds']].copy()

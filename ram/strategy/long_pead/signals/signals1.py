import numpy as np

from sklearn.ensemble import VotingClassifier
from sklearn.ensemble import BaggingClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression

from ram import config


class SignalModel1(object):

    def __init__(self, njobs=config.SKLEARN_NJOBS):
        self.NJOBS = njobs

    def get_args(self):
        return {
            'min_samples_leaf': [50, 140, 300],
            'n_estimators': [100],
            'max_features': [None, 'log2'],
            'drop_accounting': [False],
            'drop_extremes': [True],
            'drop_market_variables': [True, False],
        }

    def generate_signals(self, data_container, n_estimators,
                         max_features,
                         min_samples_leaf,
                         drop_accounting, drop_extremes,
                         drop_market_variables):
        train_data = data_container.train_data
        test_data = data_container.test_data
        features = data_container.features
        if drop_accounting:
            accounting_vars = [
                'NETINCOMEQ', 'NETINCOMETTM', 'SALESQ', 'SALESTTM',
                'ASSETS', 'CASHEV', 'FCFMARKETCAP', 'NETINCOMEGROWTHQ',
                'NETINCOMEGROWTHTTM', 'OPERATINGINCOMEGROWTHQ',
                'OPERATINGINCOMEGROWTHTTM', 'EBITGROWTHQ', 'EBITGROWTHTTM',
                'SALESGROWTHQ', 'SALESGROWTHTTM', 'FREECASHFLOWGROWTHQ',
                'FREECASHFLOWGROWTHTTM', 'GROSSPROFASSET', 'GROSSMARGINTTM',
                'EBITDAMARGIN', 'PE']
            features = [x for x in features if x not in accounting_vars]
        if drop_extremes:
            features = [x for x in features if x.find('extreme') == -1]
        if drop_market_variables:
            features = [x for x in features if x.find('Mkt_') == -1]

        clf = ExtraTreesClassifier(n_estimators=n_estimators,
                                   min_samples_leaf=min_samples_leaf,
                                   max_features=max_features,
                                   n_jobs=self.NJOBS)

        clf.fit(X=train_data[features],
                y=train_data['Response'])

        # Get indexes of long and short sides
        short_ind = np.where(clf.classes_ == -1)[0][0]
        long_ind = np.where(clf.classes_ == 1)[0][0]

        # Get test predictions to create portfolios on:
        #    Long Prediction - Short Prediction
        preds = clf.predict_proba(test_data[features])
        data_container.test_data['preds'] = \
            preds[:, long_ind] - preds[:, short_ind]

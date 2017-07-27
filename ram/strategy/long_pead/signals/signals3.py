import numpy as np

from sklearn.ensemble import VotingClassifier
from sklearn.ensemble import BaggingClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression

from ram import config

NJOBS = config.SKLEARN_NJOBS


class SignalModel1(object):

    def __init__(self):
        pass

    def get_args(self):
        return {
            'max_features': ['log2', 'sqrt'],
            'n_estimators': [40, 80],
            'min_samples_leaf': [30, 80, 140]
        }

    def generate_signals(self, data_container, max_features,
                         min_samples_leaf, n_estimators):

        train_data = data_container.train_data
        test_data = data_container.test_data
        features = data_container.features

        clf = ExtraTreesClassifier(n_estimators=n_estimators,
                                   min_samples_leaf=min_samples_leaf,
                                   max_features=max_features,
                                   n_jobs=NJOBS)

        # elif model_type == 1:
        #     clf = BaggingClassifier(LogisticRegression(), n_estimators=50,
        #                             max_samples=0.7, max_features=0.6,
        #                             n_jobs=NJOBS)
        # 
        # elif model_type == 2:
        #     clf1 = ExtraTreesClassifier(n_estimators=50, n_jobs=NJOBS,
        #                                 min_samples_leaf=60,
        #                                 max_features=tree_max_features)
        # 
        #     clf2 = BaggingClassifier(LogisticRegression(), n_estimators=50,
        #                              max_samples=0.7, max_features=0.6,
        #                              n_jobs=NJOBS)
        # 
        #     models = [('et', clf1), ('lc', clf2)]
        # 
        #     clf = VotingClassifier(estimators=models, voting='soft')

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

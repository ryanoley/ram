import numpy as np
import datetime as dt

from ram.strategy.statarb.utils import make_arg_iter

from sklearn.ensemble import ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression


from ram.strategy.statarb.abstract.signal_generator import BaseSignalGenerator


class SignalModel(BaseSignalGenerator):

    def __init__(self):
        self.skl_model = None

    def get_args(self):
        return {
            'model': [
                {'type': 'tree', 'min_samples_leaf': 500, 'max_features': 0.5},
                {'type': 'tree', 'min_samples_leaf': 500, 'max_features': 0.8},
                # {'type': 'tree', 'min_samples_leaf': 2000, 'max_features': 0.5},
                # {'type': 'tree', 'min_samples_leaf': 2000, 'max_features': 0.8},
                {'type': 'reg'},
            ]
        }

    def set_args(self, model):
        if model['type'] == 'tree':
            self.skl_model = ExtraTreesClassifier(n_jobs=-1,
                                                  random_state=123,
                                                  min_samples_leaf=model['min_samples_leaf'],
                                                  max_features=model['max_features'],
                                                  n_estimators=30)
        else:
            self.skl_model = LogisticRegression()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def set_features(self, features):
        self._features = np.sort(features)

    def set_train_data(self, train_data):
        self._train_data = train_data

    def set_train_responses(self, train_responses):
        self._train_responses = train_responses

    def set_test_data(self, test_data):
        self._test_data = test_data

    # ~~~~~~ Model related functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def fit_model(self):
        self.skl_model.fit(X=self._train_data[self._features],
                           y=self._train_responses)

    def get_model(self):
        return self.skl_model

    def set_model(self, model):
        self.skl_model = model

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_signals(self):
        output = self._test_data[['SecCode', 'Date', 'keep_inds']].copy()
        # Here the features that are coming through have nan values
        # Where were they handled before where they weren't showing up?
        if hasattr(self.skl_model, 'predict_proba'):
            preds = self.skl_model.predict_proba(
                self._test_data[self._features])
            output.loc[:, 'preds'] = _get_preds(self.skl_model, preds)
        else:
            output.loc[:, 'preds'] = self.skl_model.predict(
                self._test_data[self._features])
        output = output[output.keep_inds]
        output = output.pivot(index='Date', columns='SecCode',
                              values='preds')
        output = output.rank(axis=1, pct=True)
        output = output.unstack().reset_index().dropna()
        output.columns = ['SecCode', 'Date', 'Signal']
        return output


def _get_preds(classifier, preds):
    if -1 in classifier.classes_:
        short_ind = np.where(classifier.classes_ == -1)[0][0]
        long_ind = np.where(classifier.classes_ == 1)[0][0]
        return preds[:, long_ind] - preds[:, short_ind]
    else:
        long_ind = np.where(classifier.classes_ == 1)[0][0]
        return preds[:, long_ind]

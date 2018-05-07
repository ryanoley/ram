import numpy as np
import datetime as dt

from ram.strategy.statarb.utils import make_arg_iter

from sklearn.ensemble import ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression


from ram.strategy.statarb.abstract.signal_generator import BaseSignalGenerator
from ram.strategy.statarb.version_004.data.data_container import accounting_features, starmine_features


class SignalModel(BaseSignalGenerator):

    def __init__(self):
        self.skl_model = None

    def get_args(self):
        return {
            'model': [
                {'type': 'tree', 'min_samples_leaf': 500,
                 'max_features': 0.8, 'n_estimators': 30},
                {'type': 'reg'},
            ],
            'technical_feature_set': [1],
            'technical_only': [True, False]
        }

    def set_args(self, model, technical_feature_set, technical_only):
        if model['type'] == 'tree':
            self.skl_model = ExtraTreesClassifier(
                n_jobs=-1, random_state=123,
                min_samples_leaf=model['min_samples_leaf'],
                max_features=model['max_features'],
                n_estimators=model['n_estimators'])
        else:
            self.skl_model = LogisticRegression()
        self._technical_feature_set = technical_feature_set
        self._technical_only = technical_only

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def set_features(self, features):
        self._features = np.sort(features)

    def _get_features(self):
        non_tech = accounting_features + starmine_features
        if self._technical_feature_set == 1:
            features = [x for x in self._features if x not in non_tech]
        elif self._technical_feature_set == 2:
            # LONGER
            features = ['boll2_40', 'boll3_40', 'boll_40', 'disc_40',
                        'mfi_30', 'prma_2_40', 'prma_3_40', 'prma_4_40',
                        'ret_40d', 'rsi_30', 'vol_40']
        else:
            # SHORTER
            features = ['boll2_10', 'boll3_10', 'boll_10', 'disc_20',
                        'mfi_15', 'prma_2_20', 'prma_3_20', 'prma_4_20',
                        'ret_10d', 'rsi_15', 'vol_10']
        # Add in non-technical?
        if not self._technical_only:
            features += non_tech
        return features

    def set_train_data(self, train_data):
        self._train_data = train_data

    def set_train_responses(self, train_responses):
        self._train_responses = train_responses

    def set_test_data(self, test_data):
        self._test_data = test_data

    # ~~~~~~ Model related functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def fit_model(self):
        features = self._get_features()
        self.skl_model.fit(X=self._train_data[features],
                           y=self._train_responses)

    def get_model(self):
        return self.skl_model

    def set_model(self, model):
        self.skl_model = model

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_signals(self):
        features = self._get_features()
        output = self._test_data[['SecCode', 'Date', 'keep_inds']].copy()
        # Here the features that are coming through have nan values
        # Where were they handled before where they weren't showing up?
        if hasattr(self.skl_model, 'predict_proba'):
            preds = self.skl_model.predict_proba(
                self._test_data[features])
            output.loc[:, 'preds'] = _get_preds(self.skl_model, preds)
        else:
            output.loc[:, 'preds'] = self.skl_model.predict(
                self._test_data[features])
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

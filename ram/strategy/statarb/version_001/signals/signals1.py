import numpy as np

from sklearn.ensemble import ExtraTreesClassifier

from ram.strategy.statarb.abstract.signal_generator import BaseSignalGenerator
from ram.strategy.statarb.version_001.data.data_container_pairs import \
    accounting_features, starmine_features, ibes_features


class SignalModel1(BaseSignalGenerator):

    def __init__(self):
        self.skl_model = ExtraTreesClassifier(n_jobs=-1,
                                              random_state=123)

    def get_args(self):
        return {
            'model_params': [
                {'min_samples_leaf': 200,
                 'n_estimators': 100,
                 'max_features': 0.8},
                {'min_samples_leaf': 50,
                 'n_estimators': 30,
                 'max_features': 0.6},
            ],
            'drop_ibes': [True],
            'drop_accounting': [True],
            'drop_starmine': [True, False],
            'drop_market_variables': ['constrained']
        }

    def set_data_args(self,
                      data_container,
                      model_params,
                      drop_ibes,
                      drop_accounting,
                      drop_starmine,
                      drop_market_variables):
        # MODEL PARAMS
        self.skl_model.set_params(**model_params)

        # FEATURES
        features = data_container.get_training_feature_names()

        if drop_ibes:
            features = [x for x in features if x not in ibes_features]

        if drop_accounting:
            features = [x for x in features if x not in accounting_features]

        if drop_starmine:
            features = [x for x in features if x not in starmine_features]

        if drop_market_variables == 'constrained':
            features = [x for x in features if x.find('MKT_') == -1]
            features.extend(['MKT_VIX_AdjClose', 'MKT_VIX_PRMA10',
                             'MKT_SP500Index_VOL10', 'MKT_SP500Index_PRMA10',
                             'MKT_SP500Index_BOLL20'])
        elif drop_market_variables:
            features = [x for x in features if x.find('MKT_') == -1]

        # A bit of security if variables come through with different
        features.sort()
        self._features = features
        self._train_data = data_container.get_training_data()
        self._train_responses = data_container.get_training_responses()
        self._test_data = data_container.get_test_data()

    # ~~~~~~ Model related functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def fit_model(self):
        self.skl_model.fit(X=self._train_data[self._features],
                           y=self._train_responses['Response'])

    def get_model(self):
        return self.skl_model

    def set_model(self, model):
        self.skl_model = model

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_signals(self):
        output = self._test_data[['SecCode', 'Date']]
        preds = self.skl_model.predict_proba(self._test_data[self._features])
        output['preds'] = _get_preds(self.skl_model, preds)
        return output


def _get_preds(classifier, preds):
    if -1 in classifier.classes_:
        short_ind = np.where(classifier.classes_ == -1)[0][0]
        long_ind = np.where(classifier.classes_ == 1)[0][0]
        return preds[:, long_ind] - preds[:, short_ind]
    else:
        long_ind = np.where(classifier.classes_ == 1)[0][0]
        return preds[:, long_ind]

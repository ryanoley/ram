import numpy as np

from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier
from sklearn.linear_model import LinearRegression

from ram.strategy.statarb.abstract.signal_generator import BaseSignalGenerator
from ram.strategy.statarb.version_001.data.data_container_pairs import \
    accounting_features, starmine_features, ibes_features


class SignalModel1(BaseSignalGenerator):

    def __init__(self):
        self.skl_model = None

    def get_args(self):
        return {
            'model_params': [
                {'model_type': 'random_forest',
                 'min_samples_leaf': 200,
                 'n_estimators': 100,
                 'max_features': 0.7},

                {'model_type': 'linear_model'},

                {'model_type': 'extra_trees',
                 'min_samples_leaf': 200,
                 'n_estimators': 100,
                 'max_features': 0.7},
            ],

            'drop_ibes': [True, False],
            'drop_accounting': [True, False],
            'drop_starmine': [False],
            'drop_market_variables': ['constrained']
        }

    def set_args(self,
                 model_params,
                 drop_ibes,
                 drop_accounting,
                 drop_starmine,
                 drop_market_variables):

        model_type = model_params.pop('model_type')
        if model_type == 'extra_trees':
            self.skl_model = ExtraTreesClassifier(n_jobs=-1,
                                                  random_state=123)
        if model_type == 'random_forest':
            self.skl_model = RandomForestClassifier(n_jobs=-1,
                                                    random_state=123)
        if model_type == 'linear_model':
            self.skl_model = LinearRegression()


        self._model_params = model_params
        self._drop_ibes = drop_ibes
        self._drop_accounting = drop_accounting
        self._drop_starmine = drop_starmine
        self._drop_market_variables = drop_market_variables

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def set_features(self, features):
        self._features = features

    def set_train_data(self, train_data):
        self._train_data = train_data

    def set_train_responses(self, train_responses):
        self._train_responses = train_responses

    def set_test_data(self, test_data):
        self._test_data = test_data

    # ~~~~~~ Model related functionality ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _process_args(self):
        # MODEL PARAMS
        self.skl_model.set_params(**self._model_params)

        # FEATURE PROCSSING
        features = self._features

        if self._drop_ibes:
            features = [x for x in features if x not in ibes_features]

        if self._drop_accounting:
            features = [x for x in features if x not in accounting_features]

        if self._drop_starmine:
            features = [x for x in features if x not in starmine_features]

        if self._drop_market_variables == 'constrained':
            features = [x for x in features if x.find('MKT_') == -1]
            features.extend(['MKT_VIX_AdjClose', 'MKT_VIX_PRMA10',
                             'MKT_SP500Index_VOL10', 'MKT_SP500Index_PRMA10',
                             'MKT_SP500Index_BOLL20'])
        elif self._drop_market_variables:
            features = [x for x in features if x.find('MKT_') == -1]

        # A bit of security if variables come through in different order
        features.sort()
        self._features = features

    def fit_model(self):
        self._process_args()
        self.skl_model.fit(X=self._train_data[self._features],
                           y=self._train_responses['Response'])

    def get_model(self):
        return self.skl_model

    def set_model(self, model):
        self.skl_model = model

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_signals(self):
        self._process_args()
        output = self._test_data[['SecCode', 'Date']].copy()
        if hasattr(self.skl_model, 'predict_proba'):
            preds = self.skl_model.predict_proba(self._test_data[self._features])
            output.loc[:, 'preds'] = _get_preds(self.skl_model, preds)
        else:
            output.loc[:, 'preds'] = self.skl_model.predict(
                self._test_data[self._features])
        return output


def _get_preds(classifier, preds):
    if -1 in classifier.classes_:
        short_ind = np.where(classifier.classes_ == -1)[0][0]
        long_ind = np.where(classifier.classes_ == 1)[0][0]
        return preds[:, long_ind] - preds[:, short_ind]
    else:
        long_ind = np.where(classifier.classes_ == 1)[0][0]
        return preds[:, long_ind]

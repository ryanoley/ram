import numpy as np

from sklearn.ensemble import ExtraTreesClassifier, AdaBoostClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LinearRegression

from ram.strategy.statarb.abstract.signal_generator import BaseSignalGenerator
from ram.strategy.statarb.version_001.data.data_container_pairs import \
    accounting_features, starmine_features, ibes_features

try:
    from xgboost import XGBClassifier
    SIGNAL_MODELS = ['xgb_1', 'xgb_2']
except:
    SIGNAL_MODELS = ['linear_model', 'extra_trees_1', 'extra_trees_2']


class SignalModel1(BaseSignalGenerator):

    def __init__(self):
        self.skl_model = None

    def get_args(self):
        return {
            'signal_model': SIGNAL_MODELS,
            'drop_ibes': [True],
            'drop_accounting': [False],
            'drop_starmine': [False],
            'drop_market_variables': ['constrained']
        }

    def set_args(self,
                 signal_model,
                 drop_ibes,
                 drop_accounting,
                 drop_starmine,
                 drop_market_variables):

        if signal_model == 'extra_trees_1':
            self.skl_model = ExtraTreesClassifier(n_jobs=-1,
                                                  random_state=123,
                                                  min_samples_leaf=200,
                                                  n_estimators=100,
                                                  max_features=0.8)

        if signal_model == 'extra_trees_2':
            self.skl_model = ExtraTreesClassifier(n_jobs=-1,
                                                  random_state=123,
                                                  min_samples_leaf=50,
                                                  n_estimators=30,
                                                  max_features=0.6)

        elif signal_model == 'ada_boost_1':
            self.skl_model = AdaBoostClassifier(
                random_state=123,
                n_estimators=100,
                base_estimator=DecisionTreeClassifier(min_samples_leaf=50,
                                                      min_samples_split=200))

        elif signal_model == 'linear_model':
            # Used as a baseline
            self.skl_model = LinearRegression()

        elif signal_model == 'xgb_1':
            self.skl_model = XGBClassifier(
                n_jobs=-1,
                subsample=0.5,
                colsample_bylevel=0.7,
                max_depth=25,
                )

        elif signal_model == 'xgb_2':
            self.skl_model = XGBClassifier(
                n_jobs=-1,
                subsample=0.5,
                max_depth=12,
                )

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
            features.extend(['MKT_AdjClose_11113', 'MKT_PRMA10_11113',
                             'MKT_VOL10_50311', 'MKT_PRMA10_50311',
                             'MKT_BOLL20_50311'])

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
        output = self._test_data[['SecCode', 'Date']].copy()
        # Here the features that are coming through have nan values
        # Where were they handled before where they weren't showing up?
        if hasattr(self.skl_model, 'predict_proba'):
            preds = self.skl_model.predict_proba(
                self._test_data[self._features])
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

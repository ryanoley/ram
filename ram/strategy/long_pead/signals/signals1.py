import numpy as np

from sklearn.ensemble import ExtraTreesClassifier

from ram.strategy.long_pead.signals.base import BaseSignal


class SignalModel1(BaseSignal):

    def __init__(self):
        self.skl_model = ExtraTreesClassifier(n_jobs=-1)

    def get_args(self):
        return {
            'model_params': [
                {'min_samples_leaf': 200,
                 'n_estimators': 100,
                 'max_features': 0.8,
                },
                {'min_samples_leaf': 50,
                 'n_estimators': 30,
                 'max_features': 0.6,
                },
            ],
            'drop_ibes': [True],
            'drop_accounting': [True],
            'drop_extremes': [True],
            'drop_starmine': [True, False],
            'drop_market_variables': ['constrained']
        }

    def get_skl_model(self):
        return self.skl_model

    def generate_signals(self,
                         data_container,
                         model_params,
                         drop_ibes,
                         drop_accounting,
                         drop_extremes,
                         drop_starmine,
                         drop_market_variables):

        train_data = data_container.train_data
        test_data = data_container.test_data
        features = data_container.features

        if drop_ibes:
            features = [x for x in features if x[:4] != 'IBES']

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
        if drop_market_variables == 'constrained':
            features = [x for x in features if x.find('Mkt_') == -1]
            features.extend(['Mkt_VIX_AdjClose', 'Mkt_VIX_PRMA10',
                             'Mkt_SP500Index_VOL10', 'Mkt_SP500Index_PRMA10',
                             'Mkt_SP500Index_BOLL20'])
        elif drop_market_variables:
            features = [x for x in features if x.find('Mkt_') == -1]
        if drop_starmine:
            starmine_vars = [
                'LAG1_ARM', 'LAG1_ARMREVENUE', 'LAG1_ARMRECS',
                'LAG1_ARMEARNINGS', 'LAG1_ARMEXRECS', 'LAG1_SIRANK',
                'LAG1_SIMARKETCAPRANK', 'LAG1_SISECTORRANK',
                'LAG1_SIUNADJRANK', 'LAG1_SISHORTSQUEEZE',
                'LAG1_SIINSTOWNERSHIP']
            features = [x for x in features if x not in starmine_vars]

        self.skl_model.set_params(**model_params)
        self.skl_model.fit(X=train_data[features],
                           y=train_data['Response'])

        preds = self.skl_model.predict_proba(test_data[features])
        test_data['preds'] = _get_preds(self.skl_model, preds)
        self.preds_data = test_data[['SecCode', 'Date', 'preds']].copy()


def _get_preds(classifier, preds):
    if -1 in classifier.classes_:
        short_ind = np.where(classifier.classes_ == -1)[0][0]
        long_ind = np.where(classifier.classes_ == 1)[0][0]
        return preds[:, long_ind] - preds[:, short_ind]
    else:
        long_ind = np.where(classifier.classes_ == 1)[0][0]
        return preds[:, long_ind]

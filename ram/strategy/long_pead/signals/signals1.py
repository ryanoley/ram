import numpy as np

from sklearn.ensemble import ExtraTreesClassifier

from ram import config


class SignalModel1(object):

    def __init__(self, njobs=config.SKLEARN_NJOBS):
        self.NJOBS = njobs

    def get_args(self):
        return {
            'model_params': [
                {'min_samples_leaf': 100,
                 'n_estimators': 100,
                 'max_features': 0.8,
                },

                {'sort_variable': 'PRMA10_AdjClose'},
                {'sort_variable': 'VOL10_AdjClose'},
                {'sort_variable': 'RSI10_AdjClose'},
                {'sort_variable': 'LAG1_ARM'},
                {'sort_variable': 'LAG1_SISECTORRANK'},
                # {'min_samples_leaf': 50,
                #  'n_estimators': 100,
                #  'max_features': 0.8,
                # },
                # {'min_samples_leaf': 100,
                #  'n_estimators': 100,
                #  'max_features': 0.6,
                # },
                # {'min_samples_leaf': 50,
                #  'n_estimators': 100,
                #  'max_features': 0.6,
                # },
            ],
            'drop_accounting': [False, True],
            'drop_extremes': [True],
            'drop_starmine': [False, True],
            'drop_extract_alpha': [True],
            'drop_market_variables': ['constrained', False, True],
            'training': ['quarterly']
        }

    def generate_signals(self,
                         data_container,
                         model_params,
                         drop_accounting,
                         drop_extremes,
                         drop_starmine,
                         drop_market_variables,
                         drop_extract_alpha,
                         training):

        train_data = data_container.train_data
        test_data = data_container.test_data
        features = data_container.features

        if 'sort_variable' in model_params:
            test_data = test_data[['SecCode', 'Date',
                                   model_params['sort_variable']]].copy()
            test_data['preds'] = test_data[model_params['sort_variable']]
            self.preds_data = test_data[['SecCode', 'Date', 'preds']]
            return

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
        if drop_extract_alpha:
            extract_alpha_vars = [
                'EA_TRESS', 'EA_spread_component', 'EA_skew_component',
                'EA_volume_component', 'EA_CAM1', 'EA_CAM1_slow',
                'EA_reversal_component', 'EA_factor_momentum_component',
                'EA_liquidity_shock_component', 'EA_seasonality_component',
                'EA_tm1', 'EA_Digital_Revenue_Signal']
            features = [x for x in features if x not in extract_alpha_vars]

        clf = ExtraTreesClassifier(n_jobs=self.NJOBS, **model_params)

        if training == 'weekly':

            train_data['preds'] = 0

            for i in np.arange(1, max(test_data.week_index)+1):
                test_data_2 = test_data[test_data.week_index == i]

                inds = train_data.week_index_train_offset < i
                clf.fit(X=train_data.loc[inds, features],
                        y=train_data.loc[inds, 'Response'])

                # Get indexes of long and short sides
                short_ind = np.where(clf.classes_ == -1)[0][0]
                long_ind = np.where(clf.classes_ == 1)[0][0]

                # Get test predictions to create portfolios on:
                #    Long Prediction - Short Prediction
                preds = clf.predict_proba(test_data_2[features])

                test_data_2.loc[:, 'preds'] = preds[:, long_ind] - \
                    preds[:, short_ind]

                train_data = train_data.append(test_data_2)

            test_data = train_data[train_data.TestFlag].reset_index(True)
            self.preds_data = test_data[['SecCode', 'Date', 'preds']]

        elif training == 'monthly':

            train_data['preds'] = 0
            dates = test_data.Date.unique()
            months = np.array([d.month for d in dates])

            for i, m in enumerate(np.unique(months)):
                min_date = min(dates[months == m])
                max_date = max(dates[months == m])
                test_data_2 = test_data[(test_data.Date >= min_date) &
                                        (test_data.Date <= max_date)].copy()

                # THIS IS A BIG ASSUMPTION. DO WE WANT TO DROP THESE OBS?
                inds = train_data.month_index <= i
                clf.fit(X=train_data.loc[inds, features],
                        y=train_data.loc[inds, 'Response'])

                # Get indexes of long and short sides
                short_ind = np.where(clf.classes_ == -1)[0][0]
                long_ind = np.where(clf.classes_ == 1)[0][0]

                # Get test predictions to create portfolios on:
                #    Long Prediction - Short Prediction
                preds = clf.predict_proba(test_data_2[features])

                test_data_2.loc[:, 'preds'] = preds[:, long_ind] - \
                    preds[:, short_ind]

                train_data = train_data.append(test_data_2)

            test_data = train_data[train_data.TestFlag].reset_index(True)
            self.preds_data = test_data[['SecCode', 'Date', 'preds']]

        else:
            # Hack to accomodate response vars
            inds = train_data.week_index_train_offset < 1
            clf.fit(X=train_data.loc[inds, features],
                    y=train_data.loc[inds, 'Response'])

            # Get indexes of long and short sides
            short_ind = np.where(clf.classes_ == -1)[0][0]
            long_ind = np.where(clf.classes_ == 1)[0][0]

            # Get test predictions to create portfolios on:
            #    Long Prediction - Short Prediction
            preds = clf.predict_proba(test_data[features])

            test_data.loc[:, 'preds'] = \
                preds[:, long_ind] - preds[:, short_ind]
            self.preds_data = test_data[['SecCode', 'Date', 'preds']].copy()

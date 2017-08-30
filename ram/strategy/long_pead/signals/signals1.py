import numpy as np

from sklearn.ensemble import ExtraTreesClassifier

from ram import config


class SignalModel1(object):

    def __init__(self, njobs=config.SKLEARN_NJOBS):
        self.NJOBS = njobs

    def get_args(self):
        return {
            # 'tree_params': [
            #     {'min_samples_leaf': 200,
            #      'n_estimators': 500,
            #      'max_features': 0.8,
            #     },
            #     {'min_samples_leaf': 100,
            #      'n_estimators': 500,
            #      'max_features': 0.8,
            #     },
            #     {'min_samples_leaf': 40,
            #      'n_estimators': 100,
            #      'max_features': 0.8,
            #     },
            #     {'min_samples_leaf': 40,
            #      'n_estimators': 100,
            #      'max_features': 0.8,
            #      'bootstrap': True,
            #     },
            # 
            #     {'min_samples_leaf': 200,
            #      'n_estimators': 500,
            #      'max_features': 'log2',
            #     },
            #     {'min_samples_leaf': 40,
            #      'n_estimators': 100,
            #      'max_features': 'log2',
            #     },
            # ],
            'tree_params': [
                {'min_samples_leaf': 80,
                 'n_estimators': 100,
                 'max_features': 0.8,
                }
            ],
            'drop_accounting': [False],
            'drop_extremes': [True],
            'drop_starmine': [False],
            'drop_market_variables': [True],
            'training': ['weekly', 'monthly', 'quarterly']
        }

    def generate_signals(self,
                         data_container,
                         tree_params,
                         drop_accounting,
                         drop_extremes,
                         drop_starmine,
                         drop_market_variables,
                         training):

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
        if drop_starmine:
            starmine_vars = [
                'LAG1_ARM', 'LAG1_ARMREVENUE', 'LAG1_ARMRECS',
                'LAG1_ARMEARNINGS', 'LAG1_ARMEXRECS', 'LAG1_SIRANK',
                'LAG1_SIMARKETCAPRANK', 'LAG1_SISECTORRANK',
                'LAG1_SIUNADJRANK', 'LAG1_SISHORTSQUEEZE',
                'LAG1_SIINSTOWNERSHIP']
            features = [x for x in features if x not in starmine_vars]

        clf = ExtraTreesClassifier(n_jobs=self.NJOBS, **tree_params)

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

            clf.fit(X=train_data[features],
                    y=train_data['Response'])

            # Get indexes of long and short sides
            short_ind = np.where(clf.classes_ == -1)[0][0]
            long_ind = np.where(clf.classes_ == 1)[0][0]

            # Get test predictions to create portfolios on:
            #    Long Prediction - Short Prediction
            preds = clf.predict_proba(test_data[features])

            test_data['preds'] = preds[:, long_ind] - preds[:, short_ind]
            self.preds_data = test_data[['SecCode', 'Date', 'preds']].copy()


import numpy as np

from sklearn.ensemble import ExtraTreesClassifier


class SignalModel(object):

    def __init__(self):
        self.skl_model = None

    def get_args(self):
        return {
            'signal_model': [1],
        }

    def set_args(self, signal_model):
        self.skl_model = ExtraTreesClassifier(n_jobs=-1,
                                              random_state=123,
                                              min_samples_leaf=200,
                                              n_estimators=30,
                                              max_features=0.8)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_signals(self, data_container):
        output = data_container.test_data[['SecCode', 'Date',
                                           'keep_inds']].copy()

        features = ['day_ret', 'lag_1_ret', 'lag_2_ret', 'lag_3_ret',
                    'lag_4_ret', 'lag_5_ret', 'prma_10', 'prma_15',
                    'prma_20', 'prma_5']

        self.skl_model.fit(X=data_container.train_data[features],
                           y=data_container.train_data['Response'])

        preds = self.skl_model.predict_proba(
            data_container.test_data[features])
        output.loc[:, 'preds'] = _get_preds(self.skl_model, preds)
        output.preds = np.where(output.keep_inds, output.preds, np.nan)
        output = output.pivot(index='Date', columns='SecCode',
                              values='preds')
        return output


def _get_preds(classifier, preds):
    if -1 in classifier.classes_:
        short_ind = np.where(classifier.classes_ == -1)[0][0]
        long_ind = np.where(classifier.classes_ == 1)[0][0]
        return preds[:, long_ind] - preds[:, short_ind]
    else:
        long_ind = np.where(classifier.classes_ == 1)[0][0]
        return preds[:, long_ind]

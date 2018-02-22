import numpy as np
import datetime as dt

from ram.strategy.statarb.utils import make_arg_iter

from sklearn.ensemble import ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression


class SignalModel(object):

    def __init__(self):
        pass

    def get_args(self):
        return make_arg_iter({
            'model': [
                {'type': 'tree', 'min_samples_leaf': 500, 'max_features': 0.5},
                {'type': 'tree', 'min_samples_leaf': 500, 'max_features': 0.8},
                #{'type': 'tree', 'min_samples_leaf': 2000, 'max_features': 0.5},
                #{'type': 'tree', 'min_samples_leaf': 2000, 'max_features': 0.8},
                {'type': 'reg'},
            ]
        })

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

    def get_signals(self, data_container):
        output = data_container.test_data[['SecCode', 'Date',
                                           'keep_inds']].copy()
        features = data_container.features
        inds = data_container.train_data.TrainFlag
        self.skl_model.fit(X=data_container.train_data[features][inds],
                           y=data_container.train_data['Response'][inds])
        preds = self.skl_model.predict_proba(
            data_container.test_data[features])
        output.loc[:, 'preds'] = _get_preds(self.skl_model, preds)
        output.preds = np.where(output.keep_inds, output.preds, np.nan)
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

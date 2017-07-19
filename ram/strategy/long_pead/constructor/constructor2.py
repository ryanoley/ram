import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.constructor.portfolio import Portfolio
from ram.strategy.long_pead.constructor.utils import ern_date_blackout
from ram.strategy.long_pead.constructor.utils import make_anchor_ret_rank
from ram.strategy.long_pead.constructor.utils import ern_return

from sklearn.preprocessing import RobustScaler

from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import BaggingClassifier
from sklearn.linear_model import RidgeClassifier
from sklearn.ensemble import VotingClassifier
from sklearn.linear_model import Lasso

NJOBS = 2


class PortfolioConstructor2(object):

    def __init__(self, booksize=10e6):
        """
        Parameters
        ----------
        booksize : numeric
            Size of gross position
        """
        self.booksize = booksize
        self.train_data = pd.DataFrame()
        self.test_data = pd.DataFrame()
        self._train_data_max_time_index = -99

    def get_iterable_args(self):
        return {
            'logistic_spread': [0.1, 1, 2, 5, 10]
        }

    def get_data_args(self):
        return {
            'response_days': [[2, 4, 6], [2], [6]],
            'response_thresh': [.25, .45],
            'model_drop': [1, 2, 3, 4]
        }

    def get_daily_pl(self, arg_index, logistic_spread):
        """
        Parameters
        ----------
        """
        portfolio = Portfolio()
        # Output object
        daily_df = pd.DataFrame(index=self.iter_dates,
                                columns=['PL', 'Exposure', 'Turnover'],
                                dtype=float)

        for date in self.iter_dates:

            closes = self.close_dict[date]
            dividends = self.dividend_dict[date]
            splits = self.split_mult_dict[date]
            scores = self.scores_dict[date]
            # Could this be just a simple "Group"
            mcaps = self.market_cap_dict[date]

            portfolio.update_prices(closes, dividends, splits)

            if date == self.iter_dates.iloc[-1]:
                portfolio.close_portfolio_positions()
            else:
                sizes = self._get_position_sizes(scores, logistic_spread,
                                                 self.booksize)
                portfolio.update_position_sizes(sizes, closes)

            daily_pl = portfolio.get_portfolio_daily_pl()
            daily_turnover = portfolio.get_portfolio_daily_turnover()
            daily_exposure = portfolio.get_portfolio_exposure()

            daily_df.loc[date, 'PL'] = daily_pl
            daily_df.loc[date, 'Turnover'] = daily_turnover
            daily_df.loc[date, 'Exposure'] = daily_exposure

        # Close everything and begin anew in new quarter
        return daily_df

    def _get_position_sizes(self, scores, logistic_spread, booksize):
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.

        The weighting scheme takes on the shape of a sigmoid function,
        and the shape of the sigmoid is modulated by the hyperparameter
        logistic spread.
        """
        scores = pd.Series(scores).to_frame()
        scores.columns = ['score']
        scores = scores.sort_values('score')

        # Simple rank
        def logistic_weight(k):
            return 2 / (1 + np.exp(-k)) - 1

        n_good = (~scores.score.isnull()).sum()
        n_bad = scores.score.isnull().sum()
        scores['weights'] = [
            logistic_weight(x) for x in np.linspace(
                -logistic_spread, logistic_spread, n_good)] + [0] * n_bad
        scores.weights = scores.weights / scores.weights.abs().sum() * booksize
        return scores.weights.to_dict()

    # ~~~~~~ Data Format ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def set_and_prep_data(self, data, time_index,
                          response_days,
                          response_thresh,
                          model_drop):

        train_data, test_data, features = self._process_train_test_data(
            data, time_index)

        train_data = train_data.merge(
            smoothed_responses(train_data, days=response_days,
                               thresh=response_thresh))

        # CREATE MODELS
        clf1 = RandomForestClassifier(n_estimators=100, n_jobs=NJOBS,
                                      min_samples_leaf=60,
                                      max_features=7)

        clf2 = ExtraTreesClassifier(n_estimators=100, n_jobs=NJOBS,
                                    min_samples_leaf=60,
                                    max_features=7)

        clf3 = BaggingClassifier(LogisticRegression(), n_estimators=10,
                                 max_samples=0.7, max_features=0.6,
                                 n_jobs=NJOBS)

        clf4 = BaggingClassifier(RidgeClassifier(tol=1e-1, solver="lsqr"),
                                 n_estimators=10,
                                 max_samples=0.7, max_features=0.6,
                                 n_jobs=NJOBS)

        clf5 = BaggingClassifier(Lasso(alpha=0.3, tol=0.0001),
                                 n_estimators=10,
                                 max_samples=0.7, max_features=0.6,
                                 n_jobs=NJOBS)

        assert model_drop in [1, 2, 3, 4, 5]
        models = [('rf', clf1), ('et', clf2), ('lc', clf3),
                  ('rc', clf4), ('ls', clf5)]
        models.pop(model_drop - 1)
        clf = VotingClassifier(estimators=models, voting='soft')

        clf.fit(X=train_data[features], y=train_data['Response'])

        # Get indexes of long and short sides
        short_ind = np.where(clf.classes_ == -1)[0][0]
        long_ind = np.where(clf.classes_ == 1)[0][0]

        # Get test predictions to create portfolios on:
        #    Long Prediction - Short Prediction
        preds = clf.predict_proba(test_data[features])
        test_data['preds'] = preds[:, long_ind] - preds[:, short_ind]

        self.iter_dates = test_data.Date.drop_duplicates()
        # Formatted for portfolio construction
        self.close_dict = make_variable_dict(data, 'RClose')
        self.dividend_dict = make_variable_dict(data, 'RCashDividend', 0)
        self.split_mult_dict = make_variable_dict(data,
                                                  'SplitMultiplier', 1)
        self.market_cap_dict = make_variable_dict(data,
                                                  'MarketCap', 'pad')
        self.scores_dict = make_variable_dict(test_data, 'preds')

    def _process_train_test_data(self, data, time_index):
        """
        Handles raw data files that were read by Strategy base class.
        The time index is used for retrieving cached processed data.
        """
        # GET CACHED DATA
        if time_index == self._train_data_max_time_index:
            return self.train_data.copy(), self.test_data.copy(), self.features

        # ELSE Construct new training and test data from inputted data
        self._train_data_max_time_index = time_index

        features = [
            'PRMA120_AvgDolVol', 'PRMA10_AdjClose',
            'PRMA20_AdjClose', 'BOLL10_AdjClose', 'BOLL20_AdjClose',
            'BOLL60_AdjClose', 'MFI10_AdjClose', 'MFI20_AdjClose',
            'MFI60_AdjClose', 'RSI10_AdjClose', 'RSI20_AdjClose',
            'RSI60_AdjClose', 'VOL10_AdjClose', 'VOL20_AdjClose',
            'VOL60_AdjClose', 'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose',
            'DISCOUNT252_AdjClose',
            # Accounting Variables
            'NETINCOMEQ', 'NETINCOMETTM', 'SALESQ', 'SALESTTM', 'ASSETS',
            'CASHEV', 'FCFMARKETCAP',
            'NETINCOMEGROWTHQ',
            'NETINCOMEGROWTHTTM',
            'OPERATINGINCOMEGROWTHQ',
            'OPERATINGINCOMEGROWTHTTM',
            'EBITGROWTHQ',
            'EBITGROWTHTTM',
            'SALESGROWTHQ',
            'SALESGROWTHTTM',
            'FREECASHFLOWGROWTHQ',
            'FREECASHFLOWGROWTHTTM',
            'GROSSPROFASSET',
            'GROSSMARGINTTM',
            'EBITDAMARGIN',
            'PE',
        ]

        # ~~~~~~ CLEAN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        data.AdjVwap = np.where(
            data.AdjVwap.isnull(), data.AdjClose, data.AdjVwap)
        data.AdjClose = np.where(
            data.AdjClose.isnull(), data.AdjVwap, data.AdjClose)

        # SPLITS: Instead of using the levels, use the CHANGE in levels.
        # This is necessary for the updating of positions and prices downstream
        data.loc[:, 'SplitMultiplier'] = \
            data.SplitFactor.pct_change().fillna(0) + 1

        # NEW FEATURES
        # Blackout flags and anchor returns
        data = ern_date_blackout(data, offset1=-2, offset2=4)

        data = make_anchor_ret_rank(data, init_offset=3, window=10)

        data = ern_return(data)

        # Rank and create binaries for extreme values
        data2 = outlier_rank(data, features[0])
        for f in features[1:]:
            data2 = data2.merge(outlier_rank(data, f))
        data = data.drop(features, axis=1)
        data = data.merge(data2)
        features = features + [f + '_extreme' for f in features] + \
            ['blackout', 'anchor_ret_rank', 'earnings_ret']
        data = data[['SecCode', 'Date', 'TestFlag', 'AdjClose'] + features]

        data = data.dropna()

        # Separate training from test data
        self.train_data = self.train_data.append(data[~data.TestFlag])
        self.test_data = data[data.TestFlag]
        self.features = features

        return self.train_data.copy(), self.test_data.copy(), self.features


def make_variable_dict(data, variable, fillna=np.nan):
    data_pivot = data.pivot(index='Date', columns='SecCode', values=variable)
    if fillna == 'pad':
        data_pivot = data_pivot.fillna(method='pad')
    else:
        data_pivot = data_pivot.fillna(fillna)
    return data_pivot.T.to_dict()


def smoothed_responses(data, thresh=.25, days=[2, 4, 6]):
    if not isinstance(days, list):
        days = [days]
    rets = data.pivot(index='Date', columns='SecCode', values='AdjClose')
    for i in days:
        if i == days[0]:
            rank = rets.pct_change(i).shift(-i).rank(axis=1, pct=True)
        else:
            rank += rets.pct_change(i).shift(-i).rank(axis=1, pct=True)
    final_ranks = rank.rank(axis=1, pct=True)
    output = final_ranks.copy()
    output[:] = (final_ranks >= (1 - thresh)).astype(int) - \
        (final_ranks <= thresh).astype(int)
    output = output.unstack().reset_index()
    output.columns = ['SecCode', 'Date', 'Response']
    return output


def outlier_rank(data, variable, outlier_std=4, pad=True):
    """
    Will create two columns, and if the variable is an extreme outlier will
    code it as a 1 or -1 depending on side and force rank to median for
    the date.
    """
    pdata = data.pivot(index='Date', columns='SecCode', values=variable)
    if pad:
        pdata = pdata.fillna(method='pad')

    # Get extreme value cutoffs
    daily_min = pdata.median(axis=1) - outlier_std * pdata.std(axis=1)
    daily_max = pdata.median(axis=1) + outlier_std * pdata.std(axis=1)

    # Make data frame with Bools for extreme values
    extremes = pdata.gt(daily_max, axis=0).astype(int) - \
        pdata.lt(daily_min, axis=0).astype(int)

    # Rank
    ranks = (pdata.rank(axis=1) - 1) / (pdata.shape[1] - 1)

    # Combine
    extremes = extremes.unstack().reset_index()
    extremes.columns = ['SecCode', 'Date', variable + '_extreme']
    ranks = ranks.unstack().reset_index()
    ranks.columns = ['SecCode', 'Date', variable]
    return ranks.merge(extremes)


def _duplicate(series, shape):
    return series.repeat(shape[1]).values.reshape(shape)

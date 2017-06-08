import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.constructor.portfolio import Portfolio
from ram.strategy.long_pead.constructor.utils import ern_date_blackout
from ram.strategy.long_pead.constructor.utils import ern_price_anchor

from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import BaggingClassifier
from sklearn.linear_model import RidgeClassifier
from sklearn.ensemble import VotingClassifier


class PortfolioConstructor2(object):

    def __init__(self, booksize=10e6):
        """
        Parameters
        ----------
        booksize : numeric
            Size of gross position
        """
        self.booksize = booksize
        self._portfolios = {}
        self._data = {}

    def get_iterable_args(self):
        return {
            'logistic_spread': [.1, .5, 1, 2]
        }

    def get_data_args(self):
        return {
            'blackout_offset1': [-1],
            'blackout_offset2': [3, 6],
            'anchor_init_offset': [5],
            'anchor_window': [10]
        }

    def get_daily_pl(self, arg_index, logistic_spread):
        """
        Parameters
        ----------
        """
        portfolio = Portfolio()
        # Output object
        daily_df = pd.DataFrame(index=self.iter_dates,
                                columns=['PL', 'Exposure', 'Count'],
                                dtype=float)
        for date in self.iter_dates:
            closes = self.close_dict[date]
            dividends = self.dividend_dict[date]
            splits = self.split_mult_dict[date]
            scores = self.scores_dict[date]
            # Get PL
            portfolio.update_prices(closes, dividends, splits)
            portfolio.update_position_sizes(
                self._get_position_sizes(scores,
                                         logistic_spread,
                                         self.booksize))
            daily_df.loc[date, 'PL'] = portfolio.get_portfolio_daily_pl()
            daily_df.loc[date, 'Exposure'] = portfolio.get_exposure()
            exps = np.array([x.exposure for x in portfolio.positions.values()])
            daily_df.loc[date, 'Count'] = (exps != 0).sum()
        return daily_df

    def _get_position_sizes(self, mrets, logistic_spread, booksize):
        mrets = pd.Series(mrets).to_frame()
        mrets.columns = ['MomRet']
        mrets = mrets.sort_values('MomRet')
        # Simple rank
        def logistic_weight(k):
            return 2 / (1 + np.exp(-k)) - 1
        n_good = (~mrets.MomRet.isnull()).sum()
        n_bad = mrets.MomRet.isnull().sum()
        mrets['weights'] = [
            logistic_weight(x) for x in np.linspace(
                -logistic_spread, logistic_spread, n_good)] + [0] * n_bad
        mrets.weights = mrets.weights / mrets.weights.abs().sum() * booksize
        return mrets.weights.to_dict()

    # ~~~~~~ Data Format ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def set_and_prep_data(self, data, time_index,
                          blackout_offset1,
                          blackout_offset2,
                          anchor_init_offset,
                          anchor_window):
        # Anchor prices
        data = ern_date_blackout(data, offset1=blackout_offset1,
                                 offset2=blackout_offset2)
        data = ern_price_anchor(data, init_offset=anchor_init_offset,
                                window=anchor_window)

        # Training happens
        features = [
            'anchor_ret',
            'RANK_AvgDolVol', 'RANK_PRMA120_AvgDolVol',
            'RANK_PRMA10_AdjClose', 'RANK_PRMA20_AdjClose',

            'RANK_BOLL10_AdjClose', 'RANK_BOLL20_AdjClose', 'RANK_BOLL60_AdjClose',
            'RANK_MFI10_AdjClose', 'RANK_MFI20_AdjClose', 'RANK_MFI60_AdjClose',
            'RANK_RSI10_AdjClose', 'RANK_RSI20_AdjClose', 'RANK_RSI60_AdjClose',
            'RANK_VOL10_AdjClose', 'RANK_VOL20_AdjClose', 'RANK_VOL60_AdjClose',

            'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose', 'DISCOUNT252_AdjClose',
            'RANK_DISCOUNT63_AdjClose', 'RANK_DISCOUNT126_AdjClose', 'RANK_DISCOUNT252_AdjClose',

            'ACCTSALESGROWTH', 'ACCTSALESGROWTHTTM',
            'ACCTEPSGROWTH', 'ACCTPRICESALES']

        train_data = data[~data.TestFlag]
        train_data = train_data.merge(smoothed_responses(train_data))
        train_data = train_data[['SecCode', 'Date', 'Response'] +
                                features].dropna()

        # Cache training data
        if time_index == 0:
            self.train_data = train_data
        else:
            self.train_data = self.train_data.append(train_data)

        clf1 = RandomForestClassifier(n_estimators=100, n_jobs=-1,
                                     min_samples_leaf=30,
                                     max_features=7)

        clf2 = ExtraTreesClassifier(n_estimators=100, n_jobs=-1,
                                    min_samples_leaf=30,
                                    max_features=7)

        clf3 = BaggingClassifier(LogisticRegression(), n_estimators=10,
                                 max_samples=0.7, max_features=0.6, n_jobs=-1)

        clf4 = RidgeClassifier(tol=1e-2, solver="lsqr")

        clf = VotingClassifier(estimators=[('rf', clf1), ('et', clf2),
            ('lc', clf3), ('rc', clf4)], voting='soft')

        clf.fit(X=self.train_data[features], y=self.train_data['Response'])

        # Get indexes of long and short sides
        short_ind = np.where(clf.classes_ == -1)[0][0]
        long_ind = np.where(clf.classes_ == 1)[0][0]

        # Get test predictions to create portfolios on:
        #    Long Prediction - Short Prediction
        test_data = data[data.TestFlag]
        test_data = test_data[['SecCode', 'Date'] + features].dropna()
        preds = clf.predict_proba(test_data[features])
        test_data['preds'] = preds[:, long_ind] - preds[:, short_ind]

        # Get training and test dates
        test_dates = data[data.TestFlag].Date.drop_duplicates()
        qtrs = np.array([(x.month-1)/3+1 for x in test_dates])
        iter_dates = test_dates[qtrs == qtrs[0]]
        # Calculate State Variables, including Z-Score
        closes = data.pivot(
            index='Date', columns='SecCode', values='RClose').loc[test_dates]
        dividends = data.pivot(
            index='Date', columns='SecCode',
            values='RCashDividend').fillna(0).loc[test_dates]

        scores = test_data.pivot(index='Date', columns='SecCode',
                                 values='preds').loc[test_dates]

        # Instead of using the levels, use the change in levels.
        # This is necessary for the updating of positions and prices
        data.loc[:, 'SplitMultiplier'] = \
            data.SplitFactor.pct_change().fillna(0) + 1
        split_mult = data.pivot(
            index='Date', columns='SecCode',
            values='SplitMultiplier').fillna(1).loc[test_dates]
        self.iter_dates = iter_dates
        self.close_dict = closes.T.to_dict()
        self.dividend_dict = dividends.T.to_dict()
        self.split_mult_dict = split_mult.T.to_dict()
        self.scores_dict = scores.T.to_dict()
        self.data = data



def smoothed_responses(data, thresh=.25, days=[2, 4, 6]):
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







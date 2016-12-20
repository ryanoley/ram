import os
import itertools
import numpy as np
import pandas as pd
import datetime as dt
from sklearn import linear_model
from sklearn import ensemble 
from sklearn.decomposition import PCA
from ram.strategy.base import Strategy


class YearEnd(Strategy):

    COST = 0.0015

    def __init__(self, hold_per=4, univ_size=1000, exit_offset=0,
                 output_dir=None):
        super( YearEnd, self ).__init__()
        self.date_iter = self._get_date_iterator(hold_per, exit_offset)
        self.hold_per = hold_per
        self.univ_size = univ_size

    def get_iter_index(self):
        return self.date_iter[5:]

    def _get_date_iterator(self, hold_per=4, exit_offset=0):
        """
        Produce iterable with revlevant dates (eval_dt, entry_dt, exit_dt)
        for each event (year end). Exit is by default the first business date
        of the New Year, exit_offset will move this out by n number of days.
        """
        db_dates = self.datahandler.get_all_dates()

        fdoy = pd.date_range('2000-01-01', '2016-01-01', freq='AS')
        fdoy = np.array([x.to_pydatetime() for x in fdoy])
        fbdoy =  np.array([db_dates[db_dates > x].min() for x in fdoy])
        fbdix = np.array([np.where(db_dates == x)[0][0] for x in fbdoy])

        exitix = fbdix + exit_offset
        entryix = exitix - hold_per
        evalix = entryix - 1

        iterable = zip(db_dates[evalix], db_dates[entryix], db_dates[exitix])
        return iterable

    def run_index(self, index):
        ix = self.date_iter.index(index)

        # BUILD TEST AND TRAIN DATA FRAMES
        if hasattr(self, 'priortrain'):
            train = self.priortrain.append(self.priortest)
            train.reset_index(drop=True, inplace=True)
        else:
            train = self.get_univ_iter(self.date_iter[ : ix])
        self.priortrain = train.copy()
        test = self.get_univ_ix(index)
        self.priortest = test.copy()
        train = train.dropna()
        test = test.dropna()

        # GET SIGNALS
        features = list(set(train.columns) - set(
            ['SecCode', 'Date', 'Vwap', 'LEAD{}_Vwap'.format(self.hold_per),
             'RClose', 'MarketCap', 'AvgDolVol', 'GSECTOR', 'Ret', 'RetH']))
        signals = self.generate_signals(train[features].copy(), train.RetH,
                                        test[features].copy())

        # SET PORTFOLIOS
        test_ranks, test_n, test_cID = self._get_selection_params(signals)
        test.loc[:, 'AggSignal'] = test_ranks
        test.sort_values('AggSignal', inplace=True)
        long_ix, short_ix = self.get_portfolio_ix(test.GSECTOR.copy(), n=test_n)

        # GET DAILY RETURNS
        long_ids = list(test.loc[long_ix, 'SecCode'])
        short_ids = list(test.loc[short_ix, 'SecCode'])
        rets = self._get_dailys_rets(long_ids, short_ids, index[1], index[2])
        grp_long = rets[rets.ID.isin(long_ids)].groupby('Date')
        grp_short = rets[rets.ID.isin(short_ids)].groupby('Date')
        port_rets = pd.DataFrame(grp_long.Ret.mean() - grp_short.Ret.mean())
        port_rets['Long'] = grp_long.Ret.mean()
        port_rets['Short'] = grp_short.Ret.mean()
        port_rets['Year'] = index[1].year
        port_rets['n'] = test_n
        port_rets['cID'] = test_cID

        # ITERATE META PARAMS
        test.sort_index(inplace=True)
        self.iterate_meta_params(signals, test)

        print str(index[2])
        return port_rets

    def get_univ_iter(self, dt_iter):
        '''
        Loop through dt_iter and append results together
        '''
        univ = pd.DataFrame([])
        for index in dt_iter:
            ix_df = self.get_univ_ix(index)
            univ = univ.append(ix_df)
        return univ.reset_index(drop=True)

    def get_univ_ix(self, index):
        '''
        Create features for a single index of eval, entry and exit dates.
        Pass hold_per and univ_size here.
        '''
        # SET INPUTS FOR DATA HANDLER AND GET DATA
        (eval_date, entry_date, exit_date) = index
        ExitCol = 'LEAD{}_Vwap'.format(self.hold_per)
        filter_args = {'filter': 'AvgDolVol',
                       'where': 'MarketCap >= 100 and Close_ >= 15',
                       'univ_size': self.univ_size}
        features = ['Vwap', ExitCol, 'GSECTOR', 'LAG1_RClose', 'SI', 
                    'LAG1_MarketCap', 'LAG1_AvgDolVol',
                    'LAG1_PRMA5_Close', 'LAG1_PRMA20_Close',
                    'LAG1_PRMA60_Close', 'LAG1_PRMA250_Close',
                    'LAG1_BOLL20_Close', 'LAG1_BOLL60_Close',
                    'LAG1_BOLL250_Close',
                    'LAG1_RSI5_Close', 'LAG1_RSI60_Close',
                    'LAG1_RSI120_Close',
                    'LAG1_DISCOUNT20_Close', 'LAG1_DISCOUNT60_Close',
                    'LAG1_DISCOUNT120_Close', 'LAG1_DISCOUNT250_Close',
                    'LAG1_VOL20_Close', 'LAG1_VOL60_Close',
                    'LAG1_VOL120_Close', 'LAG1_VOL250_Close',
                    'LAG1_MFI5_Close', 'LAG1_MFI20_Close']
        df = self.datahandler.get_filtered_univ_data(
            features=features,
            start_date=entry_date,
            end_date=entry_date,
            filter_date=eval_date,
            filter_args=filter_args)

        # HEDGE SOME VARS WITH SPY
        spy_features = ['Vwap', ExitCol,
                        'LAG1_PRMA5_Close', 'LAG1_PRMA20_Close',
                        'LAG1_PRMA60_Close', 'LAG1_PRMA250_Close']
        spy = self.datahandler.get_etf_data(
            ['SPY'],
            features=spy_features,
            start_date=entry_date,
            end_date=entry_date)

        for col in spy_features[2:]:
            df[col] -= float(spy[col])

        #  ADD RETURNS
        df['Ret'] = (df[ExitCol] / df.Vwap) - 1
        df['RetH'] = (df[ExitCol] / df.Vwap) - float(spy[ExitCol] / spy.Vwap)

        # RENAME COLUMNS
        skip_cols = ['Vwap', ExitCol, 'GSECTOR', 'SI']
        df.rename(columns={x:x.split('_')[1] for x in features if
                           x not in skip_cols}, inplace=True)

        # CREATE INDUSTRY AND RANK VARIABLES
        avg_cols = ['PRMA20', 'PRMA60', 'DISCOUNT60', 'DISCOUNT250',
                    'RSI60', 'RSI120', 'VOL20', 'VOL120', 'MFI20']
        ind_data = self._industry_avg(df.GSECTOR, df[avg_cols])
        df = df.merge(ind_data, left_index=True, right_index=True)

        rank_cols = ['DISCOUNT20', 'DISCOUNT60', 'DISCOUNT120', 'DISCOUNT250']
        rank_data = self._rank_cols(df[rank_cols])
        df = df.merge(rank_data, left_index=True, right_index=True)

        df.SI = df.SI.fillna(0.)

        return df.reset_index(drop=True)

    def _industry_avg(self, ind_series, base_data):
        '''
        Aggregate the series in base_data over the unique values in
        ind_series. Returns dataframe with median values for each series
        in base data by group.
        '''
        ind_data = pd.DataFrame(index = base_data.index)
        igrp = base_data.groupby(ind_series)
        igrps = igrp.groups

        for i in igrps.keys():
            iix = igrps[i]
            medians = base_data.loc[iix,:].median()

            for (col, m) in medians.iteritems():
                ind_data.loc[iix, 'Ind_{}'.format(col)] = m
        return ind_data

    def _rank_cols(self, base_data):
        '''
        Rank the series in base_data
        '''
        rank_data = pd.DataFrame(index = base_data.index)
        for col in base_data.columns:
            ranks = np.argsort(np.argsort(base_data[col]))
            rank_data.loc[:, 'Rank_{}'.format(col)] = ranks
        return rank_data
  
    def generate_signals(self, train, resp, test):
        '''
        Train various classes of models on using train and resp.  Return
        DataFrame with predicted values for each model using test. 
        '''
        # TRANSFORM TECHICAL VARS VIA PCA
        tech_cols = [x for x in train.columns if x.find('Ind') < 0]
        tech_cols.remove('SI')
        pca = PCA(n_components=3)
        pca.fit(train[tech_cols])
        pca_train = pca.transform(train[tech_cols])
        train.loc[:, 'PCA1'] = pca_train[:, 0]
        train.loc[:, 'PCA2'] = pca_train[:, 1]
        train.loc[:, 'PCA3'] = pca_train[:, 2]
        pca_test = pca.transform(test[tech_cols])
        test.loc[:, 'PCA1'] = pca_test[:, 0]
        test.loc[:, 'PCA2'] = pca_test[:, 1]
        test.loc[:, 'PCA3'] = pca_test[:, 2]

        model_cols = list(set(train.columns) - set(tech_cols))

        # Linear Regression
        lr = linear_model.LinearRegression()
        lr.fit(train[model_cols], resp)
        lr_preds = lr.predict(test[model_cols])

        # RF Regressor
        rfr_model = ensemble.RandomForestRegressor(n_estimators = 100,
                                  min_samples_split=100,
                                  min_samples_leaf=30,
                                  random_state=123)
        rfr_model.fit(train[model_cols], resp)
        rfr_preds = rfr_model.predict(test[model_cols])

        # Ridge Regression
        rdg = linear_model.Ridge(.7)
        rdg.fit(train[model_cols], resp)
        rdg_preds = rdg.predict(test[model_cols])

        # Bayesian Ridge Regression
        brdg = linear_model.BayesianRidge()
        brdg.fit(train[model_cols], resp)
        brdg_preds = brdg.predict(test[model_cols])

        # Classifiers
        binary_thresh = .0125
        bin_resp = resp >= binary_thresh
        #Linear Regression with Binary Response
        lrb1 = linear_model.LinearRegression()
        lrb1.fit(train[model_cols], bin_resp)
        lrb_preds1 = lrb1.predict(test[model_cols])

        #Logistic Regression
        lgr1 = linear_model.LogisticRegression()
        lgr1.fit(train[model_cols], bin_resp)
        lgr_preds1 = lgr1.predict_proba(test[model_cols])[:, 1]

        binary_thresh = .0225
        bin_resp = resp >= binary_thresh
        #Linear Regression with Binary Response
        lrb2 = linear_model.LinearRegression()
        lrb2.fit(train[model_cols], bin_resp)
        lrb_preds2 = lrb2.predict(test[model_cols])

        #Logistic Regression
        lgr2 = linear_model.LogisticRegression()
        lgr2.fit(train[model_cols], bin_resp)
        lgr_preds2 = lgr2.predict_proba(test[model_cols])[:,1]

        pred_arr = np.vstack([lr_preds, rfr_preds, rdg_preds, brdg_preds,
                              lrb_preds1, lgr_preds1, lrb_preds2, lgr_preds2])
        pred_df = pd.DataFrame(data=pred_arr.transpose(),
                               columns=['LR','RFR','RDG','BRDG','LRB1','LGR1',
                                        'LRB2', 'LGR2'])
        for col in pred_df:
            pred_df.loc[:, col] = np.argsort(np.argsort(pred_df[col]))
        return pred_df

    def _get_selection_params(self, signals):
        '''
        Uses the self.strategies object to determine which meta param
        settings to use for the test period.  Returns an aggregated signal
        and  number of stocks on each side.
        '''
        if not hasattr(self, 'strategies'):
            n = 100
            cid = np.nan
            agg_signal = signals.mean(axis=1)
        else:
            grp = self.strategies.groupby(['cID', 'n'])
            df = pd.DataFrame(grp.RetLS.mean())
            df['cID'] = [x[0] for x in df.index]
            df['n'] = [x[1] for x in df.index]
            df.reset_index(drop=True, inplace=True)
            df.sort_values('RetLS', inplace=True)
            cid = df.iloc[-1]['cID']
            n = df.iloc[-1]['n']
            sel_models = self.model_combs[int(cid)]
            agg_signal = signals.iloc[:, sel_models].mean(axis=1)
        return np.array(agg_signal), n, cid

    def get_portfolio_ix(self, industries, n, ind_pct=.25):
        '''
        Select the top and bottom n indicies from industries series limiting
        the total number in each group to ind_pct of the total. Returns
        two lists with long and short indices.
        '''
        max_i = int(n * ind_pct)
        ind_counts_long = {x:0 for x in industries.unique()}
        ind_counts_short = {x:0 for x in industries.unique()}
        sel_ix_long = []
        sel_ix_short = []

        while (len(sel_ix_long) < n) and (len(industries) > 0):
            ix = industries.index[-1]
            ind = industries.pop(ix)
            if ind_counts_long[ind] < max_i:
                sel_ix_long.append(ix)
                ind_counts_long[ind] += 1

        while (len(sel_ix_short) < n) and (len(industries) > 0):
            ix = industries.index[0]
            ind = industries.pop(ix)
            if ind_counts_short[ind] < max_i:
                sel_ix_short.append(ix)
                ind_counts_short[ind] += 1

        return sel_ix_long, sel_ix_short

    def _get_dailys_rets(self, longs, shorts, start, end):
        '''
        Get the daily returns of a list of long and short SecCodes between
        start and end dates.  Apply transaction cost.
        ''' 
        # Equity Returns
        returns = self.datahandler.get_id_data(
            ids=longs + shorts,
            features=['Vwap', 'Close'],
            start_date=start,
            end_date=end)
        returns.sort_values(['ID', 'Date'], inplace=True)

        # Could do this all at once, concern is missing data        
        for i in returns.ID.unique():
            cost = self.COST if i in longs else self.COST * -1
            tmp = returns[returns.ID == i].copy()
            tmp['PriorClose'] = tmp.Close.shift(1)
            tmp['R1'] = tmp.Close / tmp.Vwap
            tmp['R2'] = tmp.Close / tmp.PriorClose
            tmp['R3'] = tmp.Vwap / tmp.PriorClose
            tmp['Ret'] = tmp.R2
            tmp.loc[tmp.Date == start, 'Ret']  = tmp.R1 - (cost / 2)
            tmp.loc[tmp.Date == end, 'Ret']  = tmp.R3 - (cost / 2)
            returns.loc[tmp.index, 'Ret'] = tmp.Ret

        returns.Ret -= 1
        return returns

    def iterate_meta_params(self, signals, test):
        '''
        Iterate through all meta parameter settings using the test data set
        and save results to be used for parameter selection.
        '''
        # ITERABLES
        port_size = [50, 60, 70, 80, 90, 100]

        if not hasattr(self, 'strategies'):
            self.strategies = pd.DataFrame(columns=['Year', 'n', 'cID',
                                                    'RetL', 'RetS', 'RetLS'],
                                           dtype=float)
            self.model_combs = []
            for l in range(3, signals.shape[1]):
               for c in itertools.combinations(range(signals.shape[1]), l):
                    self.model_combs.append(c)

        # SET ITERABLE DF
        signals.index = test.index
        signals.loc[:, 'SecCode'] = test.SecCode.copy()
        signals.loc[:, 'Ret'] = test.Ret.copy()
        signals.loc[:, 'GSECTOR'] = test.GSECTOR.copy()
        signals.loc[:, 'Date'] = test.Date.copy()
        
        for i, c in enumerate(self.model_combs):
            signals.loc[:,'Signal'] = signals.iloc[:, c].mean(axis=1)
            signals.sort_values('Signal', inplace=True)
            for p in port_size:
                ix = len(self.strategies)
                long_ix, short_ix = self.get_portfolio_ix(
                    signals.GSECTOR.copy(), p)
                self.strategies.loc[ix, 'Year'] = test.iloc[0,:]['Date'].year
                self.strategies.loc[ix, 'n'] = p
                self.strategies.loc[ix, 'cID'] = i
                LongRet = signals.loc[long_ix, 'Ret'].mean()
                ShortRet = signals.loc[short_ix, 'Ret'].mean()
                self.strategies.loc[ix, 'RetL'] = LongRet
                self.strategies.loc[ix, 'RetS'] = ShortRet
                self.strategies.loc[ix, 'RetLS'] = LongRet - ShortRet
        return


if __name__ == '__main__':

    ye = YearEnd(hold_per = 4, univ_size = 1600, exit_offset=0)
    ye.start()

    # Build entire research df
    univ = ye.get_univ_iter(ye.date_iter)
    univ = pd.read_csv('C:/temp/yearend.csv')


import os
import numpy as np
import pandas as pd
import datetime as dt
from sklearn import linear_model
from sklearn import ensemble 
from sklearn.decomposition import PCA
from ram.strategy.base import Strategy


class YearEnd(Strategy):

    COST = 0.0015

    def __init__(self, hold_per=5, univ_size=1000, port_n=50, ind_n=np.inf,
                 output_dir=None):
        super( YearEnd, self ).__init__()
        self.date_iter = self._get_date_iterator(hold_per)
        self.hold_per = hold_per
        self.univ_size = univ_size
        self.port_n = port_n
        self.ind_n = ind_n
        self.signals = pd.DataFrame([])

    def get_iter_index(self):
        return self.date_iter[5:]

    def _get_date_iterator(self, hold_per=5):
        """
        Produce iterable with revlevant dates (eval_dt, entry_dt, exit_dt)
        for each event (year end)
        """
        db_dates = self.datahandler.get_all_dates()

        fdoy = pd.date_range('2000-01-01', '2016-01-01', freq='AS')
        fdoy = np.array([x.to_pydatetime() for x in fdoy])
        fbdoy =  np.array([db_dates[db_dates > x].min() for x in fdoy])
        fbdix = np.array([np.where(db_dates == x)[0][0] for x in fbdoy])

        exitix = fbdix - 1
        entryix = exitix - hold_per
        evalix = entryix - 1

        iterable = zip(db_dates[evalix], db_dates[entryix], db_dates[exitix])
        return iterable

    def run_index(self, index):
        ix = self.date_iter.index(index)

        # Generate training and test data frames
        if hasattr(self, 'priortrain'):
            train = self.priortrain.append(self.priortest)
            train.reset_index(drop=True, inplace=True)
        else:
            train = self.get_univ_iter(self.date_iter[ : ix])

        test = self.get_univ_ix(index)
        self.priortrain = train.copy()
        self.priortest = test.copy()
        train = train.dropna()
        test = test.dropna()

        # GET SIGNALS
        features = list(set(train.columns) - set(
            ['SecCode', 'Date', 'Vwap', 'LEAD{}_Vwap'.format(self.hold_per),
             'RClose', 'MarketCap', 'AvgDolVol', 'GSECTOR', 'Ret', 'RetH']))
        signals = self.generate_signals(train[features].copy(), train.RetH,
                                        test[features].copy())
        signal_ranks = np.argsort(np.argsort(signals[0]))

        # SET PORTFOLIO
        ranks = pd.DataFrame(data={'SecCode':test.SecCode,
                                   'Ind':test.GSECTOR,
                                   'Rank':signal_ranks})
        ranks.sort('Rank', inplace=True)
        port_ix = self._get_portfolio_ix(ranks.Rank,
                                         ranks.Ind,
                                         n = self.port_n,
                                         max_i = self.ind_n)

        # Get daily returns
        codes = list(test.loc[port_ix, 'SecCode'])
        rets = self._get_dailys_rets(codes, index[1], index[2])
        grp = rets.groupby('Date')
        port_rets = pd.DataFrame(grp.RetH.mean())
        
        # Save allocs
        signal_df = signals[1]
        signal_df.index = test.index
        signal_df.loc[:, 'Ret'] = test.Ret.copy()
        signal_df.loc[:, 'RetH'] = test.RetH.copy()
        signal_df.loc[:, 'SecCode'] = test.SecCode.copy()
        signal_df.loc[:, 'GSECTOR'] = test.GSECTOR.copy()
        signal_df.loc[:, 'Date'] = test.Date.copy()
        self.signals = self.signals.append(signal_df)
        self.signals.reset_index(drop=True, inplace=True)
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
        # Set dates and args for sql datahandler
        (eval_date, entry_date, exit_date) = index

        ExitCol = 'LEAD{}_Vwap'.format(self.hold_per)
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
                    'LAG1_MFI5_Close', 'LAG1_MFI20_Close'
                    ]

        # Pull data and filter to date of interest
        filter_args = {'filter': 'AvgDolVol',
                       'where': 'MarketCap >= 100 and Close_ >= 15',
                       'univ_size': self.univ_size}

        df = self.datahandler.get_filtered_univ_data(
            features=features,
            start_date=entry_date,
            end_date=entry_date,
            filter_date=eval_date,
            filter_args=filter_args)

        # Hedge these vars with SPY
        spy_features = ['Vwap', ExitCol,
                        'LAG1_PRMA5_Close', 'LAG1_PRMA20_Close',
                        'LAG1_PRMA60_Close', 'LAG1_PRMA250_Close']
        spy = self.datahandler.get_etf_data(
            ['SPY'],
            features=spy_features,
            start_date=entry_date,
            end_date=entry_date)

        # Hedge Additional measures
        for col in spy_features[2:]:
            df[col] -= float(spy[col])

        #  Add Returns
        df['Ret'] = (df[ExitCol] / df.Vwap) - 1
        df['RetH'] = (df[ExitCol] / df.Vwap) - float(spy[ExitCol] / spy.Vwap)

        # Rename columns
        skip_cols = ['Vwap', ExitCol, 'GSECTOR', 'SI']
        df.rename(columns={x:x.split('_')[1] for x in features if
                           x not in skip_cols}, inplace=True)

        # Columns to average performance over Industry
        avg_cols = ['PRMA20', 'PRMA60', 'DISCOUNT60', 'DISCOUNT250',
                    'RSI60', 'RSI120', 'VOL20', 'VOL120', 'MFI20']
        ind_data = self._industry_avg(df.GSECTOR, df[avg_cols])
        df = df.merge(ind_data, left_index=True, right_index=True)

        # Columns to add Rank Vars 
        rank_cols = ['DISCOUNT20', 'DISCOUNT60', 'DISCOUNT120', 'DISCOUNT250']
        rank_data = self._rank_cols(df[rank_cols])
        df = df.merge(rank_data, left_index=True, right_index=True)

        # Temp fix for na SI values
        df.SI = df.SI.fillna(0)

        return df.reset_index(drop=True)

    def _industry_avg(self, ind_series, base_data):
        '''
        Aggregate the series in avg_data over the unique values in ind_series.
        Attempts to quantify how an industry has performed as a whole.
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
  
    def _get_portfolio_ix(self, scores, industries, n, max_i=np.inf):
        '''
        Select a group of n scores limiting the total in any industry
        by the max_i parameter. Returns the index of selection.
        '''
        ind_counts = {x:0 for x in industries.unique()}
        sel_ix = []
        
        while (len(sel_ix) < n) and (len(scores) > 0):
            ix = scores.index[-1]
            ind = industries.pop(ix)
            score = scores.pop(ix)
            if ind_counts[ind] < max_i:
                sel_ix.append(ix)
                ind_counts[ind] += 1
        return sel_ix

    def _get_dailys_rets(self, codes, start, end):
        '''
        Get the daily returns of a list of SecCodes between start and
        end dates.
        '''
        # SPY Returns
        spy = self.datahandler.get_etf_data(
            ['SPY'],
            features=['Vwap', 'Close'],
            start_date=start,
            end_date=end)
        spy['PriorClose'] = spy.Close.shift(1)
        spy['R1'] = spy.Close / spy.Vwap
        spy['R2'] = spy.Close / spy.PriorClose
        spy['R3'] = spy.Vwap / spy.PriorClose
        spy['MktRet'] = spy.R2
        spy.loc[spy.Date == start, 'MktRet']  = spy.R1
        spy.loc[spy.Date == end, 'MktRet']  = spy.R3
        
        # Equity Returns
        returns = self.datahandler.get_id_data(
            ids=codes,
            features=['Vwap', 'Close'],
            start_date=start,
            end_date=end)
        returns.sort(['ID','Date'], inplace=True)

        # Could do this all at once, concern is missing data        
        for i in returns.ID.unique():
            tmp = returns[returns.ID == i].copy()
            tmp['PriorClose'] = tmp.Close.shift(1)
            tmp['R1'] = tmp.Close / tmp.Vwap
            tmp['R2'] = tmp.Close / tmp.PriorClose
            tmp['R3'] = tmp.Vwap / tmp.PriorClose
            tmp['Ret'] = tmp.R2
            tmp.loc[tmp.Date == start, 'Ret']  = tmp.R1 - (self.COST / 2)
            tmp.loc[tmp.Date == end, 'Ret']  = tmp.R3 - (self.COST / 2)
            returns.loc[tmp.index, 'Ret'] = tmp.Ret
        
        returns = returns.merge(spy[['Date', 'MktRet']], how='left')
        returns['RetH'] = returns.Ret - returns.MktRet
        returns.Ret -= 1
        returns.MktRet -= 1
        return returns

    def generate_signals(self, train, resp, test):
        '''
        Produce a  measure which can be sorted and used as selection
        criteria for portfolio formation.  Higher vals are selected first.
        '''
        # Reduce technicals to PCAs
        tech_cols = [x for x in train.columns if x.find('Ind') < 0]
        tech_cols.remove('SI')
        pca = PCA(n_components=3)
        pca.fit(train[tech_cols])
        pca_vars = pca.transform(train[tech_cols])
        train.loc[:, 'PCA1'] = pca_vars[:, 0]
        train.loc[:, 'PCA2'] = pca_vars[:, 1]
        train.loc[:, 'PCA3'] = pca_vars[:, 2]
        pca_vars = pca.transform(test[tech_cols])
        test.loc[:, 'PCA1'] = pca_vars[:, 0]
        test.loc[:, 'PCA2'] = pca_vars[:, 1]
        test.loc[:, 'PCA3'] = pca_vars[:, 2]

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
        lgr_preds1 = lgr1.predict_proba(test[model_cols])[:,1]
        
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

        return lr_preds, pred_df




if __name__ == '__main__':
    
    import ipdb; ipdb.set_trace()

    
    ye = YearEnd(hold_per = 4, univ_size = 1600, port_n=50, ind_n=13)
    ye.start()
    print ye.results
    signals = ye.signals.copy()
    

    # Build entire research df
    univ = ye.get_univ_iter(ye.date_iter)
    univ = pd.read_csv('C:/temp/yearend.csv')


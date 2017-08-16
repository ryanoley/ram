
import numpy as np
import pandas as pd
import datetime as dt

COST = .0015

class PortfolioConstructor2(object):

    def __init__(self, booksize=10e6):
        pass

    def get_args(self):
        return {
            'pct_side': [0.05, 0.10],
            'hold_days': [5]
        }

    def get_daily_pl(self, data_container, pct_side, hold_days):
        """
        Parameters
        ----------
        """
        
        test = data_container.test_data.copy()

        vwaps = test.pivot(index='Date', columns='SecCode', values='AdjVwap')
        preds = test.pivot(index='Date', columns='SecCode', values='preds')
        ernflag = test.pivot(index='Date', columns='SecCode', values='EARNINGSFLAG')

        blackout = ernflag.rolling(window=(hold_days + 1), min_periods=0).sum()
        blackout.fillna(1., inplace=True)

        exit_vwaps = vwaps.copy()
        exit_vwaps[:] = np.where(blackout==1., np.nan, exit_vwaps)
        returns =  (exit_vwaps.shift(-hold_days) / vwaps) - 1

        preds = preds.shift(1).iloc[1::hold_days, :].fillna(0.)
        returns = returns.iloc[1::hold_days, :]
        returns.iloc[-1] = 0.
        
        n_securities = np.round(vwaps.shape[1] * pct_side, 0).astype(int)
        long_ixs = np.argsort(preds).iloc[:, -n_securities:]
        short_ixs = np.argsort(preds).iloc[:, :n_securities]
        
        row_ix = np.arange(len(returns))[:, np.newaxis]
        long_rets =  np.array(returns)[row_ix, long_ixs.values] - COST
        short_rets =  np.array(returns)[row_ix, short_ixs.values] + COST
    
        out = pd.DataFrame(index=returns.index,
                           data={'Long': np.nanmean(short_rets, axis=1),
                                 'Short': np.nanmean(long_rets, axis=1)})
        out['Ret'] = out.Long - out.Short
        
        return out


import numpy as np
import pandas as pd
import datetime as dt

COST = .0015
AUM = 10e6

class PortfolioConstructor1(object):

    def __init__(self, booksize=10e6):
        pass

    def get_args(self):
        return {
            'thresh': [0.01, 0.02, .04],
            'hold_days': [10, 20, 30, 40],
            'pos_size': [.01],
        }


    def get_daily_pl(self, data_container, thresh, hold_days, pos_size):
        """
        Parameters
        ----------
        """

        test = data_container.test_data.copy()
        
        assert 'Ret{}'.format(hold_days) in test.columns
        test['Ret'] = test['Ret{}'.format(hold_days)].copy()

        longs = test[test.preds >= thresh].copy()
        shorts = test[test.preds <= -thresh].copy()
        longs = longs.groupby('Date')
        shorts = shorts.groupby('Date')

        out = pd.DataFrame(index=test.Date.unique())
        out['Long'] = longs.Ret.mean() - COST
        out['nLong'] = longs.Date.count()
        out['Short'] = shorts.Ret.mean() + COST
        out['nShort'] = shorts.Date.count()
        out.fillna(0., inplace=True)
        out['Ret'] = out.Long - out.Short
        out['nPos'] = out.nLong + out.nShort
        out.sort_index(inplace=True)
        out.Ret *= (out.nPos * pos_size)
        return out





import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.basic.constructor.portfolio import Portfolio
from ram.strategy.starmine.constructor.base_constructor import Constructor


COST = .0015
AUM = 10e6

class PortfolioConstructor1(Constructor):

    def __init__(self, booksize=10e6):
        pass

    def get_args(self):
        return {
            'thresh': [0.005, 0.01, 0.015, 0.02],
            'pos_size': [.002, .004, .006]
        }


    def get_daily_pl_old(self, data_container, signals, thresh, **kwargs):
        """
        Parameters
        ----------
        """

        test = data_container.test_data.copy()
        hold_days = kwargs['hold_days']
        pos_size = kwargs['pos_size']
        
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


    def get_position_sizes(self, scores, daily_pl_data, thresh, **kwargs):
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.
        """
        scores = pd.Series(scores).to_frame()
        scores.columns = ['score']

        scores['weights'] = np.where(scores > thresh, 0 ,
                                     np.where(scores < thresh, -1, 0))

        return scores.weights.to_dict()




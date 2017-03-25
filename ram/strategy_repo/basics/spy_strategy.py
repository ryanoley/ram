import os
import itertools
import datetime as dt

import pandas_datareader.data as web

from ram.strategy_repo.base import CommittedStrategy


class SpyBasics(CommittedStrategy):

    def get_meta_data(self):
        return {
            'description': 'Basic SPY strategies'
        }

    def get_daily_returns(self):
        start = dt.datetime(2000, 1, 1)
        end = dt.datetime.utcnow()
        df = web.DataReader("SPY", "yahoo", start, end)

        output = pd.DataFrame([])
        for fast, slow in itertools.product(
                [10, 20, 40, 80], [100, 200, 500]):
            output['MA_{}_{}'.format(fast, slow)] = make_ewma_return(
                df, fast, slow)

        for offset in [0, 4, 8, 15, 20]:
            output['QT_{}'.format(offset)] = make_period_start_return(
                df, offset, per='q')

        for offset in [0, 4, 8, 15, 20]:
            output['MT_{}'.format(offset)] = make_period_start_return(
                df, offset, per='m')

        return output


def make_ewma_return(data, fast, slow):
    data = data.copy()
    data['fast_ma'] = data.Close.ewm(fast).mean()
    data['slow_ma'] = data.Close.ewm(slow).mean()
    data['ret'] = data.Close.pct_change()
    data['side'] = np.where(data.fast_ma > data.slow_ma, 1, -1)
    data['side'] = data.side.shift(1)
    return (data.side * data.ret).dropna()


def make_period_start_return(data, day_offset=0, per='m'):
    data = data.copy()
    data['month'] = [x.month for x in data.index]
    data['qtr'] = [x.quarter for x in data.index]
    if per == 'm':
        data['period_start'] = data.month.diff() != 0
    else:
        data['period_start'] = data.qtr.diff() != 0
    data.period_start = data.period_start.shift(day_offset)
    data['period_benchmark'] = np.where(data.period_start, data.Close, np.nan)
    data.period_benchmark = data.period_benchmark.fillna(method='pad')
    data['side'] = np.where(data.Close >= data.period_benchmark, 1, -1)
    data['side'] = data.side.shift(1)
    return data.side * data.ret


if __name__ == '__main__':
    SpyBasics().get_daily_returns()

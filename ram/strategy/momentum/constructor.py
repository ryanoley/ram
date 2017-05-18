import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.momentum.portfolio import Portfolio


class MomentumConstructor(object):

    def __init__(self):
        pass

    def get_iterable_args(self):
        pass

    def get_daily_returns(self, data):
        # Parse and format data
        m_factors, m_prices, uniq_dates = self._format_data(data)
        # Iterate
        port = Portfolio()
        output = pd.DataFrame({'Ret': 0}, index=uniq_dates)
        for date in uniq_dates:
            output.loc[date, 'Ret'] = port.update_prices(m_prices[date])
            # Convert factors into
            factor = m_factors.loc[date].sort_values()
            longs = factor.iloc[-50:].copy()
            shorts = factor.iloc[:50].copy()
            longs[:] = 1
            shorts[:] = -1
            alphas = longs.append(shorts).to_dict()
            port.update_positions(alphas, m_prices[date])
        return output, {}

    # ~~~~~~ DATA ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _format_data(self, data):
        # Get dates
        uniq_dates = data.Date[data.TestFlag].unique()
        # Right now get dates for only one month forward
        qtrs = np.array([(d.month-1)/3+1 for d in uniq_dates])
        inds = np.where(qtrs == qtrs[0])[0]
        inds = np.append(inds, [inds[-1]+1])
        uniq_dates = uniq_dates[inds]
        # Add one extra day because new portfolio will be constructed
        # in next quarter on this day and will have 0 PL
        data = data[data.Date.isin(uniq_dates)].copy()
        data['Factor'] = data.MA5_AdjClose / data.MA80_AdjClose
        factor = data.pivot(index='Date', columns='SecCode',
                            values='Factor')
        prices = data.pivot(index='Date', columns='SecCode',
                            values='AdjClose').T.to_dict()
        return factor, prices, uniq_dates

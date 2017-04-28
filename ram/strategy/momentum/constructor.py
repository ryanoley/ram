import numpy as np


class MomentumConstructor(object):

    def __init__(self):
        pass

    def get_daily_returns(self, data):
        # Get dates
        uniq_dates = data.Date[data.TestFlag].unique()
        # Right now get dates for only one month forward
        qtrs = np.array([(d.month-1)/3+1 for d in uniq_dates])
        uniq_dates = uniq_dates[qtrs == qtrs[0]]
        # Parse and format data
        factor, prices = self._format_data(data, uniq_dates)
        # Iterate
        for date in uniq_dates:
            pass

    def _format_data(self, data, dates):
        pass

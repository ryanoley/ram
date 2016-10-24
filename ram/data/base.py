import numpy as np
import datetime as dt

from gearbox import convert_date_array
from ram.utils.time_funcs import check_input_date


class DataHandler(object):

    def __init__(self, data):
        assert 'ID' in data.columns
        assert 'Date' in data.columns
        data['Date'] = convert_date_array(data['Date'])
        self._data = data
        self._dates = np.unique(data.Date)

    def get_filtered_univ_data(self,
                               univ_size,
                               features,
                               start_date,
                               filter_date,
                               end_date,
                               filter_column,
                               filter_bad_ids=False):
        """
        Purpose of this class is to provide an interface to get a filtered
        universe.

        Parameters
        ----------
        univ_size : int
        features : list
        start_date/filter_date/end_date : datetime
        filter_column : str
        filter_bad_ids : bool

        Returns
        -------
        data : pandas.DataFrame
            With columns ID, Date representing a unique observation.
        """
        # Check user input
        assert isinstance(univ_size, int)
        if not isinstance(features, list):
            features = [features]
        start_date = check_input_date(start_date)
        filter_date = check_input_date(filter_date)
        end_date = check_input_date(end_date)

        # FILTER
        data = self._data

        # Confirm date has data. Otherwise, trading date just before
        filter_date = self._dates[self._dates <= filter_date][-1]

        # On this filter date, return all values to filter from
        vals = data.loc[data.Date == filter_date, filter_column].dropna()
        # Get the ranks of these values. Highest values ranked lowest
        ranks = (-1 * vals).argsort().argsort()
        # Get the ids for top ranked values
        ids = data.loc[ranks[ranks < univ_size].index, 'ID']

        # Filter rows by date and ids
        inds = (data.Date >= start_date) & \
               (data.Date <= end_date) & \
               (data.ID.isin(ids))

        # Filter columns
        col_inds = ['Date', 'ID'] + features

        return data.loc[inds, col_inds].reset_index(drop=True)

 
    def get_id_data(self,
                    ids,
                    features,
                    start_date,
                    end_date):
        """
        Purpose of this class is to provide an interface to get a filtered
        universe.

        Parameters
        ----------
        ids : list
        features : list
        start_date/end_date : datetime

        Returns
        -------
        data : pandas.DataFrame
            With columns ID, Date representing a unique observation.
        """
        # Check user input
        if not isinstance(ids, list):
            ids = [ids]
        if not isinstance(features, list):
            features = [features]
        start_date = check_input_date(start_date)
        end_date = check_input_date(end_date)

        # Filter rows by date and ids
        data = self._data
        inds = (data.Date >= start_date) & \
               (data.Date <= end_date) & \
               (data.ID.isin(ids))

        # Filter columns
        col_inds = ['Date', 'ID'] + features

        return data.loc[inds, col_inds].reset_index(drop=True)

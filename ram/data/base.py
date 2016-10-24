import datetime as dt


class DataHandler(object):

    def __init__(self, data):
        self.__data = data

    def get_filtered_univ_data(self,
                               univ_size,
                               features,
                               start_date,
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
        start_date/end_date : datetime
        filter_column : str
        filter_bad_ids : bool

        Returns
        -------
        data : pandas.DataFrame
            With columns ID, Date representing a unique observation.
        """
        pass

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
        pass

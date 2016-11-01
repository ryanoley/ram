import pypyodbc
import numpy as np
import pandas as pd

from ram.data.base import DataHandler

from ram.utils.time_funcs import check_input_date
from ram.data.sql_features import sqlcmd_from_feature_list


class DataHandlerSQL(DataHandler):

    def __init__(self):

        try:
            connection = pypyodbc.connect('Driver={SQL Server};'
                                          'Server=QADIRECT;'
                                          'Database=ram;'
                                          'uid=ramuser;pwd=183madison')
        except:
            # Mac/Linux implementation. unixODBC and FreeTDS works
            connect_str = "DSN=qadirectdb;UID=ramuser;PWD=183madison"
            connection = pypyodbc.connect(connect_str)

        assert connection.connected == 1

        self._connection = connection
        self._cursor = connection.cursor()

        # Get all dates available in master database
        self._dates = np.unique(self.sql_execute(
            "select distinct Date_ " +
            "from ram.dbo.ram_master " +
            "order by Date_"
        )).flatten()

    def get_filtered_univ_data(self,
                               features,
                               start_date,
                               end_date,
                               univ_size=None,
                               filter_date=None):
        """
        Purpose of this class is to provide an interface to get a filtered
        universe.

        Parameters
        ----------
        features : list
        start_date/filter_date/end_date : string/datetime
        univ_size : int

        Returns
        -------
        data : pandas.DataFrame
            With columns ID, Date representing a unique observation.
        """
        # Hard-coded for now because there is only one filter needed.
        FILTER_COL = 'AvgDolVol'

        # Check user input
        d1, d2, d3 = _format_dates(start_date, filter_date, end_date)

        features, colselect = sqlcmd_from_feature_list(features)

        if filter_date:

            # Get next business date from filter date
            bdate = self.sql_execute(
                "select T0 from ram.dbo.trading_dates " +
                "where CalendarDate = '{0}'".format(d2)
            )[0][0]

            # Get IDs using next business date(filter_date)
            ids = np.array(self.sql_execute(
                "select top {0} IdcCode ".format(univ_size) +
                "from ram.dbo.ram_master " +
                "where Date_ = '{0}' ".format(bdate) +
                "order by {0} desc".format(FILTER_COL)
            )).flatten()
            _, idsstr = _format_ids(ids)

            univ = self.sql_execute(
                "select IdcCode, Date_, {0} ".format(colselect) +
                "from ram.dbo.ram_master " +
                "where Date_ between '{0}' and '{1}'".format(d1, d3) +
                "and IdcCode in " + idsstr
            )

        else:
            univ = self.sql_execute(
                "select IdcCode, Date_, {0} ".format(colselect) +
                "from ram.dbo.ram_master " +
                "where Date_ between '{0}' and '{1}'".format(d1, d3)
            )

        univ_df = pd.DataFrame(univ)
        univ_df.columns = ['ID', 'Date'] + features
        return univ_df

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
        d1, _, d3 = _format_dates(start_date, None, end_date)

        # Format ids. If just a single
        ids, idsstr = _format_ids(ids)

        # THIS SHOULD BE HANDLED BETTER
        table = 'ram_master'
        if 'SPY' in ids:
            ids.remove('SPY')
            ids.append('59751')
            table = 'ram_master_etf'
        if 'VXX' in ids:
            ids.remove('VXX')
            ids.append('140062')
            table = 'ram_master_etf'

        # Format ids. If just a single
        ids, idsstr = _format_ids(ids)

        features, colselect = sqlcmd_from_feature_list(features)

        # Get data using start_date, end_date, and ids from filter
        univ = self.sql_execute(
            "select IdcCode, Date_, {0} ".format(colselect) +
            "from ram.dbo.{0} ".format(table) +
            "where Date_ between '{0}' and '{1}' ".format(d1, d3) +
            "and IdcCode in " + idsstr
        )

        univ_df = pd.DataFrame(univ)
        univ_df.columns = ['ID', 'Date'] + features
        return univ_df

    def get_all_dates(self):
        return self._dates

    def sql_execute(self, sqlcmd):
        try:
            self._cursor.execute(sqlcmd)
            return self._cursor.fetchall()
        except Exception, e:
            print 'error running sqlcmd: ' + str(e)
            return []


def _format_dates(start_date, filter_date, end_date):
        return check_input_date(start_date), \
            check_input_date(filter_date), \
            check_input_date(end_date)


def _format_ids(ids):
    """
    Takes in individual or list of ids, and returns a list/array
    of those ids, and a string used to query sql database.
    """
    if isinstance(ids, str):
        ids = [ids]
    if hasattr(ids, '__iter__'):
        idsstr = str([str(i) for i in ids])
        idsstr = idsstr.replace('[', '(').replace(']', ')')
        return ids, idsstr
    else:
        raise '_format_ids : ids not iterable'


if __name__ == '__main__':

    # EXAMPLES
    dh = DataHandlerSQL()

    univ = dh.get_filtered_univ_data(
        univ_size=100,
        features=['Close', 'Close_', 'ADJClose_', 'PRMA10', 'BOLL20'],
        start_date='2016-10-01',
        end_date='2016-10-20',
        filter_date='2016-10-01',)

    univ = dh.get_filtered_univ_data(
        features=['High', 'Low', 'Close'],
        start_date='2016-10-01',
        end_date='2016-10-10')

    univ = dh.get_id_data(ids=[43030, 50183],
                          features=['High', 'Low', 'Close'],
                          start_date='2016-10-01',
                          end_date='2016-10-20')

    univ = dh.get_id_data(ids=43030,
                          features=['High', 'Low', 'Close'],
                          start_date='2016-10-01',
                          end_date='2016-10-20')

    univ = dh.get_id_data(ids='VXX',
                          features=['High', 'Low', 'Close'],
                          start_date='2016-10-01',
                          end_date='2016-10-20')

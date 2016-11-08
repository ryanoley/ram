import pypyodbc
import numpy as np
import pandas as pd
import datetime as dt

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
            """
            select distinct Date_
            from ram.dbo.ram_master_equities
            order by Date_;
            """
        )).flatten()

    def _get_filtered_ids(self, filter_date, args):
        univ_size = args['univ_size']
        filter_col = args['filter'] if 'filter' in args else 'AvgDolVol'
        where = 'and {0}'.format(args['where']) if 'where' in args else ''

        # Get exact filter date
        fdate = self._dates[self._dates <= filter_date][-1]

        # Get IDs using next business date(filter_date)
        ids = np.array(self.sql_execute(
            """
            select top {0} IdcCode
            from ram.dbo.ram_master_equities
            where Date_ = '{1}'
            and NormalTradingFlag = 1
            {2}
            order by {3} desc;
            """.format(univ_size, fdate, where, filter_col)
        )).flatten()
        return ids

    def get_filtered_univ_data(self,
                               features,
                               start_date,
                               end_date,
                               filter_date,
                               univ_size=None,
                               filter_args=None):
        """
        Purpose of this class is to provide an interface to get a filtered
        universe.

        Parameters
        ----------
        features : list
        start_date/filter_date/end_date : string/datetime
        count : int
        filter_args : dict
            Should have elements: count, where, and order

        Returns
        -------
        data : pandas.DataFrame
            With columns ID, Date representing a unique observation.
        """
        # Check user input
        d1, d2, d3 = _format_dates(start_date, filter_date, end_date)

        if not filter_args:
            assert univ_size, 'Must provide filter args or univ_size'
            filter_args = {'univ_size': univ_size}

        if filter_date:
            ids = self._get_filtered_ids(d2, filter_args)
        else:
            ids = []

        # Get features, and strings for cte and regular query
        sqlcmd = sqlcmd_from_feature_list(features, ids, d1, d3)

        univ = self.sql_execute(sqlcmd)

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

        # Get features, and strings for cte and regular query
        sqlcmd = sqlcmd_from_feature_list(features, ids, d1, d3)

        univ = self.sql_execute(sqlcmd)

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

    def get_etf_data(self,
                     tickers,
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

        ids = self._map_ticker_to_id(tickers)

        # Get features, and strings for cte and regular query
        sqlcmd = sqlcmd_from_feature_list(features, ids.ID.tolist(),
                                          d1, d3, 'ram.dbo.ram_master_etf')
        univ = self.sql_execute(sqlcmd)

        univ_df = pd.DataFrame(univ)
        univ_df.columns = ['ID', 'Date'] + features
        univ_df = univ_df.merge(ids)
        univ_df.ID = univ_df.Ticker
        univ_df = univ_df.drop('Ticker', axis=1)
        return univ_df

    def _map_ticker_to_id(self, tickers):
        if isinstance(tickers, str):
            tickers = [tickers]
        # Get Ticker, IdcCode mapping
        ids = pd.DataFrame(self.sql_execute(
            "select distinct IdcCode, Ticker "
            "from ram.dbo.ram_master_etf;"), columns=['ID', 'Ticker'])
        return ids[ids.Ticker.isin(tickers)]


def _format_dates(start_date, filter_date, end_date):
        return check_input_date(start_date), \
            check_input_date(filter_date), \
            check_input_date(end_date)


if __name__ == '__main__':

    # EXAMPLES
    dh = DataHandlerSQL()

    univ = dh.get_etf_data(
        tickers=['SPY', 'VXX'],
        features=['Close', 'RClose', 'AvgDolVol', 'LAG1_AvgDolVol'],
        start_date='2016-06-01',
        end_date='2016-10-20')

    filter_args = {'filter': 'AvgDolVol', 'where': 'MarketCap >= 200',
                   'univ_size': 20}
    univ = dh.get_filtered_univ_data(
        features=['BOLL30_Close', 'LAG2_BOLL30_Close', 'Close'],
        start_date='2016-06-01',
        end_date='2016-10-20',
        filter_date='2016-06-01',
        filter_args=filter_args)

    univ = dh.get_filtered_univ_data(
        univ_size=10,
        features=['Close', 'RClose', 'AvgDolVol', 'LAG1_AvgDolVol'],
        start_date='2016-06-01',
        end_date='2016-10-20',
        filter_date='2016-06-01')

    univ = dh.get_id_data(
        ids=[43030],
        features=['Close', 'RClose', 'AvgDolVol', 'LAG1_AvgDolVol'],
        start_date='2016-06-01',
        end_date='2016-10-20')

import pypyodbc
import numpy as np
import pandas as pd
import datetime as dt

from ram.data.base import DataHandler

from ram.utils.time_funcs import check_input_date
from ram.data.sql_features import sqlcmd_from_feature_list


class DataHandlerSQL(DataHandler):

    def __init__(self, table='ram.dbo.ram_master_equities_research'):

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
        self._table = table

        # Get all dates available in master database
        self._dates = np.unique(self.sql_execute(
            """
            select distinct Date_ from {0} order by Date_;
            """.format(table)
        )).flatten()

    def get_all_dates(self):
        return self._dates

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Data Interface

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
            Should have elements: univ_size, where, and filter

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
            ids = self._get_filtered_ids(d2, filter_args, self._table)
        else:
            ids = []

        # Get features, and strings for cte and regular query
        sqlcmd, features = sqlcmd_from_feature_list(
            features, ids, d1, d3, self._table)

        univ = self.sql_execute(sqlcmd)

        univ_df = pd.DataFrame(univ)
        univ_df.columns = ['SecCode', 'Date'] + features
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
        sqlcmd, features = sqlcmd_from_feature_list(
            features, ids, d1, d3, self._table)

        univ = self.sql_execute(sqlcmd)

        univ_df = pd.DataFrame(univ)
        univ_df.columns = ['ID', 'Date'] + features
        return univ_df

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
        sqlcmd, features = sqlcmd_from_feature_list(
            features, ids.SecCode.tolist(), d1, d3, 'ram.dbo.ram_master_etf')
        univ = self.sql_execute(sqlcmd)

        univ_df = pd.DataFrame(univ)
        univ_df.columns = ['SecCode', 'Date'] + features
        univ_df = univ_df.merge(ids)
        univ_df.ID = univ_df.Ticker
        univ_df = univ_df.drop('Ticker', axis=1)
        return univ_df

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_filtered_ids(self, filter_date, args, table):
        univ_size = args['univ_size']
        filter_col = args['filter'] if 'filter' in args else 'AvgDolVol'
        where = 'and {0}'.format(args['where']) if 'where' in args else ''
        # Get exact filter date
        fdate = self._dates[self._dates <= filter_date][-1]

        # Get IDs using next business date(filter_date). First CTE
        # filters top ID for unique Company (IsrCode).
        ids = np.array(self.sql_execute(
            """
            ; with tempdata as (
            select  M.IsrCode, M.SecCode, M.{3},
                    ROW_NUMBER() over (
                        PARTITION BY M.IsrCode
                        ORDER BY M.{3} DESC, M.SecCode) AS rank_val
            from {4} M
            left join ram.dbo.ram_sector S
                on M.SecCode = S.SecCode
                and M.Date_ between S.StartDate and S.EndDate
            where M.Date_ = '{1}'
            and M.NormalTradingFlag = 1
            {2}
            )
            select top {0} SecCode from tempdata
            where rank_val = 1
            order by {3} desc;
            """.format(univ_size, fdate, where, filter_col, table)
        )).flatten()
        return ids

    def _map_ticker_to_id(self, tickers):
        if isinstance(tickers, str):
            tickers = [tickers]
        # Get Ticker, IdcCode mapping
        ids = pd.DataFrame(self.sql_execute(
            "select distinct SecCode, Ticker "
            "from ram.dbo.ram_master_etf;"), columns=['SecCode', 'Ticker'])
        return ids[ids.Ticker.isin(tickers)]

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


if __name__ == '__main__':

    # EXAMPLES
    dh = DataHandlerSQL()

    filter_args = {'filter': 'AvgDolVol',
                   'where': 'MarketCap >= 200 and GSECTOR not in (50, 55)',
                   'univ_size': 1500}

    univ = dh.get_filtered_univ_data(
        features=['AdjClose', 'GSECTOR'],
        start_date='2016-06-01',
        end_date='2016-06-20',
        filter_date='2016-06-01',
        filter_args=filter_args)

    univ = dh.get_etf_data(
        tickers=['SPY', 'VXX'],
        features=['Close', 'RClose', 'AvgDolVol', 'LAG1_AvgDolVol'],
        start_date='2016-06-01',
        end_date='2016-10-20')

    univ = dh.get_id_data(
        ids=[43030],
        features=['Close', 'RClose', 'AvgDolVol', 'LAG1_AvgDolVol'],
        start_date='2016-06-01',
        end_date='2016-10-20')

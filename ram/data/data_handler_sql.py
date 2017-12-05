import pypyodbc
import numpy as np
import pandas as pd
import datetime as dt
from dateutil import parser as dparser

from ram.utils.time_funcs import check_input_date
from ram.data.sql_features import sqlcmd_from_feature_list

pypyodbc.connection_timeout = 8


def connection_error_handling(f):
    """Decorator"""
    def new_f(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except Exception as e:
            getattr(self, '_disconnect')()
            print('Decorator Exception')
            raise Exception(e)
        except KeyboardInterrupt:
            getattr(self, '_disconnect')()
            print('Decorator Keyboard Interrupt')
            raise KeyboardInterrupt
    new_f.__name__ = f.__name__
    new_f.__doc__ = f.__doc__
    return new_f


class DataHandlerSQL(object):

    def __init__(self, table='ram.dbo.ram_equity_pricing_research'):
        self._table = table
        self._connect()

    def _connect(self):
        self._test_time_constraint()
        try:
            try:
                connection = pypyodbc.connect('Driver={SQL Server};'
                                              'Server=QADIRECT;'
                                              'Database=ram;'
                                              'uid=ramuser;pwd=183madison')
            except:
                # Mac/Linux implementation. unixODBC and FreeTDS works
                # https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Mac-OSX
                connect_str = "DSN=qadirectdb;UID=ramuser;PWD=183madison"
                connection = pypyodbc.connect(connect_str)
            assert connection.connected == 1
            self._connection = connection
            self._cursor = connection.cursor()
            self._cursor.autocommit = True
        except:
            self._connection = None
            self._cursor = None

    def _disconnect(self):
        try:
            self._cursor.close()
            self._connection.close()
        except:
            print('No connection closed')
            pass
        self._cursor = None
        self._connection = None

    def _test_time_constraint(self):
        if (dt.datetime.now().time() >= dt.time(5, 0)) & \
                (dt.datetime.now().time() < dt.time(6, 30)):
            raise Exception('No Queries between 5:00 AM and 6:30 AM')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Data Interface
    @connection_error_handling
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
            seccodes = self._get_filtered_seccodes(d2, filter_args,
                                                   self._table)
        else:
            seccodes = []
        return self.get_seccode_data(seccodes, features, d1, d3)

    @connection_error_handling
    def get_seccode_data(self,
                         seccodes,
                         features,
                         start_date,
                         end_date):
        """
        Parameters
        ----------
        seccodes : list
        features : list
        start_date/end_date : datetime

        Returns
        -------
        data : pandas.DataFrame
            With columns ID, Date representing a unique observation.
        """
        # Check user input
        start_date, _, end_date = _format_dates(start_date, None, end_date)

        output = pd.DataFrame(columns=['SecCode', 'Date'])
        # With large numbers of SecCodes and Features, there is not enough
        # memory to perform a query. Break up by features
        batches = range(0, 301, 10)
        for i1, i2 in zip(batches[:-1], batches[1:]):
            batch_features = features[i1:i2]
            if len(batch_features) == 0:
                break
            # Get features, and strings for cte and regular query
            sqlcmd, batch_features = sqlcmd_from_feature_list(
                batch_features, seccodes, start_date, end_date, self._table)
            univ = self.sql_execute(sqlcmd)
            univ_df = pd.DataFrame(
                univ, columns=['SecCode', 'Date'] + batch_features)
            _check_for_duplicates(univ_df, ['SecCode', 'Date'])
            output = output.merge(univ_df, on=['SecCode', 'Date'],
                                  how='outer')
            _check_for_duplicates(output, ['SecCode', 'Date'])
        return output

    @connection_error_handling
    def get_index_data(self,
                       seccodes,
                       features,
                       start_date,
                       end_date):
        """
        SP500
        -----
        Index (SPX): 50311
        Growth (SGX): 61258
        Value (SVX): 61259

        RUSSELL 1000
        ------------
        Index (RUI): 11097
        Growth (RLG): 11099
        Value (RLV): 11100

        RUSSELL 2000
        ------------
        Index (RUT): 10955
        Growth (RUO): 11101
        Value (RUJ): 11102

        RUSSELL 3000
        ------------
        Index (RUA): 11096
        Growth (RAG): 11103
        Value (RAV): 11104

        Volatility
        ----------
        CBOE SP500 Volatility, 30 day (VIX): 11113
        CBOE SP500 Short Term Volatility, 9 day (VXST): 11132814
        CBOE SP500 3 Month Volatility, 93 day (XVX): 10922530

        Parameters
        ----------
        seccodes : list
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
            features, seccodes, d1, d3, 'ram.dbo.ram_index_pricing')

        univ = self.sql_execute(sqlcmd)

        univ_df = pd.DataFrame(univ)
        univ_df.columns = ['SecCode', 'Date'] + features
        return univ_df

    @connection_error_handling
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
        tickers : list
        features : list
        start_date/end_date : datetime

        Returns
        -------
        data : pandas.DataFrame
            With columns ID, Date representing a unique observation.
        """
        # Check user input
        d1, _, d3 = _format_dates(start_date, None, end_date)

        seccodes = self._map_ticker_to_seccode(tickers)

        # Get features, and strings for cte and regular query
        sqlcmd, features = sqlcmd_from_feature_list(
            features, seccodes.SecCode.tolist(), d1, d3,
            'ram.dbo.ram_etf_pricing')
        univ = self.sql_execute(sqlcmd)

        univ_df = pd.DataFrame(univ)
        univ_df.columns = ['SecCode', 'Date'] + features
        univ_df = univ_df.merge(seccodes)
        univ_df.ID = univ_df.Ticker
        univ_df = univ_df.drop('Ticker', axis=1)
        return univ_df

    @connection_error_handling
    def get_all_dates(self):
        if not hasattr(self, '_dates'):
            # Get all dates available in master database
            self._dates = np.unique(self.sql_execute(
                """
                select distinct Date_ from {0} order by Date_;
                """.format(self._table)
            )).flatten()
        return self._dates

    @connection_error_handling
    def get_ticker_seccode_map(self):
        query_string = \
            """
            select A.SecCode, Ticker from ram.dbo.ram_master_ids A
            join (select SecCode, max(StartDate) as StartDate
                  from ram.dbo.ram_master_ids group by SecCode) B
            on A.SecCode = B.SecCode
            and A.StartDate = B.StartDate
            where A.Ticker is not null
            and A.Ticker != ''
            """
        mapping = self.sql_execute(query_string)
        mapping = pd.DataFrame(mapping)
        mapping.columns = ['SecCode', 'Ticker']
        return mapping

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _get_filtered_seccodes(self, filter_date, args, table):
        univ_size = args['univ_size']
        filter_col = args['filter'] if 'filter' in args else 'AvgDolVol'
        where = 'and {0}'.format(args['where']) if 'where' in args else ''
        # Get exact filter date
        all_dates = self.get_all_dates()
        filter_date = all_dates[all_dates <= filter_date][-1]

        # Get SecCodess using next business date(filter_date). First CTE
        # filters top ID for unique Company (Issuer).
        seccodes = np.array(self.sql_execute(
            """
            ; with tempdata as (
            select      ID.Issuer, M.SecCode, M.{3},
                        ROW_NUMBER() over (
                            PARTITION BY ID.Issuer
                            ORDER BY M.{3} DESC, M.SecCode) AS rank_val
            from        {4} M
            join        ram.dbo.ram_master_ids ID
                on      M.SecCode = ID.SecCode
                and     M.Date_ between ID.StartDate and ID.EndDate
            left join   ram.dbo.ram_idccode_to_gvkey_map G
                on      M.IdcCode = G.IdcCode
                and     M.Date_ between G.StartDate and G.EndDate
            left join   ram.dbo.ram_compustat_sector S
                on      S.GVKey = G.GVKey
                and     M.Date_ between S.StartDate and S.EndDate
            where       M.Date_ = '{1}'
            and         M.NormalTradingFlag = 1
            and         M.OneYearTradingFlag = 1
            {2}
            )
            select top {0} SecCode from tempdata
            where rank_val = 1
            order by {3} desc;
            """.format(univ_size, filter_date, where, filter_col, table)
        )).flatten()
        return seccodes

    def _map_ticker_to_seccode(self, tickers):
        if isinstance(tickers, str):
            tickers = [tickers]
        # Get Ticker, IdcCode mapping
        seccodes = pd.DataFrame(self.sql_execute(
            "select distinct SecCode, Ticker "
            "from ram.dbo.ram_master_ids_etf;"), columns=['SecCode', 'Ticker'])
        return seccodes[seccodes.Ticker.isin(tickers)]

    @connection_error_handling
    def prior_trading_date(self, t0_dates=dt.date.today()):
        if not isinstance(t0_dates, list):
            t0_dates = [t0_dates]
        if not isinstance(t0_dates[0], dt.date):
            try:
                t0_dates = [dparser.parse(x) for x in t0_dates]
            except:
                return np.nan
        date_order = np.argsort(t0_dates)
        input_order = np.argsort(date_order)

        str_dates = ['{0}/{1}/{2}'.format(x.month, x.day, x.year)
                     for x in t0_dates]
        if len(str_dates) > 1:
            sql_dates = tuple(str_dates)
        else:
            sql_dates = "('" + str_dates[0] + "')"

        sql_cmd = ("select Tm1 "
                   "from ram.dbo.ram_trading_dates "
                   "where CalendarDate in {}".format(sql_dates))
        prior_date = self.sql_execute(sql_cmd)
        prior_date = [x[0].date() for x in prior_date]

        if len(t0_dates) == 1:
            return prior_date[0]
        return np.array(prior_date)[input_order]

    def sql_execute(self, sqlcmd):
        self._test_time_constraint()
        try:
            if self._connection is None:
                self._connect()
            self._cursor.execute(sqlcmd)
            return self._cursor.fetchall()
        except Exception as e:
            self._disconnect()
            raise Exception(e)

    def close_connections(self):
        self._disconnect()


def _format_dates(start_date, filter_date, end_date):
        return check_input_date(start_date), \
            check_input_date(filter_date), \
            check_input_date(end_date)


def _check_for_duplicates(dataFrame, columns=['SecCode', 'Date']):
    if type(columns) is str:
        columns = [columns]
    # Check if duplicates exist, if so raise Error
    if dataFrame.duplicated(subset=columns).sum() > 0:
        duplicates = dataFrame.loc[dataFrame.duplicated(subset=columns),
                                   columns]
        raise ValueError('Duplicate rows in data frame:\n {}'.format(
            duplicates.to_string()))
    return


if __name__ == '__main__':

    # EXAMPLES
    dh = DataHandlerSQL()

    filter_args = {'filter': 'AvgDolVol',
                   'where': 'MarketCap >= 200 and GSECTOR not in (50, 55)',
                   'univ_size': 10}

    univ = dh.get_filtered_univ_data(
        features=['MFI5_AdjClose', 'MFI10_AdjClose', 'MFI20_AdjClose',
                  'RSI5_AdjClose', 'RSI10_AdjClose', 'RSI20_AdjClose'],
        start_date='2000-01-01',
        end_date='2001-04-01',
        filter_date='2001-01-01',
        filter_args=filter_args)

    data = dh.get_index_data(
        seccodes=[50311],
        features=['PRMA10_AdjClose'],
        start_date='2000-01-01',
        end_date='2001-04-01')

    univ = dh.get_seccode_data(
        seccodes=[4760, 78331, 58973],
        features=['GSECTOR', 'AdjClose', 'AvgDolVol', 'MarketCap'],
        start_date='1996-04-17',
        end_date='1997-03-31')

    univ = dh.get_etf_data(
        tickers=['SPY', 'VXX'],
        features=['Close', 'RClose', 'AvgDolVol', 'LAG1_AvgDolVol'],
        start_date='2016-06-01',
        end_date='2016-10-20')

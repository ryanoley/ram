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
            from ram.dbo.ram_master
            order by Date_;
            """
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
        # Check user input
        d1, d2, d3 = _format_dates(start_date, filter_date, end_date)

        # Get features, and strings for cte and regular query
        features, cte_str, cte2_str = sqlcmd_from_feature_list(features)

        # Start date to pull for CTE so that all features are calculated
        sdate = d1 - dt.timedelta(days=365)

        if filter_date:
            # Hard-coded for now because there is only one filter needed.
            FILTER_COL = 'AvgDolVol'

            # Get exact filter date
            fdate = self._dates[self._dates <= d2][-1]

            # Get IDs using next business date(filter_date)
            ids = np.array(self.sql_execute(
                """
                select top {0} IdcCode
                from ram.dbo.ram_master
                where Date_ = '{1}'
                and NormalTradingFlag = 1
                order by {2} desc;
                """.format(univ_size, fdate, FILTER_COL)
            )).flatten()
            _, idsstr = _format_ids(ids)

            query = \
                """
                ; with cte1 as (
                    select
                        IdcCode,
                        Date_,
                        {0}
                    from ram.dbo.ram_master
                    where Date_ between '{1}' and '{2}'
                    and IdcCode in {3}
                )

                , cte2 as (
                    select
                        IdcCode,
                        Date_,
                        {4}
                    from cte1
                )

                select
                    *
                from cte2
                where Date_ between '{5}' and '{6}'
                """.format(cte_str, sdate, d3, idsstr, cte2_str, d1, d3)
            univ = self.sql_execute(query)

        else:
            # No IdcCode condition in cte1
            query = \
                """
                ; with cte1 as (
                    select
                        IdcCode,
                        Date_,
                        {0}
                    from ram.dbo.ram_master
                    where Date_ between '{1}' and '{2}'
                )

                , cte2 as (
                    select
                        IdcCode,
                        Date_,
                        {3}
                    from cte1
                )

                select
                    *
                from cte2
                where Date_ between '{4}' and '{5}'
                """.format(cte_str, sdate, d3, cte2_str, d1, d3)

            univ = self.sql_execute(query)

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
        features, cte_str, bod_str = sqlcmd_from_feature_list(features)

        # Format ids, and check if ETFs
        ids, _ = _format_ids(ids)

        if ('SPY' in ids) | ('VXX' in ids):
            table = 'ram.dbo.ram_master_etf'
            if 'SPY' in ids:
                ids.remove('SPY')
                ids.append('59751')
            if 'VXX' in ids:
                ids.remove('VXX')
                ids.append('140062')
        else:
            table = 'ram.dbo.ram_master'
        # Reformat ids in case ETFs were swapped
        _, idsstr = _format_ids(ids)

        # Start date for first CTE is before requested to create room
        # for technical variables.
        sdate = d1 - dt.timedelta(days=365)

        # Query
        query = \
        """
            ; with cte1 as (
                select
                    IdcCode,
                    Date_,
                    {0}
                from {1}
                where Date_ between '{2}' and '{3}'
                and IdcCode in {4}
            )

            , cte2 as (
                select
                    IdcCode,
                    Date_,
                    {5}
                from cte1
            )

            select
                *
            from cte2
            where Date_ between '{6}' and '{7}'
            """.format(cte_str, table, sdate, d3, idsstr,
                       bod_str, d1, d3)
        univ = self.sql_execute(query)

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
    if not hasattr(ids, '__iter__'):
        ids = [ids]
    idsstr = str([str(i) for i in ids])
    idsstr = idsstr.replace('[', '(').replace(']', ')')
    return ids, idsstr


if __name__ == '__main__':

    # EXAMPLES
    dh = DataHandlerSQL()

    univ = dh.get_filtered_univ_data(
        univ_size=10,
        features=['Close', '#$@%', 'ADJClose_', 'VOL20', 'PRMA10', 'PRMA20'],
        start_date='2016-06-01',
        end_date='2016-10-20',
        filter_date='2016-06-01')

    univ = dh.get_filtered_univ_data(
        features=['High', 'Low', 'Close'],
        start_date='2016-10-01',
        end_date='2016-10-10')

    univ = dh.get_id_data(ids=[43030, 50183],
                          features=['High', 'Low', 'Close'],
                          start_date='2016-10-01',
                          end_date='2016-10-20')

    univ = dh.get_id_data(ids='VXX',
                          features=['Close', 'ADJClose', 'VOL30'],
                          start_date='2016-10-01',
                          end_date='2016-10-20')

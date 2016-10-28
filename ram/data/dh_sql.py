import numpy as np
import pandas as pd
import pypyodbc

from ram.utils.time_funcs import check_input_date
from ram.data.base import DataHandler


class DataHandlerSQL(DataHandler):

    def __init__(self):
        connection = pypyodbc.connect('Driver={SQL Server};Server=QADIRECT;'
                                      'Database=ram;uid=ramuser;pwd=183madison')
        assert connection.connected == 1
        self.cursor = connection.cursor()
        # This could be ram_master instead
        sql_dts = ("select distinct Date_ " +
                   " from ram.dbo.ram_master" +
                   " order by Date_")
        self._dates = np.array(self.sql_execute(sql_dts)).flatten()
        self._connection = connection
        # Ordered columns for table ram_master
        self._db_cols = ['ID', 'Date', 'Open', 'High', 'Low', 'Close', 'Vwap',
                         'Volume', 'AvgDolVol', 'MarketCap', 'CashDividend',
                         'DividendFactor', 'SplitFactor', 'NormalTradingFlag']

    def get_filtered_univ_data(self,
                               features,
                               start_date,
                               end_date,
                               univ_size=None,
                               filter_column='AvgDolVol',
                               filter_date=None):
        """
        Purpose of this class is to provide an interface to get a filtered
        universe.

        Parameters
        ----------
        features : list
        start_date/filter_date/end_date : datetime
        univ_size : int
        filter_column : str

        Returns
        -------
        data : pandas.DataFrame
            With columns ID, Date representing a unique observation.
        """
        # Check user input
        if not isinstance(features, list):
            features = [features]

        start_date = check_input_date(start_date)
        filter_date = check_input_date(filter_date)
        end_date = check_input_date(end_date)

        if filter_date:
            # Get next business date from filter date
            bdate_sql = ("select T0" +
                         " from ram.dbo.trading_dates " +
                         " where CalendarDate = '" + str(filter_date) + "'")
            bdate = self.sql_execute(bdate_sql)[0][0]

            # Get IDs using next business date(filter_date)
            id_sql = ("select top " + str(univ_size) +" IdcCode"
                      " from ram.dbo.ram_master" +
                      " where Date_ = '" + str(bdate) +
                      "' order by " + filter_column + " desc")
            ids = np.array(self.sql_execute(id_sql)).flatten()

            # Get data using start_date, end_date, and ids from filter
            univ_sql = ("select * from ram.dbo.ram_master " +
                        " where Date_ between '" + str(start_date) +"' and '"
                        + str(end_date) + "' and IdcCode in " +
                        str(tuple(ids.astype(str))))
            univ = self.sql_execute(univ_sql)
        else:
            # Get data using start_date, end_date, 
            univ_sql = ("select * from ram.dbo.ram_master" +
                        " where Date_ between '" + str(start_date) +"' and '"
                        + str(end_date)) + "'"
            univ = self.sql_execute(univ_sql)            

        univ_df = pd.DataFrame(univ, columns=self._db_cols)
        # Filter columns
        col_inds = ['Date', 'ID'] + features

        return univ_df.loc[:, col_inds]

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
            # -9999 included for single security queries
            ids = [ids, -9999]
        if not isinstance(features, list):
            features = [features]
        start_date = check_input_date(start_date)
        end_date = check_input_date(end_date)
        
        ids = np.array(ids).astype(str)
        # Get data using start_date, end_date, and ids from filter
        univ_sql = ("select * from ram.dbo.ram_master" +
                    " where Date_ between '" + str(start_date) + "' and '" +
                    str(end_date) + "' and IdcCode in " + str(tuple(ids)))
        univ = self.sql_execute(univ_sql)

        univ_df = pd.DataFrame(univ, columns=self._db_cols)
        # Filter columns
        col_inds = ['Date', 'ID'] + features

        return univ_df.loc[:, col_inds]

    def get_all_dates(self):
        return self._dates

    def sql_execute(self, sqlcmd):
        try:
            self.cursor.execute(sqlcmd)
            return self.cursor.fetchall()
        except Exception, e:
            print 'error running sqlcmd: ' + str(e)
            return []

    def close(self):
        self._connection.close()


def main():
    dh = DataHandlerSQL()

    univ =  dh.get_filtered_univ_data(univ_size=100,
                               features=['High','Low','Close'],
                               start_date='2016-10-01',
                               end_date='2016-10-20',
                               filter_date='2016-10-01',)

    univ =  dh.get_filtered_univ_data(features=['High','Low','Close'],
                               start_date='2016-10-01',
                               end_date='2016-10-10')

    univ = dh.get_id_data(ids = [43030, 50183],
                          features=['High','Low','Close'],
                          start_date='2016-10-01',
                          end_date='2016-10-20')

    dh.close()


if __name__ == '__main__':
    main()

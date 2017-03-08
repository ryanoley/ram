import os
import pypyodbc
import numpy as np
import pandas as pd
import datetime as dt

from gearbox import convert_date_array

DDIR = os.path.join(os.getenv('DATA'), 'ram', 'data', 'gvkey_mapping')


def create_sql_table():
    # Master filtered IDs file
    df_filtered = pd.read_csv(os.path.join(DDIR, 'filtered_ids_master.csv'))
    df_filtered.StartDate = convert_date_array(df_filtered.StartDate)
    df_filtered.EndDate = convert_date_array(df_filtered.EndDate)
    # Good data
    df_good = pd.read_csv(os.path.join(DDIR, 'good_idcgvkeydata.csv'),
                          header=0, skiprows=[1]).drop_duplicates()
    df_good.columns = ['IdcCode', 'GVKey']
    df_good['StartDate'] = df_filtered.StartDate.min()
    df_good['EndDate'] = df_filtered.EndDate.max()
    # Import to database
    tcc = TableCreationClass()
    tcc.make_table()
    tcc.insert_dataframe(df_good.append(df_filtered))
    tcc.close_connections()


###############################################################################
# Create Tables in Database

class TableCreationClass(object):

    def __init__(self):
        try:
            self.connection = pypyodbc.connect('Driver={SQL Server};'
                                               'Server=QADIRECT;'
                                               'Database=ram;'
                                               'uid=ramuser;pwd=183madison')
        except:
            # Mac/Linux implementation. unixODBC and FreeTDS works
            connect_str = "DSN=qadirectdb;UID=ramuser;PWD=183madison"
            self.connection = pypyodbc.connect(connect_str)
        self.cursor = self.connection.cursor()

    def make_table(self):
        sql = """\
            if object_id('ram.dbo.ram_idccode_to_gvkey_map', 'U') is not null 
                drop table ram.dbo.ram_idccode_to_gvkey_map
            
            CREATE TABLE ram.dbo.ram_idccode_to_gvkey_map (
            IdcCode INT,
            GVKey INT,
            StartDate SMALLDATETIME,
            EndDate SMALLDATETIME
            PRIMARY KEY (IdcCode, GVKey, StartDate)
            )
            """
        self.cursor.execute(sql)
        self.connection.commit()

    def close_connections(self):
        self.cursor.close()
        self.connection.close()

    def insert_dataframe(self, df):
        # Can only insert 1000 values at a time
        sql = "INSERT INTO ram.dbo.ram_idccode_to_gvkey_map VALUES "
        formatted_rows = self._get_formatted_rows(df)
        i = 0
        while True:
            vals = formatted_rows[(i*1000):((i+1)*1000)]
            vals = ','.join(vals)
            if len(vals):
                final_sql = sql + vals + ';'
                self.cursor.execute(final_sql)
                self.connection.commit()
                i += 1
            else:
                break

    @staticmethod
    def _get_formatted_rows(df):
        formatted_rows = []
        for i, vals in df.iterrows():
            idc_code = vals.IdcCode
            gvkey = vals.GVKey
            start_date = vals.StartDate
            end_date = vals.EndDate
            formatted_rows.append(
                "({}, {}, '{}', '{}')".format(idc_code, gvkey,
                                              start_date, end_date))
        return formatted_rows


if __name__ == '__main__':
    create_sql_table()

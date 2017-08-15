'''
Manages the download of historical and daily data sets from thomson rueters
ftp sites for Starmine data sets.  Writes these data sets into tables
in the ram database.
'''

import os
import re
import urllib
import urllib2
import pypyodbc
import numpy as np
import pandas as pd
from zipfile import ZipFile
import dateutil.parser as dparser


STARMINE_DIR = os.path.join(os.getenv('DATA'), 'Starmine')
STARMINE_SQL = os.path.join(os.getenv('GITHUB'), 'ram', 'data',
                            'ram_sql_tables', 'starmine')


class StarmineDataManager(object):
    FTP_HISTORY = 'ftp://roundabout:nbk8kcrxxbxf@researchftp.starmine.com'
    FTP_LIVE = ("ftp://roundaboutamdd:tuph33mxdsxn@downloads.starmine.com"
                "/daily_download")

    ftp_hist_subdirs = {
        'SmartEstimate': ['SmartEstimate-EPS-Daily', 'SmartEstimate-EBT-Daily',
                          'SmartEstimate-REV-Daily'],
        'ShortInterest': ['ShortInterest-Daily'],
        'ARM':  ['ARM-Daily']
    }

    def __init__(self, src_dir=STARMINE_DIR):
        self._src_dir = src_dir

    def db_connect(self):
        self._connection = pypyodbc.connect(
            'Driver={SQL Server};Server=QADIRECT;'
            'Database=ram;uid=ramuser;pwd=183madison')
        self.cursor = self._connection.cursor()
        self.cursor.autocommit = True
        return

    def db_disconnect(self):
        self._connection.close()
        self.cursor.close()
        self._connection = None
        self.cursor = None
        return

    def download_ftp_hist(self, data_type, ftp_base=FTP_HISTORY):
        '''
        Downloads a directory from the ftp site provided.  Deletes any existing
        files and writes new files to a directory of the same name locally.
        Downloaded file paths are printed out.
        '''

        for sub_dir in self.ftp_hist_subdirs[data_type]:
            local_path = os.path.join(self._src_dir, 'history', sub_dir)

            if os.path.isdir(local_path):
                current_files = os.listdir(local_path)
                old_files = [x for x in current_files if x.find('.zip') > 0]
                for fl in old_files:
                    os.remove(os.path.join(local_path, fl))
            else:
                os.mkdir(local_path)

            ftp_path = os.path.join(ftp_base, sub_dir).replace('\\', '/')
            ftp_files = urllib2.urlopen(ftp_path).read().splitlines()
            new_files = [x.split()[-1] for x in ftp_files if
                         x.split()[-1].find('.zip') > 0]

            for fl in new_files:
                fl_path = os.path.join(ftp_path, fl).replace('\\', '/')
                out_path = os.path.join(local_path, fl)
                urllib.urlretrieve(fl_path, out_path)
                print fl
        return

    def download_ftp_daily(self, ftp_path=FTP_LIVE):
        '''
        Connects to ftp site and downloads new files into local ARM,
        SmartEstimate, or ShortInterest directories. Downloaded file paths
        are printed to console.
        '''
        local_path = os.path.join(self._src_dir, 'daily')

        current_files = os.listdir(os.path.join(local_path, 'ARM'))
        current_files += os.listdir(os.path.join(local_path, 'SmartEstimate'))
        current_files += os.listdir(os.path.join(local_path, 'ShortInterest'))
        current_files = [x for x in current_files if x.find('.zip') > 0]

        ftp_files = urllib2.urlopen(ftp_path).read().splitlines()
        ftp_files = [x.split()[-1] for x in ftp_files if
                     x.split()[-1].find('.zip') > 0]

        new_files = list(set(ftp_files) - set(current_files))
        for fl in new_files:
            fl_path = os.path.join(ftp_path, fl).replace('\\', '/')
            if fl.find('ARM') > 0:
                sub_dir = 'ARM'
            elif fl.find('Estimates') > 0:
                sub_dir = 'SmartEstimate'
            elif fl.find('SI') > 0:
                sub_dir = 'ShortInterest'
            out_path = os.path.join(local_path, sub_dir, fl)
            urllib.urlretrieve(fl_path, out_path)
            print fl_path
        return

    def write_history(self, data_type, table_name, sel_inds=None,
                      na_inds=None):
        '''
        Delete all records from table_name in qadirect.  Iterate through
        zip files in sub_dir, filter using sel_inds and na_inds,
        and load into db. Files added to db are printed to console.
        '''
        # Clear out existing history
        self.db_connect()
        table_sql_path = os.path.join(STARMINE_SQL, table_name + '.sql' )
        with open(table_sql_path, 'r') as inp:
            table_sql_script = inp.read()
        self.cursor.execute(table_sql_script)

        # Create generic insert statement for new records
        fld_txt = '?,' * len(sel_inds)
        SQLCommand = ("INSERT INTO " + table_name +
                      " VALUES (" + fld_txt[:-1] + ")")

        # Get Files to load
        sub_dir = self.ftp_hist_subdirs[data_type]
        local_path = os.path.join(self._src_dir, 'history', sub_dir)
        zip_files = os.listdir(local_path)
        zip_files = [x for x in zip_files if x.find('.zip') > -1]

        for file_name in zip_files:
            file_path = os.path.join(local_path, file_name)
            df = read_starmine_zip(file_path, sel_inds, na_inds, sep='\t')
            data = [tuple(x) for x in df.values]
            self.cursor.executemany(SQLCommand, data)
            self.cursor.commit()
            print file_name

        self.db_disconnect()
        return

    def write_smart_estimate_history(self, table_name, sel_inds=None,
                                     na_inds=None):
        '''
        Loading SmartEstimate history specifically.  Deletes all records in
        table_name and loads in all files from historical dir.
        '''
        # Clear out existing history
        self.db_connect()
        table_sql_path = os.path.join(STARMINE_SQL, table_name + '.sql' )
        with open(table_sql_path, 'r') as inp:
            table_sql_script = inp.read()
        self.cursor.execute(table_sql_script)

        # Create generic insert statement for new records
        fld_txt = '?,' * 12
        SQLCommand = ("INSERT INTO " + table_name +
                      " VALUES (" + fld_txt[:-1] + ")")

        # Get Files to load
        local_path = os.path.join(self._src_dir, 'history')
        sub_dirs = self.ftp_hist_subdirs['SmartEstimate']
        fl_names = []
        fl_paths = []
        for sub_dir in sub_dirs:
            list_dir = os.listdir(os.path.join(local_path, sub_dir))
            fl_names += list_dir
            fl_paths += [os.path.join(local_path, sub_dir, fl) for fl
                         in list_dir]

        fl_names = np.array([fl for fl in fl_names if fl.find('.zip') > -1])
        fl_paths = np.array([fl for fl in fl_paths if fl.find('.zip') > -1])

        years = [re.search(r'_(\d\d\d\d)_', fl).groups()[0] for fl in fl_names]
        years = np.array(years, dtype=int)

        dtypes = [re.search(r'_(\w*?)_\d', fl).groups()[0] for fl in fl_names]
        dtypes = np.array(dtypes, dtype=str)

        key_columns = ['Data-through Date', 'StarMine Security ID']

        for year in np.unique(years):
            year_paths = fl_paths[np.where(years == year)]
            year_dtypes = dtypes[np.where(years == year)]

            meta_df = pd.DataFrame([])
            out_df = pd.DataFrame([])
            for fl_path, dtype in zip(year_paths, year_dtypes):
                df = read_starmine_zip(fl_path, sel_inds, na_inds, sep='\t')
                df.rename(
                    columns={'FQ1 SmartEstimate': 'SE_{}'.format(dtype),
                             'FQ1 Predicted Surprise Pct':
                             'SE_{}_Surprise'.format(dtype)}, inplace=True)
                data_columns = df.columns[-2:].tolist()
                
                if len(out_df) == 0:
                    out_df = df[key_columns + data_columns].copy()
                else:
                    out_df = out_df.merge(df[key_columns + data_columns],
                                          how='outer')
                meta_df = meta_df.append(df[df.columns[:-2]])

            meta_df.drop_duplicates(subset=key_columns, inplace=True)
            data_columns = out_df.columns[2:].tolist()
            out_df = pd.merge(out_df, meta_df)
            out_df = out_df[meta_df.columns.tolist() + data_columns]
            out_df = out_df.where(pd.notnull(out_df), None)
            data = [tuple(x) for x in out_df.values]
            self.cursor.executemany(SQLCommand, data)
            self.cursor.commit()
            print 'SmartEstimate_{}'.format(year)

        self.db_disconnect()
        return

    def write_daily(self, sub_dir, table_name, sel_inds=None, na_inds=None):
        '''
        Load files in sub_dir into table_name in qad ram database.  Only files
            more recent than max forward date in db are lodated.
        Sel_inds and na_inds are column indices used to select and filter
            from data.  File names of loaded files are printed to console.
        '''
        local_path = os.path.join(self._src_dir, 'daily', sub_dir)

        # Get dates of files in local dir
        daily_files = os.listdir(local_path)
        daily_files = [x for x in daily_files if x.find('.zip') > 0]
        daily_dates = [y.split('_')[-1].replace('.zip', '') for y
                       in daily_files]
        daily_dates = [dparser.parse(y).date() for y in daily_dates]

        # Get latest update date from DB
        self.db_connect()
        self.cursor.execute("select max(AsOfDate) from " + table_name)
        db_date = dparser.parse(self.cursor.fetchall()[0][0]).date()

        # Identify files after most recent update
        new_files = [x > db_date for x in daily_dates]
        new_files = [i for (i, v) in zip(daily_files, new_files) if v]

        # Generic SQL command for insert
        fld_txt = '?,' * len(sel_inds)
        SQLCommand = ("INSERT INTO " + table_name +
                      " VALUES (" + fld_txt[:-1] + ")")

        for file_name in new_files:
            fl_path = os.path.join(local_path, file_name)
            df = read_starmine_zip(fl_path, sel_inds, na_inds)

            if table_name == 'ram_starmine_smart_estimate':
                df.iloc[:, 5] = 1.0
            data = [tuple(x) for x in df.values]
            self.cursor.executemany(SQLCommand, data)
            self.cursor.commit()
            print file_name

        self.db_disconnect()
        return


def read_starmine_zip(zip_path, sel_inds=None, na_inds=None, sep=','):
    zFile = ZipFile(zip_path, 'r')
    names = zFile.namelist()
    df = pd.read_csv(zFile.open(names[0]), sep=sep, low_memory=False)
    df = df[np.all(df.iloc[:, na_inds].notnull(), axis=1)]

    fill_inds = []
    sel_inds = list(sel_inds)
    while np.nan in sel_inds:
        fill_inds.append(sel_inds.index(np.nan) + len(fill_inds))
        sel_inds.remove(np.nan)
    df = df.iloc[:, sel_inds]
    df = df.where(pd.notnull(df), None)

    for ix in fill_inds:
        df.insert(ix, str(ix), None)

    return df.reset_index(drop=True)


def main():
    import argparse

    ###########################################################################
    #   Command line arguments
    ###########################################################################

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-data_type', '--data_type', default='SmartEstimate',
        help='Must be either SmartEstimate or ShortInterest')
    parser.add_argument(
        '-hist_ftp', '--hist_ftp', action='store_true',
        help='Download new historical files from ftp')
    parser.add_argument(
        '-hist_load', '--hist_load', action='store_true',
        help='Load historical data')
    args = parser.parse_args()

    ###########################################################################

    assert args.data_type in ['SmartEstimate', 'ShortInterest', 'ARM']
    sdm = StarmineDataManager()

    if args.hist_ftp:
        sdm.download_ftp_hist(args.data_type)

    if args.hist_load:
        # PROMPT FOR LOAD
        if args.data_type == 'SmartEstimate':
            table_name = 'ram_starmine_smart_estimate'
            sel_inds = [0, 1, 2, 3, 5, 9, 22, 23]
            na_inds = [0, 1, 22]
            sdm.write_smart_estimate_history(table_name, sel_inds, na_inds)
        else:
            if args.data_type == 'ShortInterest':
                table_name = 'ram_starmine_short_interest'
                sel_inds = [0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11]
                na_inds = [0, 1]
            elif args.data_type == 'ARM':
                table_name = 'ram_starmine_arm'
                sel_inds = [0, 1, 2, 3, 5, 10, 12, 13, 14, 17]
                na_inds = [0, 1, 14]
            sdm.write_history(args.data_type, table_name, sel_inds, na_inds)
    else:
        # Download new files from ftp directory
        sdm.download_ftp_daily()

        # Write new SmartEstimate Data
        daily_dir = 'SmartEstimate'
        table_name = 'ram_starmine_smart_estimate'
        sel_inds = [0, 1, np.nan, np.nan, 2, np.nan, 9, 20, 31, 42, 53, 64]
        na_inds = [0, 1, 9]
        sdm.write_daily(daily_dir, table_name, sel_inds, na_inds)

        # Write new Short Interest Data
        daily_dir = 'ShortInterest'
        table_name = 'ram_starmine_short_interest'
        sel_inds = [0, 1, np.nan, np.nan, 3, 9, 10, 11, 12, 13, 16]
        na_inds = [0, 1]
        sdm.write_daily(daily_dir, table_name, sel_inds, na_inds)

        # Write new ARM Data
        daily_dir = 'ARM'
        table_name = 'ram_starmine_arm'
        sel_inds = [0, 1, np.nan, np.nan, 2, 10, 12, 13, 16, 14]
        na_inds = [0, 1, 16]
        sdm.write_daily(daily_dir, table_name, sel_inds, na_inds)


if __name__ == '__main__':
    main()

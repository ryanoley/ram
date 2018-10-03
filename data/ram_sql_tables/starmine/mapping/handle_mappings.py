import os
import pandas as pd
from gearbox import read_sql

from ram.data.data_handler_sql import DataHandlerSQL


#################
#  IMPORT DATA  #
#################

# Get handled list
fpath = os.path.join(os.getenv('GITHUB'), 'ram', 'data', 'ram_sql_tables',
                     'starmine', 'mapping', 'handled_mappings.csv')

handled = pd.read_csv(fpath)
handled = handled[['SecCode', 'SecId', 'StartDate', 'EndDate']]

# Get todays problem list
dpath = os.path.join(os.getenv('DATA'), 'ram', 'data', 'starmine')

# First, delete problem file
try:
    os.remove(os.path.join(dpath, 'PROBLEMS.csv'))
except:
    pass

problems = read_sql(os.path.join(dpath, max(os.listdir(dpath))))


########################
#  WRITE PROBLEM FILE  #
########################

# Extract SecCodes that have been handled
problems = problems[~problems.SecCode.isin(handled.SecCode)]

if len(problems) > 0:
    problems.to_csv(os.path.join(dpath, 'PROBLEMS.csv'), index=None)


##################################
#  UPDATE DATABASE WITH HANDLED  #
##################################

def insert_mapping_into_db(mapping):
    dh = DataHandlerSQL()
    formatted_rows = _format_rows(mapping)
    if len(formatted_rows):
        vals = ','.join(formatted_rows)
        sql = "insert into ram.dbo.ram_starmine_map2 (SecCode, SecId, StartDate, EndDate) values "
        sql = sql + vals + ';'
        dh.sql_execute_no_return(sql, time_constrained=False)
    return


def _format_rows(mapping):
    output = []
    for _, x in mapping.iterrows():
        output.append("({}, {}, '{}', '{}')".format(
            x['SecCode'], x['SecId'], x['StartDate'], x['EndDate']))
    return output


insert_mapping_into_db(handled)

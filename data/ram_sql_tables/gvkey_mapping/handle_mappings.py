import os
import pandas as pd
import datetime as dt

from gearbox import read_sql, convert_date_array

from ram.data.data_handler_sql import DataHandlerSQL


def get_current_mapping():
    dh = DataHandlerSQL()
    command = \
        """
        select IdcCode, GVKey, StartDate, EndDate
        from ram.dbo.ram_idccode_to_gvkey_map_NEW;
        """
    result = dh.sql_execute(command)
    data = pd.DataFrame(result,
                        columns=['IdcCode', 'GVKey', 'StartDate', 'EndDate'])
    return data


def import_problem_mappings():
    file_name = '{}_problem_mappings.txt'.format(
        dt.date.today().strftime('%Y%m%d'))
    df = read_sql(os.path.join(os.getenv('DATA'), 'ram', 'data',
                               'gvkey_mapping2', file_name))
    # Fix problem columns
    df.Cusip = df.Cusip.apply(lambda x: x.strip())
    df.StartDate = convert_date_array(df.StartDate)
    df.EndDate = convert_date_array(df.EndDate)
    return df


def import_manually_handled_mappings():
    df = pd.read_csv(os.path.join(os.getenv('GITHUB'), 'ram', 'data',
                                  'ram_sql_tables', 'gvkey_mapping',
                                  'handled_mapping.csv'))
    return df



def filter_problems(problems, handled):
    # Merge
    p = problems[['IdcCode', 'GVKey']].drop_duplicates()
    h = handled.copy()
    h['handled'] = 1
    p['problems'] = 1
    ph = h.merge(p, how='outer')

    # If null, then new GVKey added to IdcCodes
    pcodes1 = ph.IdcCode[ph.handled.isnull()].unique().tolist()

    # If null, then potentially something resolved itself
    pcodes2 = ph.IdcCode[ph.problems.isnull()].unique().tolist()

    problem_idc_codes = pcodes1 + pcodes2

    return problems[problems.IdcCode.isin(
        problem_idc_codes)].reset_index(drop=True)



# Get current mapping
mapping = get_current_mapping()

problems = import_problem_mappings()

handled = import_manually_handled_mappings()

# See if problems have been handled, if not, write to file
problems = filter_problems(problems, handled)








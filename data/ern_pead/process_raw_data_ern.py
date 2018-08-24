import os

from ram.utils.read_write import import_sql_output


def _assert_no_duplicates(data):
    # Confirm no duplicate SecCodes/ReportDates
    # Write test for this?
    assert data[['SecCode', 'ReportDate']].drop_duplicates().shape[0] == data.shape[0]


ddir = os.path.join(os.getenv('DATA'), 'ram', 'data', 'temp_ern_pead', 'earnings')

report_dates_file_name = 'report_dates_returns.txt'

feature_file_names = [
    'technical_vars_1.txt',
    'technical_vars_2.txt',
    'technical_vars_3.txt',
    'technical_vars_4.txt',
    'accounting.txt',
    'dividend_yield.txt',
    'starmine_arm.txt',
]

data = import_sql_output(os.path.join(ddir, report_dates_file_name))


for f in feature_file_names[:1]:
    tmp = import_sql_output(os.path.join(ddir, f))
    print(tmp.shape)
    _assert_no_duplicates(tmp)
    data = data.merge(tmp, how='left')
    print("Loaded: %s" % f)

_assert_no_duplicates(data)

# Handle Non-dividend observations
data.DividendYield = data.DividendYield.fillna(0)
data.DividendYield_Rank = data.DividendYield_Rank.fillna(0)



##### COMPARE DATES/DATA #####

from gearbox import read_csv, convert_date_array
import datetime as dt


dataOrig = read_csv(os.path.join(os.getenv('DATA'), 'earnings', 'prod', 'research_data.csv'))

dataOrig['SecCode'] = dataOrig.ID
dataOrig['ReportDate'] = convert_date_array(dataOrig.EarningsDate)

t1 = data[['SecCode', 'ReportDate', 'PRMA10']].copy()
t2 = dataOrig[['SecCode', 'ReportDate', 'PRMA10']].copy()


t1 = t1[t1.ReportDate >= dt.date(2000, 1, 1)]
t2 = t2[t2.ReportDate >= dt.date(2000, 1, 1)]

t1 = t1[t1.ReportDate < dt.date(2017, 1, 1)]
t2 = t2[t2.ReportDate < dt.date(2017, 1, 1)]

t1['PRMA10'] = (t1.PRMA10 - 1) * 100


tM = t1.merge(t2, how='outer', on=['SecCode', 'ReportDate'])
tM[tM.SecCode == 11149455]






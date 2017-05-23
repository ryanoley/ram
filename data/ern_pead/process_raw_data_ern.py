import os

from ram.utils.read_write import import_sql_output



def _assert_no_duplicates(data):
    # Confirm no duplicate SecCodes/ReportDates
    # Write test for this?
    assert data[['SecCode', 'ReportDate']].drop_duplicates().shape[0] == data.shape[0]


ddir = os.path.join(os.getenv('DATA'), 'ram', 'data', 'temp_ern_pead', 'earnings')

report_dates_file_name = 'report_dates_returns.csv'

feature_file_names = [
    'dividend_yield.csv',
    'starmine_arm.csv',
    'technical_vars_1.csv',
    'technical_vars_2.csv',
    'technical_vars_3.csv',
    'technical_vars_4.csv'
]

data = import_sql_output(os.path.join(ddir, report_dates_file_name))

for f in feature_file_names:
    tmp = import_sql_output(os.path.join(ddir, f))
    _assert_no_duplicates(tmp)
    data = data.merge(tmp, how='left')
    print("Loaded: %s" % f)

_assert_no_duplicates(data)

# Handle Non-dividend observations
data.DividendYield = data.DividendYield.fillna(0)
data.DividendYield_Rank = data.DividendYield_Rank.fillna(0)



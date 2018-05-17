import os
import pandas as pd
import datetime as dt

from gearbox import read_sql

# Import problem mappings
file_name = '{}_problem_mappings.txt'.format(dt.date.today().strftime('%Y%m%d'))



path = os.path.join(os.getenv('DATA'), 'ram', 'data', 'gvkey_mapping2', file_name)



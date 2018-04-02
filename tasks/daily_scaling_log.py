import os
import json
import pandas as pd
import datetime as dt
from shutil import copyfile
from gearbox import convert_date_array

DATA = os.path.join(os.getenv('DATA'), 'gearbox', 'prod')

def write_params_to_archive(params_path, archive_path):

    port_params = json.load(open(params_path))
    AUM = port_params['aum']
    ERN_PROP = port_params['earnings_prop']
    PEAD_PROP = port_params['pead_prop']
    MAX_EXP = port_params['max_exp']

    if os.path.isfile(archive_path):
        archive_df = pd.read_csv(archive_path)
        archive_df.Date = convert_date_array(archive_df.Date)
    else:
        archive_df = pd.DataFrame(columns=['Date', 'AUM', 'ERN', 'PEAD',
                                            'MaxExp'])

    ix = len(archive_df)
    archive_df.loc[ix] = {'Date':dt.date.today(), 'AUM':AUM, 'ERN':ERN_PROP,
                            'PEAD':PEAD_PROP, 'MaxExp':MAX_EXP}
    archive_df.drop_duplicates(inplace=True)
    archive_df.to_csv(archive_path, index=False)
    return

###############################################################################

def main():
    import ipdb; ipdb.set_trace()
    ###########################################################################
    # Paths to prod params
    params_path = os.path.join(DATA, 'port_params.json')
    archive_path = os.path.join(DATA, 'archive', 'port_params_archive.csv')

    ###########################################################################
    # Read in scaling params and write out
    assert(os.path.isfile(params_path))

    write_params_to_archive(params_path, archive_path)

    dtstr = dt.date.today().strftime('%Y%m%d')
    output_file = 'port_params_{}.json'.format(dtstr)
    output_path = os.path.join(DATA, 'archive', output_file)
    copyfile(params_path, output_path)


if __name__ == '__main__':
    main()

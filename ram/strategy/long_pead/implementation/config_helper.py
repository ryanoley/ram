import os
import pandas as pd
from ram.strategy.long_pead.implementation import config


def print_unique_ids():
    """
    Extracts unique IDs by sector for all
    """
    ddir = os.path.join(os.getenv('DATA'), 'ram', 'implementation',
                        'LongPeadStrategy', 'training')

    files = [x for x in os.listdir(ddir) if x.find('training') > 0]

    sector_ids = {}

    for sector in config.sectors:
        ids = []
        sector_files = [x for x in files if x.find('sector_{}'.format(sector)) > 0]
        for f in sector_files:
            path = os.path.join(ddir, f)
            data = pd.read_csv(path)
            ids.append(data.SecCode.unique().tolist())
        ids = sum(ids, [])

        print('\nSector {} ids\n'.format(sector))
        print(ids)


if __name__ == '__main__':

    print_unique_ids()

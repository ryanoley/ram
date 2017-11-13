import os
import json
import inspect
import pandas as pd

from ram.strategy.long_pead.main import LongPeadStrategy

# Get data and signals components
data = LongPeadStrategy.data
signals = LongPeadStrategy.signals

# Daily pipeline

skl_model = load_skl_model()

data =

signals.generate_signals()



# Import best params


run_name = 'run_'




# Use this to get
inspect.getargspec



"""
from sklearn.ensemble import ExtraTreesClassifier

from gearbox import convert_date_array

from ram.config import PREPPED_DATA_DIR, IMPLEMENTATION_DATA_DIR
from ram.strategy.long_pead.implementation import config
from ram.strategy.long_pead.implementation.modelling import DataContainer
from ram.strategy.long_pead.implementation.modelling import SignalModel
from ram.strategy.long_pead.implementation.modelling import LongPeadStrategy



imp_data_dir = os.path.join(IMPLEMENTATION_DATA_DIR, 'LongPeadStrategy',
                            'training')


# Get all files for given sector
for sector in config.sectors:

    # Get column parameters
    dpath = os.path.join(imp_data_dir,
                         'column_params_sector_{}.json'.format(sector))
    sector_params = json.load(open(dpath, 'r'))

    for version in config.sector_data_versions[sector]:

        # This strategy class is used to import data
        strategy = LongPeadStrategy(prepped_data_version=version)
        strategy._get_prepped_data_file_names()

        # Data Container formats all historical data
        dc = DataContainer()
        dc.add_market_data(strategy.read_market_index_data())
        for i in range(len(strategy._prepped_data_files)):
            n = len(strategy._prepped_data_files) - 5
            if i < n:
                continue
            # Import, process, and stack data
            dc.add_data(strategy.read_data_from_index(i))
            print i

        # Fit and cache models
        for p in config.sector_params[sector]:
            params = sector_params[str(p)]
            dc.prep_data(params['response_params'],
                         params['training_qtrs'])

            model_name = 'model_sector_{}'.format(sector)

            model_cache_path = os.path.join(imp_data_dir,
                                            )

            signals = SignalModel()
            signals.make_cache_model(
                model_cache_path,
                train_data=dc.train_data,
                features=dc.features,
                model_params=params['model_params'],
                drop_accounting=params['drop_accounting'],
                drop_extremes=params['drop_extremes'],
                drop_starmine=params['drop_starmine'],
                drop_market_variables=params['drop_market_variables']
                )

"""
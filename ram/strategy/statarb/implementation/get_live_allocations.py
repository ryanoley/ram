import os
import json
import pickle
import pandas as pd
import datetime as dt

from ram import config
from ram.strategy.statarb import statarb_config
from ram.strategy.statarb.main import StatArbStrategy

from gearbox import convert_date_array


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def import_raw_data(implementation_dir=config.IMPLEMENTATION_DATA_DIR):
    """
    Loads implementation raw data and processes it
    """
    statarb_path = os.path.join(implementation_dir, 'StatArbStrategy',
                                'daily_raw_data')
    all_files = _get_all_raw_data_file_names(statarb_path)
    todays_files = _get_max_date_files(all_files)
    output = {}
    for f in todays_files:
        name = _format_raw_data_name(f)
        output[name] = _import_format_raw_data(os.path.join(statarb_path, f))
    output['market_data'] = _import_format_raw_data(
        os.path.join(statarb_path, 'market_index_data.csv'))
    return output


def _get_all_raw_data_file_names(raw_data_dir_path):
    """
    Filters out market_index_data.csv
    """
    all_files = os.listdir(raw_data_dir_path)
    all_files = [x for x in all_files if x.find('current_blueprint') > 0]
    all_files.sort()
    return all_files


def _get_max_date_files(all_files):
    max_date = max([x.split('_')[0] for x in all_files])
    todays_files = [x for x in all_files if x.find(max_date) > -1]
    todays_files = [x for x in todays_files if x.find('.csv') > -1]
    return todays_files


def _format_raw_data_name(file_name):
    return file_name[file_name.rfind('version'):].replace('.csv', '')


def _import_format_raw_data(path):
    data = pd.read_csv(path)
    data.Date = convert_date_array(data.Date)
    data.SecCode = data.SecCode.astype(int).astype(str)
    return data


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def import_run_map(
        implementation_dir=config.IMPLEMENTATION_DATA_DIR,
        trained_model_dir_name=statarb_config.trained_models_dir_name):

    path = os.path.join(implementation_dir,
                        'StatArbStrategy',
                        'trained_models',
                        trained_model_dir_name,
                        'run_map.csv')
    data = pd.read_csv(path)
    # GCP outputs index column, which for this needs to be removed
    if data.columns[0].find('Unnamed') > -1:
        data = data.iloc[:, 1:]
    return data


def import_models_params(
        implementation_dir=config.IMPLEMENTATION_DATA_DIR,
        trained_model_dir_name=statarb_config.trained_models_dir_name):
    """
    Returns
    -------
    output : dict
        Holds parameter and sklearn model for each trained model
    """
    path = os.path.join(implementation_dir,
                        'StatArbStrategy',
                        'trained_models',
                        trained_model_dir_name)
    model_files, param_files = _get_model_files(path)
    output = {}
    for m, p in zip(model_files, param_files):
        run_name = m.replace('_skl_model.pkl', '')
        output[run_name] = {}
        output[run_name]['params'] = \
            json.load(open(os.path.join(path, p), 'r'))
        output[run_name]['model'] = \
            pickle.load(open(os.path.join(path, m), 'r'))
    return output


def _get_model_files(path):
    """
    Return file names from production trained models directories, and
    makes sure the model name is aligned with the param file name
    """
    all_files = os.listdir(path)
    all_files = [x for x in all_files if x.find('run_map.csv') == -1]
    model_files = [x for x in all_files if x.find('skl_model') > -1]
    model_files.sort()
    param_files = [x for x in all_files if x.find('params') > -1]
    param_files.sort()
    # Assert that they are aligned
    for m, p in zip(model_files, param_files):
        assert m.replace('_skl_model.pkl', '') == p.replace('_params.json', '')
    return model_files, param_files


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# data = import_raw_data()
# run_map = import_run_map()
# models = import_models_params()




# # Perhaps create five different versions of strategy?
# strategy = StatArbStrategy()

# # LOOP over unique data versions
# strategy.strategy_code_version = 'version_001'
# strategy.prepped_data_version = 'version_0013'
# strategy.strategy_init()

# # DATA
# strategy.data.prep_live_data(data['version_0013'], data['market_data'])

# # Fake live data
# live_data = data['version_0013'].copy()
# live_data = live_data[live_data.Date == live_data.Date.max()]
# live_data = live_data[['SecCode', 'Date', 'AdjClose', 'AdjOpen',
#                        'AdjHigh', 'AdjLow', 'AdjVwap', 'AdjVolume']]
# live_data.Date = dt.datetime.utcnow().date()
# strategy.data.process_live_data(live_data)


# model = models['run_0003_100']['model']
# params = models['run_0003_100']['params']



# strategy.signals.set_args(**params['signals'])  # This will eventually not work
# strategy.signals.set_features(strategy.data.get_train_features())
# strategy.signals.set_test_data(strategy.data.get_test_data())
# strategy.signals.set_model(model)

# signals = strategy.signals.get_signals()


# strategy.constructor.set_args(**params['constructor'])
# strategy.constructor.set_constructor_data(strategy.data.get_constructor_data())

# scores = signals[['SecCode', 'preds']].set_index('SecCode').to_dict()['preds']
# sizes = strategy.constructor.get_day_position_sizes(0, scores)


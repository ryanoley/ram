import os
import json
import datetime as dt

from ram import config
from ram.strategy.statarb import statarb_config
from ram.strategy.base import read_json

from ram.data.data_constructor import DataConstructor
from ram.data.data_constructor_blueprint import DataConstructorBlueprint


base_path = os.path.join(config.IMPLEMENTATION_DATA_DIR, 'StatArbStrategy')

raw_data_path = os.path.join(base_path, 'daily_raw_data')

blueprints_path = os.path.join(base_path, 'preprocessed_data',
                               statarb_config.preprocessed_data_dir)

blueprints = os.listdir(blueprints_path)
blueprints = [x for x in blueprints if x.find('blueprint') > -1]

# Iterate
b = read_json(os.path.join(blueprints_path, blueprints[0]))
blueprint = DataConstructorBlueprint(blueprint_json=b)

# Set date parameters
start_date = dt.datetime.utcnow() - dt.timedelta(days=380)
end_date = dt.datetime.utcnow() - dt.timedelta(days=1)
blueprint.seccodes_filter_arguments['start_date'] = start_date.strftime('%Y-%m-%d')
blueprint.seccodes_filter_arguments['end_date'] = end_date.strftime('%Y-%m-%d')

# Update date and time

# Run in the morning
dc = DataConstructor()
dc.run_live(blueprint, 'StatArbStrategy')




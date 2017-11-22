import os
import json
import datetime as dt

from ram import config
from ram.strategy.statarb import statarb_config

from ram.data.data_constructor import DataConstructor
from ram.data.data_constructor_blueprint import DataConstructorBlueprint

from ram.strategy.statarb.statarb_config import implementation_top_models


blueprints_path = os.path.join(
    config.IMPLEMENTATION_DATA_DIR,
    'StatArbStrategy',
    'preprocessed_data',
    statarb_config.preprocessed_data_dir)

blueprints = os.listdir(blueprints_path)


b = json.load(open(os.path.join(blueprints_path, blueprints[0]), 'r'))
blueprint = DataConstructorBlueprint(blueprint_json=b)

# Set date parameters
start_date = dt.datetime.utcnow() - dt.timedelta(days=380)
end_date = dt.datetime.utcnow() - dt.timedelta(days=1)
blueprint.seccodes_filter_arguments['start_date'] = start_date.strftime('%Y-%m-%d')
blueprint.seccodes_filter_arguments['end_date'] = end_date.strftime('%Y-%m-%d')

# Update date and time

# Run in the morning
dc = DataConstructor()
dc.run(blueprint)

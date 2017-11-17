from copy import deepcopy

from ram.data.data_constructor_blueprint import DataConstructorBlueprint, \
    DataConstructorBlueprintContainer

from ram.strategy.statarb.version_001.data_blueprints import *

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

blueprint_container = DataConstructorBlueprintContainer()

blueprint_container.add_blueprint(sector20_0)
blueprint_container.add_blueprint(sector20_1)
blueprint_container.add_blueprint(sector20_2)

blueprint_container.add_blueprint(sector25_0)
blueprint_container.add_blueprint(sector25_1)
blueprint_container.add_blueprint(sector25_2)

blueprint_container.add_blueprint(sector45_0)
blueprint_container.add_blueprint(sector45_1)
blueprint_container.add_blueprint(sector45_2)

from ram.data.data_constructor_blueprint import DataConstructorBlueprint, \
    DataConstructorBlueprintContainer

from ram.strategy.statarb.version_002.data_blueprints import sector20_0
from ram.strategy.statarb.version_003.data_blueprints import *

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

blueprint_container = DataConstructorBlueprintContainer()

blueprint_container.add_blueprint(sector20_0)

blueprint_container.add_blueprint(sector20_1)
blueprint_container.add_blueprint(sector25_1)
blueprint_container.add_blueprint(sector45_1)

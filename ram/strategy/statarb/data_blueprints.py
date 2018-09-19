from ram.data.data_constructor_blueprint import DataConstructorBlueprint, \
    DataConstructorBlueprintContainer

from ram.strategy.statarb.version_004.data_blueprints import *

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

blueprint_container = DataConstructorBlueprintContainer()

blueprint_container.add_blueprint(sector20_1)
blueprint_container.add_blueprint(sector25_1)
blueprint_container.add_blueprint(sector45_1)
# Speculative
blueprint_container.add_blueprint(sector10_1)
blueprint_container.add_blueprint(sector15_1)
blueprint_container.add_blueprint(sector30_1)
blueprint_container.add_blueprint(sector35_1)
blueprint_container.add_blueprint(sector40_1)
blueprint_container.add_blueprint(sector50_1)
blueprint_container.add_blueprint(sector55_1)

blueprint_container.add_blueprint(all_sectors)
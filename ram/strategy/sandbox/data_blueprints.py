from copy import deepcopy

from ram.data.data_constructor_blueprint import DataConstructorBlueprint, \
    DataConstructorBlueprintContainer

# BASE
base = DataConstructorBlueprint(constructor_type='universe',
                                description='TEMP',
                                market_data_flag=False,
                                strategy_name='sandbox')


base.universe_filter_arguments['filter'] = 'AvgDolVol'

base.universe_filter_arguments['where'] = 'MarketCap >= 100 and Close_ >= 5' \
                                            + ' and AvgDolVol >= 1'

base.universe_filter_arguments['univ_size'] = 500

base.universe_date_parameters['frequency'] = 'Q'

base.universe_date_parameters['train_period_length'] = 1

base.universe_date_parameters['test_period_length'] = 1

base.universe_date_parameters['start_year'] = 2004


base.features = [
# Descriptive
'TM1', 'T1', 'GGROUP', 'MarketCap', 'AvgDolVol', 'SplitFactor',
'RVwap', 'ROpen','RHigh', 'RLow', 'RClose', 'RCashDividend',

# Pricing
'AdjOpen', 'LEAD1_AdjOpen', 'AdjClose', 'LEAD1_AdjClose', 'AdjVwap',

# Pricing Features
'PRMAH5_AdjClose',  'PRMAH10_AdjClose', 'PRMAH20_AdjClose',
'PRMAH60_AdjClose','PRMAH120_AdjClose', 'PRMAH250_AdjClose',

'PRMA5_AdjClose',  'PRMA10_AdjClose', 'PRMA20_AdjClose',
'PRMA60_AdjClose','PRMA120_AdjClose', 'PRMA250_AdjClose',

'VOL5_AdjClose',  'VOL10_AdjClose', 'VOL20_AdjClose', 'VOL60_AdjClose',
'VOL120_AdjClose', 'VOL250_AdjClose',

'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose', 'DISCOUNT252_AdjClose',

'MFI5_AdjClose', 'MFI10_AdjClose', 'MFI20_AdjClose', 'MFI60_AdjClose',
'MFI120_AdjClose', 'MFI250_AdjClose',

'RSI5_AdjClose', 'RSI10_AdjClose', 'RSI20_AdjClose', 'RSI60_AdjClose',
'RSI120_AdjClose', 'RSI250_AdjClose',

'BOLL5_AdjClose', 'BOLL10_AdjClose', 'BOLL20_AdjClose',
'BOLL60_AdjClose', 'BOLL120_AdjClose', 'BOLL250_AdjClose',

# STARMINE
'ARM', 'EPSSURPRISEFQ1', 'REVENUESURPRISEFQ1', 'SIRANK', 'SISHORTSQUEEZE',
'SIINSTOWNERSHIP', 'PTARGETMEAN', 'RECMEAN', 'RECNREC'
]


# Make container
blueprint_container = DataConstructorBlueprintContainer()

for sector in range(10, 60, 5):
    sector_bp = deepcopy(base)
    sector_bp.description = 'Sector {}'.format(sector)
    sector_bp.universe_filter_arguments['where'] += ' and GSECTOR = {}'.format(sector)
    blueprint_container.add_blueprint(sector_bp)

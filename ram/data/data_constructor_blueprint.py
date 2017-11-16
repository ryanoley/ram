import json


class DataConstructorBlueprint(object):

    def __init__(self,
                 constructor_type=None,
                 market_data_flag=False,
                 strategy_name=None,
                 blueprint_json=None):
        """
        Parameters
        ----------
        constructor_type : str
            ['etfs', 'seccodes', 'universe']
        market_data_flag : bool
            Whether to create market data for data version
        strategy_name : str
            Name of strategy, otherwise the blueprint will output
            to a standardized directory
        blueprint_json : dict
            From outputted, so to init from file
        """
        if blueprint_json:
            self.from_json(blueprint_json)
            return

        if strategy_name:
            self.output_dir_name = strategy_name
        else:
            self.output_dir_name = 'GeneralOutput'

        # Features are set by default
        self.features = ['PRMA10_Close']

        # Set default parameters given constructor type
        if constructor_type == 'universe':
            self.constructor_type = 'universe'
            self.universe_filter_arguments = {
                'filter': 'AvgDolVol',
                'where': 'MarketCap >= 200 and Close_ between 15 and 1000',
                'univ_size': 10
            }
            self.universe_date_parameters = {
                'frequency': 'Q',
                'quarter_frequency_month_offset': 1,
                'train_period_length': 4,
                'test_period_length': 1,
                'start_year': 2017
            }

        elif constructor_type == 'etfs':
            self.constructor_type = 'etfs'
            self.etfs_filter_arguments = {
                'tickers': ['SPY'],
                'start_date': '2010-01-01',
                'end_date': '2015-01-01'
            }

        elif constructor_type == 'seccodes':
            self.constructor_type = 'seccodes'
            self.seccodes_filter_arguments = {
                'seccodes': [6027, 36799],
                'start_date': '2010-01-01',
                'end_date': '2015-01-01'
            }

        elif constructor_type == 'indexes':
            self.constructor_type = 'indexes'
            self.indexes_filter_arguments = {
                'seccodes': [50311],
                'start_date': '2010-01-01',
                'end_date': '2015-01-01'
            }

        else:
            raise ValueError('constructor_type not correct')

        # Market data parameters
        if market_data_flag:
            self.market_data_params = {
                'features': ['AdjClose', 'PRMA10_AdjClose', 'PRMA20_AdjClose',
                             'VOL10_AdjClose', 'VOL20_AdjClose',
                             'RSI10_AdjClose', 'RSI20_AdjClose',
                             'BOLL10_AdjClose', 'BOLL20_AdjClose'],
                'seccodes': [50311, 61258, 61259, 11097, 11099, 11100, 10955,
                             11101, 11102, 11096, 11103, 11104, 11113,
                             11132814, 10922530]
            }

    def to_json(self):
        output = {}
        output['output_dir_name'] = self.output_dir_name
        output['features'] = self.features
        if self.constructor_type == 'universe':
            output['constructor_type'] = 'universe'
            output['universe_filter_arguments'] = \
                self.universe_filter_arguments
            output['universe_date_parameters'] = \
                self.universe_date_parameters

        elif self.constructor_type == 'etfs':
            output['constructor_type'] = 'etfs'
            output['etfs_filter_arguments'] = \
                self.etfs_filter_arguments

        elif self.constructor_type == 'seccodes':
            output['constructor_type'] = 'seccodes'
            output['seccodes_filter_arguments'] = \
                self.seccodes_filter_arguments

        elif self.constructor_type == 'indexes':
            output['constructor_type'] = 'indexes'
            output['indexes_filter_arguments'] = \
                self.indexes_filter_arguments
        return output

    def from_json(self, blueprint_json):
        for key, val in blueprint_json.iteritems():
            setattr(self, key, val)
        return


class DataConstructorBlueprintContainer(object):

    def __init__(self):
        self._index = 0
        self._blueprints = {}

    def add_blueprint(self, blueprint, description):
        name = 'blueprint_{:04d}'.format(self._index+1)
        self._blueprints[name] = {
            'blueprint': blueprint,
            'description': description,
            'key': self._index,
        }
        self._index += 1

    def get_blueprint_by_name_or_index(self, index):
        try:
            index = int(index)
        except:
            pass
        if isinstance(index, str):
            return self._blueprints[index]['blueprint']
        else:
            for k, b in self._blueprints.iteritems():
                if b['key'] == index:
                    return b['blueprint']

    def __repr__(self):
        out_string = ' ~~ Available Blueprints ~~\n'
        out_string += ' Key\tVersion\t\tDescription\n'
        out_string += ' ---\t-------\t\t-----------\n'
        keys = self._blueprints.keys()
        keys.sort()
        for k in keys:
            b = self._blueprints[k]
            out_string += ' [{}]\t{}\t{}\n'.format(
                b['key'], k, b['description'])
        return out_string

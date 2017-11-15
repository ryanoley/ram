
class DataConstructorBlueprint(object):

    def __init__(self,
                 strategy,
                 constructor_type):
        """
        Parameters
        ----------
        constructor_type : str
        """
        assert isinstance(strategy, Strategy)
        self.strategy_name = strategy.__class__.__name__

    def get_constructor_type(self):
        pass

    def run_index(self, index):
        pass

    def get_column_parameters(self):
        return []

    def get_features(self):
        return ['AvgDolVol', 'PRMA10_Close']

    def get_univ_date_parameters(self):
        return {
            'frequency': 'Q',
            'quarter_frequency_month_offset': 1,
            'train_period_length': 4,
            'test_period_length': 2,
            'start_year': 2017
        }

    def get_univ_filter_args(self):
        return {
            'filter': 'AvgDolVol',
            'where': 'MarketCap >= 200 and GSECTOR not in (55) ' +
            'and Close_ between 15 and 1000',
            'univ_size': 10}


from ram.strategy.base import Strategy


class LongPeadStrategy(Strategy):

    # ~~~~~~ Overwritten methods ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_features(self):
        return ['a', 'b']

    def process_raw_data(self, data, time_index, market_data=None):
        return None

    def run_index(self, index):
        return None

    def get_column_parameters(self):
        return None

    # ~~~~~~ Overwritten methods ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


if __name__ == '__main__':

    from ram.strategy.base import make_argument_parser

    make_argument_parser(LongPeadStrategy)

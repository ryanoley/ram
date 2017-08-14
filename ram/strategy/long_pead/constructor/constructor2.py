import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.long_pead.utils import make_variable_dict

from ram.strategy.long_pead.constructor.portfolio import Portfolio
from ram.strategy.long_pead.constructor.constructor1 import PortfolioConstructor1


class PortfolioConstructor2(PortfolioConstructor1):

    def get_args(self):
        return {
            'logistic_spread': [0.01, 0.1, 0.5, 1]
        }

    def get_position_sizes(self, scores, logistic_spread):
        pass

import numpy as np

from ram.strategy.sandbox.base.features import two_var_signal


class SignalModel1(object):

    def get_args(self):
        return {
            'sort_feature': [
                # Technicals
                'PRMAH20_AdjClose', 'PRMAH60_AdjClose', 'PRMAH120_AdjClose'
            ],
            'binary_feature':[
                'VolMax4', 'VolMin4', 'RSIMax4', 'RSIMin4',
                'MFIMax4', 'MFIMin4', 'BOLLMax4', 'BOLLMin4'
            ],
            'sort_pct': [.33]
        }

    def trade_signals(self, test_data, sort_feature, binary_feature, sort_pct):

        binary_pivot = test_data.pivot(index='Date', columns='SecCode',
                                       values=binary_feature)
        sort_pivot = test_data.pivot(index='Date', columns='SecCode',
                                      values=sort_feature)

        signals = two_var_signal(binary_pivot, sort_pivot, sort_pct)
        self.signals = signals
        self.sort_feature = sort_feature
        self.binary_feature = binary_feature
        self.sort_pct = sort_pct
        return


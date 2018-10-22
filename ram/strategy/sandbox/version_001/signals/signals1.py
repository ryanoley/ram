import numpy as np

from ram.strategy.sandbox.base.features import two_var_signal


class SignalModel1(object):

    def get_args(self):
        return {
            'rsi_feature': [
                # Technicals
                'rsi_2', 'rsi_5'
            ],
            'prma_filter':[
                'PRMA120', 'PRMA250'
            ],
            'sig_pct': [5, 10]
        }

    def trade_signals(self, test_data, rsi_feature, prma_filter, sig_pct):
        rsi_pivot = test_data.pivot(index='Date', columns='SecCode',
                                    values=rsi_feature)
        rsi_pivot[:] = np.where(rsi_pivot < sig_pct, 1,
                                np.where(rsi_pivot > (100 - sig_pct), -1, 0))

        abv_pivot = test_data.pivot(index='Date', columns='SecCode',
                                      values='Abv_{}'.format(prma_filter))

        blw_pivot = test_data.pivot(index='Date', columns='SecCode',
                                    values='Blw_{}'.format(prma_filter))

        output = rsi_pivot.copy()
        output[:] = 0
        short_signals = ((rsi_pivot == -1) & (blw_pivot==1)).astype(int)
        long_signals = ((rsi_pivot == 1) & (abv_pivot==1)).astype(int)
        output += long_signals - short_signals

        out_df = output.unstack().reset_index()
        out_df.columns = ['SecCode', 'Date', 'signal']

        exit_pivot = test_data.pivot(index='Date', columns='SecCode',
                                    values='PRMAH10_AdjClose')
        exit_pivot[:] = np.where(exit_pivot >=0., 1, -1)
        exit_df = exit_pivot.unstack().reset_index()
        exit_df.columns = ['SecCode', 'Date', 'signal']

        out_df = output.unstack().reset_index()
        out_df.columns = ['SecCode', 'Date', 'signal']

        self.signals = out_df
        self.exits = exit_df
        self.rsi_feature = rsi_feature
        self.prma_filter = prma_filter
        self.sig_pct = sig_pct
        return



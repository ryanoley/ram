import os
import numpy as np
import pandas as pd
import datetime as dt

from scipy.stats.mstats import winsorize

from ram.strategy.long_pead.utils import ern_date_blackout
from ram.strategy.long_pead.utils import make_anchor_ret_rank
from ram.strategy.long_pead.utils import ern_return
from ram.strategy.long_pead.utils import outlier_rank
from ram.strategy.long_pead.utils import make_variable_dict
from ram.strategy.long_pead.utils import simple_responses
from ram.strategy.long_pead.utils import smoothed_responses

from ram.strategy.long_pead.data.base import BaseDataContainer
from ram.strategy.long_pead.data.pairs_selector import PairSelector
from ram.strategy.long_pead.data.pairs_selector_filter import \
    PairSelectorFilter

from gearbox import create_time_index, convert_date_array


class DataContainerPairs(BaseDataContainer):

    def __init__(self):
        self._time_index_data_for_responses = {}
        self._time_index_response_data = {}
        # Deliverable
        self._processed_train_data = pd.DataFrame()
        self._processed_test_data = pd.DataFrame()
        self._features = []

    def get_args(self):
        return {
            'response_params': [
                {'type': 'smoothed',
                 'response_days': [2, 4, 6],
                 'response_thresh': 0.3},
                {'type': 'smoothed',
                 'response_days': [2],
                 'response_thresh': 0.4},
                # {'type': 'simple', 'response_days': 2},
            ],
            'training_qtrs': [-99, 20]
        }

    def prep_data(self, time_index, response_params, training_qtrs):
        """
        This is the function that adjust the hyperparameters for further
        down stream. Signals and the portfolio constructor expect the
        train/test_data and features objects.
        """
        response_data = self._get_smoothed_response_data(
            time_index,
            response_params['response_days'],
            response_params['response_thresh'])

        # Fresh copies of processed raw data
        train_data, test_data, features = self._get_train_test_features()

        # Adjust per hyperparameters
        train_data = self._trim_training_data(train_data, training_qtrs)
        # Merge response data
        train_data = train_data.merge(response_data)
        test_data = test_data.merge(response_data)
        self.train_data = train_data
        self.test_data = test_data
        self.features = features

    def add_data(self, data, time_index):
        """
        Takes in raw data, processes it and caches it
        """
        # Pair data
        pair_info, spreads, zscores = PairSelector().rank_pairs(
            data, 20, filter_n_pairs_per_seccode=30)
        self.zscores = zscores.loc[data.Date[data.TestFlag].unique()]
        self.zscores_pair_info = pair_info
        # Trim only one quarter's worth of training data
        min_date = data.Date[data.TestFlag].min()
        trim_date = min_date - dt.timedelta(days=80)
        trim_date = dt.date(trim_date.year, trim_date.month, 1)
        data = data[data.Date >= trim_date].copy()
        # Separated for testing ease
        data, features = self._process_data(data)
        # Add market data
        data = data.merge(self._market_data, how='left').fillna(0)
        features_mkt = self._market_data.columns.tolist()
        features_mkt.remove('Date')
        features += features_mkt
        # Separate training from test data
        self._processed_train_data = \
            self._processed_train_data.append(data[~data.TestFlag])
        self._processed_test_data = data[data.TestFlag]
        self.features = features
        self._time_index_data_for_responses[time_index] = \
            data[['SecCode', 'Date', 'AdjClose', 'TestFlag', 'RClose',
                  'AvgDolVol', 'MarketCap']]

    def _process_data(self, data):
        """
        Separated from add data for testing purposes
        """
        proposed_features = [
            'PRMA120_AvgDolVol', 'PRMA10_AdjClose',
            'PRMA20_AdjClose', 'BOLL10_AdjClose', 'BOLL20_AdjClose',
            'BOLL60_AdjClose', 'MFI10_AdjClose', 'MFI20_AdjClose',
            'MFI60_AdjClose', 'RSI10_AdjClose', 'RSI20_AdjClose',
            'RSI60_AdjClose', 'VOL10_AdjClose', 'VOL20_AdjClose',
            'VOL60_AdjClose', 'DISCOUNT63_AdjClose', 'DISCOUNT126_AdjClose',
            'DISCOUNT252_AdjClose',
            # Accounting Variables
            'NETINCOMEQ', 'NETINCOMETTM', 'SALESQ', 'SALESTTM', 'ASSETS',
            'CASHEV', 'FCFMARKETCAP',
            'NETINCOMEGROWTHQ',
            'NETINCOMEGROWTHTTM',
            'OPERATINGINCOMEGROWTHQ',
            'OPERATINGINCOMEGROWTHTTM',
            'EBITGROWTHQ',
            'EBITGROWTHTTM',
            'SALESGROWTHQ',
            'SALESGROWTHTTM',
            'FREECASHFLOWGROWTHQ',
            'FREECASHFLOWGROWTHTTM',
            'GROSSPROFASSET',
            'GROSSMARGINTTM',
            'EBITDAMARGIN',
            'PE',
            # StarMine variables
            'LAG1_ARM', 'LAG1_ARMREVENUE', 'LAG1_ARMRECS', 'LAG1_ARMEARNINGS',
            'LAG1_ARMEXRECS', 'LAG1_SIRANK', 'LAG1_SIMARKETCAPRANK',
            'LAG1_SISECTORRANK', 'LAG1_SIUNADJRANK', 'LAG1_SISHORTSQUEEZE',
            'LAG1_SIINSTOWNERSHIP',
            # IBES
            'IBES_Discount', 'IBES_Discount_PRMA',
        ] + ['IBES_Increases_Tm{}'.format(i) for i in range(1, 6)] + \
            ['IBES_Decreases_Tm{}'.format(i) for i in range(1, 6)]

        # IBES feature processing
        if 'PTARGETUNADJ' in data.columns:
            data = data.merge(make_ibes_increases_decreases_binaries(data))
            data['IBES_Discount'] = winsorize(data.PTARGETUNADJ / data.RClose - 1, limits=(0.005, 0.005))
            discounts = data.pivot(index='Date', columns='SecCode', values='IBES_Discount')
            # PRMA for discount
            discounts_smooth = discounts.rolling(4).mean()
            discounts_feature = (discounts / discounts_smooth).rank(pct=True).unstack().reset_index().fillna(0.5)
            discounts_feature.columns = ['SecCode', 'Date', 'IBES_Discount_PRMA']
            data = data.merge(discounts_feature)

        # Get features from data columns
        features = list(set(proposed_features).intersection(data.columns))

        # ~~~~~~ CLEAN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        data.AdjVwap = np.where(
            data.AdjVwap.isnull(), data.AdjClose, data.AdjVwap)
        data.AdjClose = np.where(
            data.AdjClose.isnull(), data.AdjVwap, data.AdjClose)

        # SPLITS: Instead of using the levels, use the CHANGE in levels.
        # This is necessary for the updating of positions and prices downstream
        data.loc[:, 'SplitMultiplier'] = \
            data.SplitFactor.pct_change().fillna(0) + 1

        # Process gics and Sector
        data['GGROUP'] = data.GGROUP.fillna(0).astype(int).astype(str)
        data['GSECTOR'] = data.GGROUP.apply(lambda x: x[:2])

        # Blackout flags and anchor returns
        data = ern_date_blackout(data, offset1=-2, offset2=4)

        data = make_anchor_ret_rank(data, init_offset=3, window=10)

        data = ern_return(data)

        # Handle nan values, rank and create binaries for extreme values
        data2 = outlier_rank(data, features[0])
        for f in features[1:]:
            data2 = data2.merge(outlier_rank(data, f))
        data = data.drop(features, axis=1)
        data = data.merge(data2)
        features = features + [f + '_extreme' for f in features] + \
            ['blackout', 'anchor_ret_rank', 'earnings_ret']
        return data, features

    def add_market_data(self, data):
        if hasattr(self, '_market_data'):
            return

        name_map = pd.DataFrame([
            ('11132814', 'ShortVIX'),
            ('11113', 'VIX'),
            ('10922530', 'LongVIX'),

            ('11097', 'R1000Index'),
            ('11099', 'R1000Growth'),
            ('11100', 'R1000Value'),

            ('10955',  'R2000Index'),
            ('11101', 'R2000Growth'),
            ('11102', 'R2000Value'),

            ('11096', 'R3000Index'),
            ('11103', 'R3000Growth'),
            ('11104', 'R3000Value'),

            ('50311', 'SP500Index'),
            ('61258', 'SP500Growth'),
            ('61259', 'SP500Value'),
        ], columns=['SecCode', 'IndexName'])
        data = data.merge(name_map)

        features = [
            'AdjClose', 'PRMA10_AdjClose', 'PRMA20_AdjClose',
            'VOL10_AdjClose', 'VOL20_AdjClose', 'RSI10_AdjClose',
            'RSI20_AdjClose', 'BOLL10_AdjClose', 'BOLL20_AdjClose'
        ]

        market_data = pd.DataFrame()
        for f in features:
            pdata = data.pivot(index='Date', columns='IndexName', values=f)
            # Only keep levels (AdjClose) for VIX indexes
            if f == 'AdjClose':
                pdata = pdata[['ShortVIX', 'VIX', 'LongVIX']]
            pdata.columns = ['Mkt_{}_{}'.format(
                col, f.replace('_AdjClose', '')) for col in pdata.columns]
            market_data = market_data.join(pdata, how='outer')
        # Nan Values set to medians of rows
        market_data = market_data.fillna(market_data.median())
        self._market_data = market_data.reset_index()

    ###########################################################################

    def _get_train_test_features(self):
        return (
            self._processed_train_data.copy(),
            self._processed_test_data.copy(),
            self.features
        )

    def _trim_training_data(self, data, training_qtrs):
        if training_qtrs == -99:
            return data
        inds = create_time_index(data.Date)
        max_ind = np.max(inds)
        return data.iloc[inds > (max_ind-training_qtrs)]

    def _get_smoothed_response_data(self, time_index, response_days,
                                    response_thresh):
        # Process input
        if not isinstance(response_days, list):
            response_days = [response_days]
        resp_dict = self._time_index_response_data
        # Cached data exist for this particular quarter?
        if time_index not in resp_dict:
            resp_dict[time_index] = {}
        data_name = str((response_days, response_thresh))
        if data_name not in resp_dict[time_index]:
            resp_dict[time_index][data_name] = smoothed_responses(
                self._time_index_data_for_responses[time_index],
                days=response_days, thresh=response_thresh)
        # Stack time indexes
        time_indexes = resp_dict.keys()
        time_indexes.sort()
        responses = pd.DataFrame()
        for t in time_indexes[:-1]:
            temp_r = resp_dict[t][data_name]
            responses = responses.append(temp_r[~temp_r.TestFlag])
        t = time_indexes[-1]
        return responses.append(resp_dict[t][data_name])


def make_ibes_increases_decreases_binaries(data):
    price_targets = data.pivot(index='Date', columns='SecCode',
                               values='PTARGETMEAN')
    for i in range(1, 6):
        target_changes = price_targets.pct_change(1).shift(i)
        f1 = (target_changes > 0).astype(int).unstack().reset_index()
        f1.columns = ['SecCode', 'Date', 'IBES_Increases_Tm{}'.format(i)]
        f2 = (target_changes < 0).astype(int).unstack().reset_index()
        f2.columns = ['SecCode', 'Date', 'IBES_Decreases_Tm{}'.format(i)]
        if i == 1:
            output = f1.merge(f2)
        else:
            output = output.merge(f1).merge(f2)
    return output

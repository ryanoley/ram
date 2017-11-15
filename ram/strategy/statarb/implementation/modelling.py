import os
import numpy as np
import pandas as pd
import datetime as dt

from sklearn.ensemble import ExtraTreesClassifier
from sklearn.externals import joblib

from ram.strategy.base import Strategy

from ram.strategy.long_pead.utils import ern_date_blackout
from ram.strategy.long_pead.utils import make_anchor_ret_rank
from ram.strategy.long_pead.utils import ern_return
from ram.strategy.long_pead.utils import outlier_rank
from ram.strategy.long_pead.utils import smoothed_responses

from gearbox import create_time_index, convert_date_array


class LongPeadStrategy(Strategy):

    def run_index(self, index):
        """
        Takes in integer
        """
        pass

    def get_column_parameters(self):
        """
        Takes in integer
        """
        pass

    def get_features(self):
        pass


class DataContainer(object):

    def __init__(self):
        self._processed_train_data = pd.DataFrame([])

    def add_data(self, data):
        """
        Takes in raw data, processes it and caches it
        """
        # Trim only one quarter's worth of training data
        min_date = data.Date[data.TestFlag].min()
        trim_date = min_date - dt.timedelta(days=80)
        trim_date = dt.date(trim_date.year, trim_date.month, 1)
        data = data[data.Date >= trim_date].copy()
        # Separated for testing ease
        data, features = self._process_data(data)
        # Add market data
        if hasattr(self, '_market_data') and np.any(self._market_data):
            data = data.merge(self._market_data, how='left').fillna(0)
            features_mkt = self._market_data.columns.tolist()
            features_mkt.remove('Date')
            features += features_mkt
        self._processed_train_data = \
            self._processed_train_data.append(data[~data.TestFlag])
        self.features = features

    def temp(self):
        # Pair data
        pair_info, spreads, zscores = PairSelector().rank_pairs(
            data, 20, filter_n_pairs_per_seccode=30)
        self.zscores = zscores.loc[data.Date[data.TestFlag].unique()]
        self.zscores_pair_info = pair_info

    def prep_data(self, response_params, training_qtrs):
        """
        This is the function that adjust the hyperparameters for further
        down stream. Signals and the portfolio constructor expect the
        train/test_data and features objects.
        """
        response_data = self._get_smoothed_response_data(
            response_params['response_days'],
            response_params['response_thresh'])
        # Fresh copies of processed raw data
        train_data, features = self._get_train_test_features()
        # Adjust per hyperparameters
        train_data = self._trim_training_data(train_data, training_qtrs)
        # Merge response data
        train_data = train_data.merge(response_data)
        self.train_data = train_data
        self.features = features

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
        ]

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

        data.Date = convert_date_array(data.Date)
        data.SecCode = data.SecCode.astype(int).astype(str)

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
            self.features
        )

    def _trim_training_data(self, data, training_qtrs):
        if training_qtrs == -99:
            return data
        inds = create_time_index(data.Date)
        max_ind = np.max(inds)
        return data.iloc[inds > (max_ind-training_qtrs)]

    def _get_smoothed_response_data(self, response_days, response_thresh):
        # Process input
        if not isinstance(response_days, list):
            response_days = [response_days]
        responses = smoothed_responses(
            self._processed_train_data,
            days=response_days,
            thresh=response_thresh)
        return responses[['SecCode', 'Date', 'Response']]


class SignalModel(object):

    def __init__(self):
        pass

    def make_cache_model(self,
                         model_cache_path,
                         train_data,
                         features,
                         model_params,
                         drop_accounting,
                         drop_extremes,
                         drop_starmine,
                         drop_market_variables):

        if drop_accounting:
            accounting_vars = [
                'NETINCOMEQ', 'NETINCOMETTM', 'SALESQ', 'SALESTTM',
                'ASSETS', 'CASHEV', 'FCFMARKETCAP', 'NETINCOMEGROWTHQ',
                'NETINCOMEGROWTHTTM', 'OPERATINGINCOMEGROWTHQ',
                'OPERATINGINCOMEGROWTHTTM', 'EBITGROWTHQ', 'EBITGROWTHTTM',
                'SALESGROWTHQ', 'SALESGROWTHTTM', 'FREECASHFLOWGROWTHQ',
                'FREECASHFLOWGROWTHTTM', 'GROSSPROFASSET', 'GROSSMARGINTTM',
                'EBITDAMARGIN', 'PE']
            features = [x for x in features if x not in accounting_vars]
        if drop_extremes:
            features = [x for x in features if x.find('extreme') == -1]
        if drop_market_variables == 'constrained':
            features = [x for x in features if x.find('Mkt_') == -1]
            features.extend(['Mkt_VIX_AdjClose', 'Mkt_VIX_PRMA10',
                             'Mkt_SP500Index_VOL10', 'Mkt_SP500Index_PRMA10',
                             'Mkt_SP500Index_BOLL20'])
        elif drop_market_variables:
            features = [x for x in features if x.find('Mkt_') == -1]
        if drop_starmine:
            starmine_vars = [
                'LAG1_ARM', 'LAG1_ARMREVENUE', 'LAG1_ARMRECS',
                'LAG1_ARMEARNINGS', 'LAG1_ARMEXRECS', 'LAG1_SIRANK',
                'LAG1_SIMARKETCAPRANK', 'LAG1_SISECTORRANK',
                'LAG1_SIUNADJRANK', 'LAG1_SISHORTSQUEEZE',
                'LAG1_SIINSTOWNERSHIP']
            features = [x for x in features if x not in starmine_vars]

        clf = ExtraTreesClassifier(n_jobs=-1, **model_params)

        clf.fit(X=train_data[features],
                y=train_data['Response'])

        joblib.dump(clf, model_cache_path)

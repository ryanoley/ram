import numpy as np
import pandas as pd

from abc import ABCMeta, abstractmethod, abstractproperty


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def clean_pivot_raw_data(data, value_column, lag=0):
    """
    """
    assert lag >= 0
    data = data.pivot(index='Date', columns='SecCode',
                      values=value_column).shift(lag)
    # Allow to fill up to five days of missing data if there was a
    # previous data point
    data = data.fillna(method='pad', limit=5)
    return data


def data_rank(pdata):
    """
    Simple clean and rank of data. Assumed to be pivoted already by
    clean_pivot_raw_data.
    """
    # For single day ranking, convert to DataFrame
    if isinstance(pdata, pd.Series):
        pdata = pdata.to_frame().T
    # It is assumed that these are the correct labels
    pdata.index.name = 'Date'
    pdata.columns.name = 'SecCode'
    ranks = pdata.rank(axis=1, pct=True)
    return ranks


def data_fill_median(data):
    row_median = data.median(axis=1).fillna(method='backfill')
    fill_df = pd.concat([row_median] * data.shape[1], axis=1)
    fill_df.columns = data.columns
    fill_df.index = data.index
    data = data.fillna(fill_df)
    return data


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class FeatureAggregator(object):

    def __init__(self, live_flag=False):
        self._data = pd.DataFrame()

    def add_feature(self, data, label, fill_median=True):
        """
        Assumes input has SecCodes in the columns, dates in the row index,
        essentially from the Pivot table. If it is a Series, it is assumed
        that the indexes are the SecCodes
        """
        if isinstance(data, pd.Series):
            # It is assumed that these are the correct labels
            data = data.to_frame().T
        if fill_median:
            data = data_fill_median(data)
        data = data.unstack().reset_index()
        data.columns = ['SecCode', 'Date', 'val']
        data['label'] = label
        self._data = self._data.append(data)

    def make_dataframe(self):
        """
        Will put back in missing values if there was nothing to handle
        """
        features = self._data.label.unique().tolist()
        features.sort()
        output = self._data.pivot_table(
            values='val', index=['SecCode', 'Date'],
            columns='label', aggfunc='sum').reset_index()
        # Add back in missing columns
        output = output.loc[:, ['SecCode', 'Date']+features]
        del output.index.name
        del output.columns.name
        return output


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BaseTechnicalFeature(object):

    __metaclass__ = ABCMeta

    def __init__(self, live_flag=False):
        if live_flag:
            self.fit = self.calculate_last_date
        else:
            self.fit = self.calculate_all_dates

    @abstractmethod
    def calculate_all_dates(self):
        raise NotImplementedError('BaseTechnicalFeature.calculate_all_dates')

    @abstractmethod
    def calculate_last_date(self):
        raise NotImplementedError('BaseTechnicalFeature.calculate_last_date')


class PRMA(BaseTechnicalFeature):

    def calculate_all_dates(self, data, window):
        return data / data.rolling(window).mean()

    def calculate_last_date(self, data, window):
        return data.iloc[-1] / data.iloc[-window:].mean()


class VOL(BaseTechnicalFeature):

    def calculate_all_dates(self, data, window):
        return data.pct_change().rolling(window).std()

    def calculate_last_date(self, data, window):
        return data.iloc[-(window+1):].pct_change().std()


class DISCOUNT(BaseTechnicalFeature):

    def calculate_all_dates(self, data, window):
        return data / data.rolling(window).max()

    def calculate_last_date(self, data, window):
        return data.iloc[-1] / data.iloc[-window:].max()


class BOLL(BaseTechnicalFeature):
    """
    (P - (AvgP - 2 * StdP)) / (4 * StdPrice)
    """
    def calculate_all_dates(self, data, window):
        # Set to zero for ease of testing
        std_price = data.rolling(window).std(ddof=0)
        return (data - (data.rolling(window).mean() - 2*std_price)) / \
            (4*std_price)

    def calculate_last_date(self, data, window):
        # Set to zero for ease of testing
        std_price = data.iloc[-window:].std(ddof=0)
        return (data.iloc[-1] - (data.iloc[-window:].mean() - 2*std_price)) / \
            (4*std_price)


class MFI(BaseTechnicalFeature):
    """
    Typical Price = (High + Low + Close)/3

    Raw Money Flow = Typical Price x Volume

    Money Flow Ratio = (14-period Positive Money Flow)/
        (14-period Negative Money Flow)

    Money Flow Index = 100 - 100/(1 + Money Flow Ratio)

    [http://stockcharts.com/school/doku.php?id=chart_school:
    technical_indicators:money_flow_index_mfi]
    """

    def calculate_all_dates(self, high, low, close, volume, window):
        typ_price = (high + low + close) / 3.
        lag_typ_price = typ_price.shift(1)
        raw_mf = typ_price * volume
        mf_pos = pd.DataFrame(np.where(typ_price > lag_typ_price, raw_mf, 0))
        mf_pos = mf_pos.rolling(window).sum()
        mf_neg = pd.DataFrame(np.where(typ_price < lag_typ_price, raw_mf, 0))
        mf_neg = mf_neg.rolling(window).sum()
        mfi = 100 - 100 / (1 + (mf_pos / mf_neg))
        mfi.columns = typ_price.columns
        mfi.index = typ_price.index
        return mfi

    def calculate_last_date(self, high, low, close, volume, window):
        typ_price = ((high.iloc[-(window+1):] + low.iloc[-(window+1):] +
                     close.iloc[-(window+1):]) / 3.).values
        typ_price[np.isnan(typ_price)] = 0
        lag_typ_price = typ_price[:-1]
        typ_price = typ_price[1:]
        # Using `.shape` here is for making tests work, which is unfortunate
        raw_mf = typ_price * volume.iloc[-typ_price.shape[0]:].values
        mf_pos = (typ_price > lag_typ_price) * raw_mf
        mf_pos = np.sum(mf_pos, axis=0)
        mf_neg = (typ_price < lag_typ_price) * raw_mf
        mf_neg = np.sum(mf_neg, axis=0)
        # Numpy printing warning
        with np.errstate(divide='ignore'):
            mfi = pd.Series(100 - 100 / (1 + (mf_pos / mf_neg)),
                            index=high.columns)
        return mfi


class RSI(BaseTechnicalFeature):
    """
    RS = Average Gain / Average Loss
    RSI = 100 - 100 / (1 + RS)
    """
    def calculate_all_dates(self, data, window):
        changes = data.pct_change()
        gain = pd.DataFrame(np.where(changes > 0, changes, 0))
        loss = pd.DataFrame(np.where(changes < 0, -changes, 0))
        avg_gain = gain.rolling(window).mean()
        avg_loss = loss.rolling(window).mean()
        rsi = 100 - 100 / (1 + (avg_gain / avg_loss))
        rsi.columns = changes.columns
        rsi.index = changes.index
        return rsi

    def calculate_last_date(self, data, window):
        window = window + 1
        changes = data.iloc[-window:].pct_change()
        gain = pd.DataFrame(np.where(changes > 0, changes, 0))
        loss = pd.DataFrame(np.where(changes < 0, -changes, 0))
        avg_gain = gain.mean()
        avg_loss = loss.mean()
        rsi = 100 - 100 / (1 + (avg_gain / avg_loss))
        rsi.index = changes.columns
        return rsi

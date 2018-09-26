import numpy as np
import pandas as pd

from abc import ABCMeta, abstractmethod, abstractproperty


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def clean_pivot_raw_data(data, value_column, lag=0, column='SecCode'):
    """
    NOTE: This function will fill/'pad' a maximum of 5 days worth of missing
    data going forward.
    """
    assert lag >= 0
    data = data.pivot(index='Date', columns=column,
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


def data_fill_median(data, backfill=False):
    if backfill:
        row_median = data.median(axis=1).fillna(method='backfill')
    else:
        row_median = data.median(axis=1)
    fill_df = pd.concat([row_median] * data.shape[1], axis=1)
    fill_df.columns = data.columns
    fill_df.index = data.index
    data = data.fillna(fill_df)
    return data


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class FeatureAggregator(object):

    def __init__(self, live_flag=False):
        self._data = pd.DataFrame()

    def add_feature(self, data, label, fill_median=True, backfill=False):
        """
        Assumes input has SecCodes in the columns, dates in the row index,
        essentially from the Pivot table. If it is a Series, it is assumed
        that the indexes are the SecCodes
        """
        if isinstance(data, pd.Series):
            # It is assumed that these are the correct labels
            data = data.to_frame().T
        if fill_median:
            data = data_fill_median(data, backfill)
        data = data.unstack().reset_index()
        data.columns = ['SecCode', 'Date', 'val']
        data['label'] = label
        self._data = self._data.append(data)

    def make_dataframe(self):
        """
        Will put back in missing values if there was nothing to handle
        """
        output = self._data.pivot_table(
            values='val', index=['SecCode', 'Date'],
            columns='label', aggfunc='mean').reset_index()
        del output.columns.name
        # Sorted output features
        features = self._data.label.unique().tolist()
        features.sort()
        features = ['SecCode', 'Date'] + features
        # Add back in missing columns
        missing_cols = set(features) - set(output.columns)
        output = output.join(pd.DataFrame(columns=missing_cols))
        output = output[features]
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
        assert len(data) >= window
        return data / data.rolling(window).mean()

    def calculate_last_date(self, data, window):
        assert len(data) >= window
        return data.iloc[-1] / data.iloc[-window:].mean().values


class VOL(BaseTechnicalFeature):

    def calculate_all_dates(self, data, window):
        assert len(data) >= window
        return data.pct_change().rolling(window).std()

    def calculate_last_date(self, data, window):
        assert len(data) >= window
        out = data.iloc[-(window+1):].pct_change().std()
        out.name = data.index[-1]
        return out


class DISCOUNT(BaseTechnicalFeature):

    def calculate_all_dates(self, data, window):
        assert len(data) >= window
        return data / data.rolling(window).max()

    def calculate_last_date(self, data, window):
        assert len(data) >= window
        return data.iloc[-1] / data.iloc[-window:].max().values


class BOLL(BaseTechnicalFeature):
    """
    (P - (AvgP - 2 * StdP)) / (4 * StdPrice)
    """
    def calculate_all_dates(self, data, window):
        assert len(data) >= window
        std_price = data.rolling(window).std(ddof=0)
        return (data - (data.rolling(window).mean() - 2*std_price)) / \
            (4*std_price)

    def calculate_last_date(self, data, window):
        assert len(data) >= window
        std_price = data.iloc[-window:].std(ddof=0).values
        mean_price = data.iloc[-window:].mean().values
        return (data.iloc[-1] - (mean_price - 2*std_price)) / (4*std_price)


class BOLL_SMOOTH(BaseTechnicalFeature):
    """
    (P - (AvgP - 2 * StdP)) / (4 * StdPrice)
    """
    def calculate_all_dates(self, data, smooth, window):
        assert len(data) >= window
        assert smooth < window
        # Set to zero for ease of testing
        std_price = data.rolling(window).std(ddof=0)
        return (data.rolling(smooth).mean() -
                (data.rolling(window).mean() - 2*std_price)) / \
            (4*std_price)

    def calculate_last_date(self, data, smooth, window):
        assert len(data) >= window
        assert smooth < window
        # Set to zero for ease of testing
        std_price = data.iloc[-window:].std(ddof=0)
        out = (data.rolling(smooth).mean().iloc[-1] -
               (data.iloc[-window:].mean() - 2*std_price)) / \
            (4*std_price)
        out.name = data.index[-1]
        return out


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
        assert len(high) >= window
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
        assert len(high) >= window
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
                            index=high.columns,
                            name=high.index[-1])
        return mfi


class RSI(BaseTechnicalFeature):
    """
    RS = Average Gain / Average Loss
    RSI = 100 - 100 / (1 + RS)
    """
    def calculate_all_dates(self, data, window):
        assert len(data) >= window
        changes = data.diff()
        gain = pd.DataFrame(np.where(changes > 0, changes, 0))
        loss = pd.DataFrame(np.where(changes < 0, -changes, 0))
        avg_gain = gain.rolling(window).mean()
        avg_loss = loss.rolling(window).mean()
        rsi = 100 - 100 / (1 + (avg_gain / avg_loss))
        rsi.columns = changes.columns
        rsi.index = changes.index
        return rsi

    def calculate_last_date(self, data, window):
        assert len(data) >= window
        window = window + 1
        changes = data.iloc[-window:].diff()
        gain = pd.DataFrame(np.where(changes > 0, changes, 0))
        loss = pd.DataFrame(np.where(changes < 0, -changes, 0))
        avg_gain = gain.mean()
        avg_loss = loss.mean()
        rsi = 100 - 100 / (1 + (avg_gain / avg_loss))
        rsi.index = changes.columns
        rsi.name = data.index[-1]
        return rsi

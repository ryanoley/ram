import numpy as np
import pandas as pd

from abc import ABCMeta, abstractmethod, abstractproperty


class BaseTechnicalFeature(object):

    __metaclass__ = ABCMeta

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
        return 100 - 100 / (1 + (mf_pos / mf_neg))

    def calculate_last_date(self, high, low, close, volume, window):
        window = window + 1
        typ_price = (high.iloc[-window:] + low.iloc[-window:] +
                     close.iloc[-window:]) / 3.
        lag_typ_price = typ_price.shift(1)
        raw_mf = typ_price * volume.iloc[-window:]
        mf_pos = pd.DataFrame(np.where(typ_price > lag_typ_price, raw_mf, 0))
        mf_pos = mf_pos.sum()
        mf_neg = pd.DataFrame(np.where(typ_price < lag_typ_price, raw_mf, 0))
        mf_neg = mf_neg.sum()
        return 100 - 100 / (1 + (mf_pos / mf_neg))


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
        return 100 - 100 / (1 + (avg_gain / avg_loss))

    def calculate_last_date(self, data, window):
        window = window + 1
        changes = data.iloc[-window:].pct_change()
        gain = pd.DataFrame(np.where(changes > 0, changes, 0))
        loss = pd.DataFrame(np.where(changes < 0, -changes, 0))
        avg_gain = gain.mean()
        avg_loss = loss.mean()
        return 100 - 100 / (1 + (avg_gain / avg_loss))

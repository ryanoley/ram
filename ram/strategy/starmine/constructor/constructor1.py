
import numpy as np
import pandas as pd
import datetime as dt

from ram.strategy.basic.constructor.portfolio import Portfolio
from ram.strategy.starmine.constructor.base_constructor import Constructor, filter_seccodes
from ram.strategy.starmine.utils import make_variable_dict, get_prior_business_date


LOW_PRICE_FILTER = 7
COST = .0015


class PortfolioConstructor1(Constructor):

    def get_args(self):
        return {
            'thresh': [0.005, 0.01, 0.015],
            'pos_size': [.002, .0035]
        }

    def get_daily_pl_old(self, data_container, signals, thresh, **kwargs):
        """
        Parameters
        ----------
        """

        test = data_container.test_data.copy()
        hold_days = kwargs['hold_days']
        pos_size = kwargs['pos_size']
        
        assert 'Ret{}'.format(hold_days) in test.columns
        test['Ret'] = test['Ret{}'.format(hold_days)].copy()

        longs = test[test.preds >= thresh].copy()
        shorts = test[test.preds <= -thresh].copy()
        longs = longs.groupby('Date')
        shorts = shorts.groupby('Date')

        out = pd.DataFrame(index=test.Date.unique())
        out['Long'] = longs.Ret.mean() - COST
        out['nLong'] = longs.Date.count()
        out['Short'] = shorts.Ret.mean() + COST
        out['nShort'] = shorts.Date.count()
        out.fillna(0., inplace=True)
        out['Ret'] = out.Long - out.Short
        out['nPos'] = out.nLong + out.nShort
        out.sort_index(inplace=True)
        out.Ret *= (out.nPos * pos_size)
        return out


    def get_daily_pl(self, data_container, signals, **kwargs):
        """
        Parameters
        ----------
        data_container
        signals
        kwargs
        """
        scores_dict = make_variable_dict(signals.preds_data, 'preds')

        portfolio = Portfolio()

        daily_pl_data = data_container.daily_pl_data.copy()
        unique_test_dates = np.unique(daily_pl_data.Date)
        prior_bdates = get_prior_business_date(unique_test_dates)

        # Output object
        daily_df = pd.DataFrame(index=unique_test_dates, dtype=float)

        for prior_dt, date in zip(prior_bdates, unique_test_dates):
            
            if prior_dt not in scores_dict.keys():
                scores = {}
            else:
                scores = scores_dict[prior_dt]

            vwaps = data_container.vwap_dict[date]
            closes = data_container.close_dict[date]
            dividends = data_container.dividend_dict[date]
            splits = data_container.split_mult_dict[date]

            # If close is very low, drop as well
            low_price_seccodes = filter_seccodes(closes, LOW_PRICE_FILTER)

            for seccode in set(low_price_seccodes):
                scores[seccode] = np.nan

            portfolio.update_prices(closes, dividends, splits)

            if date == unique_test_dates[-1]:
                portfolio.close_portfolio_positions()
            else:
                sizes = self.get_position_sizes(scores, daily_pl_data,
                                                date, prior_dt, **kwargs)
                sizes = self._get_position_sizes_dollars(sizes)
                portfolio.update_position_sizes(sizes, vwaps)
                daily_pl_data = self.update_pl_data(daily_pl_data, sizes,
                                     date, prior_dt)

            daily_df = update_daily_df(daily_df, portfolio, date)

        # Time Index aggregate stats
        stats = {}
        return daily_df, stats


    def get_position_sizes(self, scores, daily_pl_data, date, prior_dt,
                           thresh, pos_size):
        """
        Position sizes are determined by the ranking, and for an
        even number of scores the position sizes should be symmetric on
        both the long and short sides.
        """

        prior_df = daily_pl_data[(daily_pl_data.Date == prior_dt)].copy()
        dt_df = daily_pl_data[(daily_pl_data.Date == date)].copy()
        close_secCodes = set(dt_df.loc[(dt_df.ExitFlag == 1), 'SecCode'])

        scores = pd.Series(scores).to_frame()
        scores.columns = ['score']
        if len(scores) == 0:
            new_shorts = new_longs = set([])
        else:
            new_longs = set(scores[scores.score >= thresh].index)
            new_shorts = set(scores[scores.score <= -thresh].index)

        prev_shorts =  set(prior_df.loc[(prior_df.LiveFlag == -1), 'SecCode'])
        live_shorts = prev_shorts - close_secCodes
        live_shorts.update(new_shorts)

        prev_longs =  set(prior_df.loc[(prior_df.LiveFlag == 1), 'SecCode'])
        live_longs = prev_longs - close_secCodes
        live_longs.update(new_longs)

        output = pd.DataFrame(index=daily_pl_data.SecCode.unique(),
                              data = {'weights':0.})
        output.loc[output.index.isin(live_longs), 'weights'] = 1.
        output.loc[output.index.isin(live_shorts), 'weights'] = -1.
        
        exposure = pos_size * np.abs(output.weights).sum()
        scaled_size = (1. / exposure) * pos_size if exposure > 1. else pos_size
        output['weights'] *= scaled_size
        
        if scaled_size == pos_size:
            positions_to_update = new_longs.copy()
            positions_to_update.update(new_shorts)
            positions_to_update.update(close_secCodes)
        else:
            positions_to_update = live_longs.copy()
            positions_to_update.update(live_shorts)
            positions_to_update.update(close_secCodes)

        output = output[output.index.isin(positions_to_update)]

        return pd.Series(output.weights)


    def update_pl_data(self, data, sizes, date, prior_dt):
        assert 'LiveFlag' in data.columns
        if isinstance(sizes, dict):
            sizes = pd.Series(sizes)
        
        prev_longs = set(data.loc[(data.Date == prior_dt) &
            (data.LiveFlag == 1), 'SecCode'])
        prev_shorts = set(data.loc[(data.Date == prior_dt) &
            (data.LiveFlag == -1), 'SecCode'])

        new_longs = set(sizes[sizes > 0].index)
        new_shorts = set(sizes[sizes < 0].index)
        close_secCodes = set(sizes[sizes == 0].index)

        new_longs.update(prev_longs)
        new_shorts.update(prev_shorts)
        hold_longs = new_longs - close_secCodes
        hold_shorts = new_shorts - close_secCodes

        data.loc[(data.Date == date) & (data.SecCode.isin(hold_longs)),
            'LiveFlag'] = 1
        data.loc[(data.Date == date) & (data.SecCode.isin(hold_shorts)),
            'LiveFlag'] = -1
        return data


    def update_daily_df(self, data, portfolio, date):
        daily_df = data.copy()
        pl_long, pl_short = portfolio.get_portfolio_daily_pl()
        daily_turnover = portfolio.get_portfolio_daily_turnover()
        daily_exposure = portfolio.get_portfolio_exposure()
    
        daily_df.loc[date, 'PL'] = pl_long + pl_short
        daily_df.loc[date, 'LongPL'] = pl_long
        daily_df.loc[date, 'ShortPL'] = pl_short
        daily_df.loc[date, 'Turnover'] = daily_turnover
        daily_df.loc[date, 'Exposure'] = daily_exposure
        daily_df.loc[date, 'OpenPositions'] = sum([
            1 if x.shares != 0 else 0
            for x in portfolio.positions.values()])
        # Daily portfolio stats
        daily_stats = portfolio.get_portfolio_stats()
        daily_df.loc[date, 'stat1'] = daily_stats['stat1']
        return daily_df
    

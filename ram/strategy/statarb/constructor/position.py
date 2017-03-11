import numpy as np


class MultiLegPosition(object):

    def __init__(self, legs, prices, sizes, comm=0.005):
        """
        Parameters
        ----------
        legs : numpy arrays of positions
            The column header from the data files that is the ID
        prices : numpy arrays of positions
            The transaction prices (nominal)
        sizes : numpy arrays of positions
            The dollar value of the position being put on. SIGN IS IMPORTANT.
        comm : float
            Cost on both sides of transaction. Comm is the number of dollars
            per share rate
        """
        assert isinstance(legs, np.ndarray)
        assert isinstance(prices, np.ndarray)
        assert isinstance(sizes, np.ndarray)
        self.legs = legs
        # Flags to aid accounting
        self.open_position = True
        self.to_close_position = False
        # Commission per share
        self.COMM = comm
        # Never open position if no data
        if np.any(~np.isnan(prices)):
            # Number of shares
            self.shares = (sizes / prices).astype(int)
        else:
            self.shares = np.array([0] * len(sizes))
            self.to_close_position = True
        # Entry prices
        self.prices_entry = prices
        # Current prices
        self.prices_current = prices
        # Cost of entering the position calculated here
        self.daily_pl = -1 * np.sum(np.abs(self.shares) * comm)
        # Exposure numbers
        self.net_exposure = np.sum(self.shares * prices)
        self.gross_exposure = np.sum(np.abs(self.shares) * prices)
        # Stats
        self.stat_holding_days = 0
        self.stat_perc_gain = 0
        self.stat_rebalance_count = 0

    def update_position_prices(self, prices, dividends=None, splits=None):
        """
        Parameters
        ----------
        prices : dict
        dividends : dict
        splits : dict

        NOTES
        -----
        The column in the database is SplitFactor, but it has been manipulated
        to the percent change in this factor. As an example, when AAPL
        did a 7:1 split in June 2014, the SplitFactor went from .147 to 1,
        or a 700% change. So sp1 will be 7, and the shares should be
        multiplied by 7 and the entry price divided by 7.
        """
        # Pull prices and things from dicts
        new_prices = np.array([])
        new_dividends = np.array([])
        new_splits = np.array([])
        for leg in self.legs:
            new_prices = np.append(new_prices, prices[leg])
            if dividends:
                new_dividends = np.append(new_dividends, dividends[leg])
            else:
                new_dividends = np.append(new_dividends, 0)
            if splits:
                new_splits = np.append(new_splits, splits[leg])
            else:
                new_splits = np.append(new_splits, 1)

        # Handle Splits
        if np.any(new_splits != 1):
            self.shares = self.shares * new_splits
            self.prices_entry = self.prices_entry / new_splits
            self.prices_current = self.prices_current / new_splits

        # Handle NAN prices
        if np.any(np.isnan(new_prices)):
            inds = ~np.isnan(new_prices)
            self.daily_pl = np.sum((new_prices[inds] -
                                   self.prices_current[inds]) *
                                   self.shares[inds])
            # Dividend income
            self.daily_pl += np.sum(self.shares[inds] * new_dividends[inds])
            self.net_exposure = 0
            self.gross_exposure = 0
            self.to_close_position = True

            # Stats
            self.stat_perc_gain = self.stat_perc_gain + (
                self.daily_pl / np.sum(np.abs(self.shares) *
                                       self.prices_entry))

        else:
            self.daily_pl = \
                np.sum((new_prices - self.prices_current) * self.shares)
            # Dividend income
            self.daily_pl += np.sum(self.shares * new_dividends)
            self.net_exposure = np.sum(new_prices * self.shares)
            self.gross_exposure = np.sum(new_prices * np.abs(self.shares))

            # Stats
            self.stat_perc_gain = \
                np.sum((new_prices - self.prices_entry) * self.shares) / \
                np.sum(np.abs(self.shares) * self.prices_entry)

        # Stats
        self.stat_holding_days += 1
        self.prices_current = new_prices
        return

    def update_position_exposure(self, new_gross_exposure):
        """
        Returns net exposure to zero.
        """
        long_inds = self.shares > 0
        short_inds = self.shares < 0
        new_long_size = new_gross_exposure / np.sum(long_inds) / 2.
        new_short_size = new_gross_exposure / np.sum(short_inds) / 2.
        new_shares = self.shares.copy()
        new_shares[long_inds] = new_long_size / \
            self.prices_current[long_inds]
        new_shares[short_inds] = new_short_size / \
            self.prices_current[short_inds] * -1
        trans_cost = np.sum(np.abs(self.shares - new_shares)) * self.COMM
        self.daily_pl -= trans_cost
        self.shares = new_shares
        self.net_exposure = np.sum(self.prices_current * self.shares)
        self.gross_exposure = np.sum(self.prices_current * np.abs(self.shares))
        self.stat_rebalance_count += 1
        return

    def close_position(self):
        if self.open_position:
            self.daily_pl -= np.sum(np.abs(self.shares) * self.COMM)
            self.shares[:] = 0
            self.net_exposure = 0
            self.gross_exposure = 0
            self.open_position = False
        return

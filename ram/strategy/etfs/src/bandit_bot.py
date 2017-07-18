import numpy as np
import pandas as pd

import tensorflow as tf

from tqdm import tqdm


class BanditBot:

    name = 'BaseBot'

    inflation_loss = 0.995

    def __init__(self, roi=1., bankroll=1., nt=50, p=1., q=0., r_buy=1., r_sell=1.):
        self.roi = roi # Return on Investment - used to calculate Buy And Hold strategy
        self.bankroll = bankroll # needed to determine how much we can short
        self.p = p # our principle 
        self.q = q # amount of stock
        self.p0 = p # save the initial bankroll for reset
        self.q0 = q 
        self.r_buy = r_buy # the ratio of p to spend on purchase 
        self.r_sell = r_sell # the ratio of p to liquidate on sell
        self.nt = nt # length of input vector
        self.p_margin = 0 # in development
        self.q_margin = 0

    def buy(self, x, t):
        """
        Buy at time index t at value x[t]
        """
        p_spent = self.p * self.r_buy
        q_bought = p_spent / x[t]
        self.q += q_bought
        self.p -= p_spent
        return (p_spent, q_bought)

    def sell(self, x, t):
        """
        Sell at time index t at value x[t]
        """
        q_sold = self.q * self.r_sell 
        p_earned = q_sold * x[t]
        self.q -= q_sold
        self.p += p_earned
        return (p_earned, q_sold)

    def short_position(self, x, t1, t2):
        """
        The noble short sell. "Borrow" stock in order to sell it immediately,
        then buy it back at a later date in order return the borrowed shares.
        """
        q_short = self.bankroll / x[t1] # decide the amount we want to short, since in theory can short infinite
        self.q_margin += q_short
        p_earned = q_short * x[t1]
        self.p += p_earned

        q_return = self.q_margin
        p_buyback = q_return * x[t2]
        self.q_margin -= q_return
        self.p -= p_buyback
        return (p_earned-p_buyback, q_short)

    def liquidate(self, x):
        """
        Sell all positions so we are left only with cash
        """
        q_sold = self.q
        p_earned = q_sold * x[-1]
        self.q = 0
        self.p += p_earned
        return (p_earned, q_sold)

    def score(self, x):
        """
        Liquidate all shares at current market price and then compute
        how much money we made. For the reinforcement paradigm to work well,
        we define score == 0 as neither gained nor lost any money this round
        """
        self.liquidate(x)
        return float((self.roi * self.inflation_loss * self.p) - self.p0)

    def reset(self):
        """
        Set funds back to original bankroll
        """
        self.p = self.p0
        self.q = self.q0

    def __call__(self, x, *args, **kwargs):
        """
        We will use the call protocol to represent running the bot on a
        single epoch. This will be overloaded for each subclass. Just a
        fancy little convenience.
            reward = bot()
        would be the same as: 
            reward = bot.pullBandit()
        """
        result = np.random.randn(1)
        if result > 0:
            return 1
        else:
            return -1

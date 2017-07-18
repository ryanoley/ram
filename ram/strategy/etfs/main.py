import numpy as np
from tqdm import tqdm
import tensorflow as tf
from ram.strategy.etfs.data import ETFData

from ram.strategy.etfs.src.bandit_bot import BanditBot


data = ETFData(['SPY', 'IWM'])



class StratTwin(BanditBot):

    name = 'StratTwin'

    def __init__(self, nt=50, p=1., q=0., r_buy=1., r_sell=1.):
        BanditBot.__init__(self, nt=nt, p=p, q=q, r_buy=r_buy, r_sell=r_sell)

    def __call__(self, x, *args, **kwargs):
        nt = len(x)
        t1 = nt // 3 # assess period
        if x[t1] < x[0]:
            self.short_position(x, t1+1, -1)
        else:
            self.buy(x, t1+1)
        return self.score(x) # liquidate full position




# Create our ensemble
bots = [StratTwin()]

num_bandits = len(bots)


total_episodes = 100000
mini_epoch_size = 100
print_epoch_size = 10000



tf.reset_default_graph()


# These two lines established the feed-forward part of the network. 
# This does the actual choosing.
weights = tf.Variable(tf.ones([num_bandits]))
chosen_action = tf.argmax(weights, 0)

# The next six lines establish the training proceedure. 
# We feed the reward and chosen action into the network
# to compute the loss, and use it to update the network.
reward_holder = tf.placeholder(shape=[1], dtype=tf.float32)
action_holder = tf.placeholder(shape=[1], dtype=tf.int32)

responsible_weight = tf.slice(weights, action_holder, [1])

loss = -(tf.log(responsible_weight) * reward_holder)
optimizer = tf.train.GradientDescentOptimizer(learning_rate=0.001)

update = optimizer.minimize(loss)



# My implementation. Is this correct
x_train = data.data[data.data.Ticker == 'SPY'].loc[:, 'Close'].values



sample_ratio = total_episodes / mini_epoch_size
print('Mini-epoch size: {}\nNum Mini-epochs: {}'.format(mini_epoch_size, total_episodes // mini_epoch_size))

# Set scoreboard for bandits to 0.
total_reward = np.zeros(num_bandits)

# Set the chance of taking a random action.
e = 0.2

init = tf.initialize_all_variables()
verbose_updates = False

# Launch the tensorflow graph
with tf.Session() as sess:

    sess.run(init)

    for i in tqdm(range(total_episodes)):

        print('Ep {} of {}'.format(i, total_episodes))

        tp = np.random.randint(0, len(x_train))
        # Select time period
        x = x_train[tp].ravel()

        # Choose either a random action or one from our network.
        if np.random.rand(1) < e:
            action = np.random.randint(num_bandits)
        else:
            action = sess.run(chosen_action)

#         reward = pullBandit(bandits[action]) #Get our reward from picking one of the bandits.
        reward = bots[action](x) #Get our reward from picking one of the bandits.

        # Update the network
        _, resp, ww = sess.run(
            [update, responsible_weight, weights],
            feed_dict={
                reward_holder: [reward],
                action_holder: [action]
            })

        #Update our running tally of scores.
        total_reward[action] += reward

        if i % print_epoch_size == 0 and verbose_updates:
            print('Results: ', ' '.join(['{:.3f}'.format(bot.p) for bot in bots]))
            print("Running reward: {}".format(str(total_reward)))
        if i % mini_epoch_size == 0:
            [bot.reset() for bot in bots]

winner = np.argmax(ww)

print("The agent thinks bandit {} is the most promising....".format(bots[winner].name))



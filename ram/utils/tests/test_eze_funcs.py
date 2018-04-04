import os
import unittest

from pandas.util.testing import assert_frame_equal

from ram.utils.eze_funcs import *


class TestEzeFuncs(unittest.TestCase):

    def setUp(self):
        etb_dir = os.getcwd()
        self.etb_fl1 = os.path.join(etb_dir, '010118.EZBR.txt')
        self.etb_fl2 = os.path.join(etb_dir, '020118.EZBR.txt')
        # Create EZBR Files
        with open(self.etb_fl1, 'w') as f:
            f.write("AAPL\n")
            f.write("IBM\n")
            f.write("GOOGL\n")
            f.write("SPY\n")

        with open(self.etb_fl2, 'w') as f:
            f.write("IWM\n")
            f.write("QQQ\n")
            f.write("SPY\n")

    def test_get_all_etb_files(self):
        result = get_all_etb_files(ETB_DIR = os.getcwd())
        benchmark = ['010118.EZBR.txt', '020118.EZBR.txt']
        self.assertEqual(result, benchmark)

    def test_get_max_etb_data(self):
        result = get_max_etb_data(ETB_DIR = os.getcwd())
        benchmark = pd.DataFrame(data={'Ticker':['IWM', 'QQQ', 'SPY']})
        assert_frame_equal(result, benchmark)

    def test_get_etb_status(self):
        inp_tickers = ['QQQ','VXX']
        result = etb_status(inp_tickers, ETB_DIR=os.getcwd())
        benchmark = pd.DataFrame(data={'Ticker':['QQQ','VXX'],
                                        'ETB_HTB':['ETB', 'HTB']},
                                 columns=['Ticker', 'ETB_HTB'])
        assert_frame_equal(result, benchmark)

        inp_tickers = np.array(['GOOGL','VXX'])
        result = etb_status(inp_tickers, ETB_DIR=os.getcwd())
        benchmark = pd.DataFrame(data={'Ticker':['GOOGL','VXX'],
                                        'ETB_HTB':['HTB', 'HTB']},
                                 columns=['Ticker', 'ETB_HTB'])
        assert_frame_equal(result, benchmark)

    def tearDown(self):
        os.remove(self.etb_fl1)
        os.remove(self.etb_fl2)


if __name__ == '__main__':
    unittest.main()

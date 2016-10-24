import pandas as pd

from platform.data.base import DataHandler


class FlatFileDataHandler(DataHandler):

    def __init__(self, data):
        self.master_data = data

    def get_data(self, dt):
        return self.master_data.loc[self.master_data.Date < dt]

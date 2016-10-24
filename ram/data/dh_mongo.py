import pymongo
import numpy as np
import pandas as pd
import datetime as dt

from ram.utils.time_funcs import check_input_date
from ram.data.base import DataHandler


class DataHandlerMongoDb(DataHandler):

    def __init__(self):
        client = pymongo.MongoClient(host='192.168.2.8')
        self._mongoc = client.arb_system.prices

    def get_filtered_univ_data(self,
                               univ_size,
                               features,
                               start_date,
                               filter_date,
                               end_date,
                               filter_column,
                               filter_bad_ids=False):
        """
        Purpose of this class is to provide an interface to get a filtered
        universe.

        Parameters
        ----------
        univ_size : int
        features : list
        start_date/filter_date/end_date : datetime
        filter_column : str
        filter_bad_ids : bool

        Returns
        -------
        data : pandas.DataFrame
            With columns ID, Date representing a unique observation.
        """
        # Check user input
        assert isinstance(univ_size, int)
        if not isinstance(features, list):
            features = [features]
        start_date = check_input_date(start_date)
        filter_date = check_input_date(filter_date)
        end_date = check_input_date(end_date)

        # List to aggregate over
        pipeline = []

        # DATES
        timefilter = {
            '$match': {
                'DailyDate': {
                    '$gte': start_date,
                    '$lte': end_date
                }
            }
        }
        pipeline.append(timefilter)

        # UNIVERSE FILTER - Get top value from certain date
        ids = self._get_filtered_ids(filter_date, univ_size,
                                     filter_column, filter_bad_ids)

        idfilter = {'$match': {'ID': {'$in': ids}}}
        pipeline.append(idfilter)

        # FEATURES
        project = {
            'ID': '$ID',
            'DailyDate': '$DailyDate'
        }
        for col in features:
            project.update({col: '${0}'.format(col)})
        project = {'$project': project}
        pipeline.append(project)

        # QUERY FROM COLLECTION
        cursor = self._mongoc.aggregate(pipeline)
        df = pd.DataFrame(list(cursor))

        del df['_id']

        # FORMAT AND CREATE OBJECTS
        # Remove underscores from all id columns if present
        if df.ID.iloc[0][:1] == '_':
            df['ID'] = df.ID.apply(lambda x: x[1:])
        df = df.loc[:, ['DailyDate', 'ID']+features]
        df = df.rename(columns={'DailyDate': 'Date'})
        return df

    def _get_filtered_ids(self,
                          filter_date,
                          filter_num,
                          filter_column,
                          filter_bad_ids):
        """
        Used to get top values for given column at a specific date.
        """
        pipeline = []

        # Verify filter date is available. Otherwise get available day before.
        uniq_dates = np.unique(list(self._mongoc.distinct('DailyDate')))
        filter_date = uniq_dates[uniq_dates <= filter_date][-1]

        # DATES
        filter1 = {'$match': {'DailyDate': filter_date}}
        pipeline.append(filter1)

        # ONE YEAR DATA
        if filter_bad_ids:
            matchfilter = {'$match': {'OneYearFlag': 'T',
                                      'NormalTradingFlag': 'T'}}
            pipeline.append(matchfilter)

        # FILTER COL
        filtercol1 = {'$sort': {'{0}'.format(filter_column): -1}}
        filtercol2 = {'$limit': filter_num}
        pipeline.append(filtercol1)
        pipeline.append(filtercol2)

        # PROJECT
        project = {'$project': {'ID': '$ID'}}
        pipeline.append(project)

        # QUERY FROM COLLECTION
        cursor = self._mongoc.aggregate(pipeline)
        idseries = pd.DataFrame(list(cursor))['ID'].tolist()
        return idseries

    def get_id_data(self,
                    ids,
                    features,
                    start_date,
                    end_date):
        """
        Purpose of this class is to provide an interface to get a filtered
        universe.

        Parameters
        ----------
        ids : list
        features : list
        start_date/end_date : datetime

        Returns
        -------
        data : pandas.DataFrame
            With columns ID, Date representing a unique observation.
        """
        # Check user input
        if not isinstance(ids, list):
            ids = [ids]
        if not isinstance(features, list):
            features = [features]
        start_date = check_input_date(start_date)
        end_date = check_input_date(end_date)

        pass


if __name__ == '__main__':

    bdh = DataHandlerMongoDb()

    df = bdh.get_filtered_univ_data(
        univ_size=40,
        features=['Close', 'VWAP'],
        start_date=dt.datetime(2014, 1, 1),
        filter_date=dt.datetime(2015, 1, 1),
        end_date=dt.datetime(2015, 4, 1),
        filter_column='AvgDolVolume',
        filter_bad_ids=False)

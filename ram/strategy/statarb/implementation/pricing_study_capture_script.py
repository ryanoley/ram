import os
import pandas as pd
import datetime as dt
from ram import config

dpath = os.path.join(config.BASE_DIR, 'pricing_study')


def check_directory():
    if not os.path.isdir(dpath):
        os.mkdir(dpath)


def capture_prices(name):
    file_label = dt.datetime.now().strftime('%Y%m%d') + '_' + name + '.csv'
    file_path = os.path.join(dpath, file_label)
    data = pd.DataFrame({'V1': range(10)})
    data.to_csv(file_path, index=None)


def main():
    first_pull_flag = False
    while True:
        time = dt.datetime.now().time()
        # if time > dt.time(15, 45):
        if time > dt.time(11, 38) and not first_pull_flag:
            first_pull_flag = True
            print('First pull @ {}'.format(time))
            capture_prices('345_pull')
        # if time > dt.time(16, 5):
        if time > dt.time(11, 39):
            print('Second pull @ {}'.format(time))
            capture_prices('close_pull')
            break


if __name__ == '__main__':
    check_directory()
    main()

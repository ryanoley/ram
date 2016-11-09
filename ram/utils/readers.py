import os

from gearbox import read_csv, convert_date_array


def get_all_strategy_results():
    """
    At this point reads all results files
    """
    OUTDIR = os.path.join(os.getenv('DATA'), 'ram', 'strategy_output')
    results = [nm for nm in os.listdir(OUTDIR) if nm.find('returns.csv') > 0]

    # Put all return series together
    for nm in results:

        df = read_csv(os.path.join(OUTDIR, nm))

        # Infer date column because some are labelled and others are not
        if isinstance(df.index[0], int):
            if df.iloc[0, 0].find('-') > 0:
                df.iloc[:, 0] = convert_date_array(df.iloc[:, 0])
            else:
                print 'Passing on {0}'.format(nm)
                continue

        elif isinstance(df.index[0], str):
            if df.index[0].find('-') > 0:
                df = df.reset_index()
                df.iloc[:, 0] = convert_date_array(df.iloc[:, 0])
            else:
                print 'Passing on {0}'.format(nm)
                continue

        else:
            print 'Passing on {0}'.format(nm)
            continue
        # Adjust columns
        name = nm.replace('_returns.csv', '')
        df.columns = ['Date'] + ['{0}_{1}'.format(name, i) for i
                                 in range(len(df.columns)-1)]

        if nm == results[0]:
            all_results = df
        else:
            all_results = all_results.merge(df, how='outer')

    all_results = all_results.set_index('Date')
    return all_results

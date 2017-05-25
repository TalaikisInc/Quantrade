from os.path import join, isfile
from os import listdir

from pandas import read_csv, to_datetime, DataFrame, concat
from numpy import maximum, sort, zeros_like
from matplotlib import pyplot as plt
from django.conf import settings


path_to = join(settings.METATRADER_HISTORY_PATHS[0][1], "")

filenames = [f for f in listdir(path_to) if isfile(join(path_to, f))]

for filename in filenames:
    try:
        if "DATA_MODEL" in filename:
            symbol = filename.split("_")[3]
            period = filename.split("_")[4].split(".")[0]

            if int(period) == 1440:
                df = read_csv(filepath_or_buffer=join(settings.METATRADER_HISTORY_PATHS[0][1], \
                    filename), sep=',', delimiter=None, \
                    header=0, names=None, index_col=0, usecols=None, squeeze=False, prefix=None, \
                    mangle_dupe_cols=True, dtype=None, engine=None, \
                    converters=None, true_values=None, false_values=None, \
                    skipinitialspace=False, skiprows=None, nrows=None, \
                    na_values=None, keep_default_na=True, na_filter=True, \
                    verbose=False, skip_blank_lines=True, parse_dates=False, \
                    infer_datetime_format=False, keep_date_col=False, \
                    date_parser=None, dayfirst=False, iterator=False, chunksize=None, \
                    compression='infer', thousands=None, decimal='.', lineterminator=None, \
                    quotechar='"', quoting=0, escapechar=None, comment=None, \
                    encoding=None, dialect=None, tupleize_cols=False, \
                    error_bad_lines=True, warn_bad_lines=True, skipfooter=0, \
                    skip_footer=0, doublequote=True, delim_whitespace=False, \
                    as_recarray=False, compact_ints=False, use_unsigned=False, \
                    low_memory=False, buffer_lines=None, memory_map=False, \
                    float_precision=None)

                #make index
                df.sort_index(axis=0, ascending=True, inplace=True)
                df.index = to_datetime(df.index).to_pydatetime()
                df.index.name = "DATE_TIME"

                max_y = maximum.accumulate(df.CLOSE)
                dd = df.CLOSE - max_y
                dd = sort(dd.as_matrix(), axis=0)
                probs, values, patches = plt.hist(dd, bins=50, weights=zeros_like(dd) + 1. / dd.size)

                p_list = [p for p in probs]
                v_list = [v for v in values]
                p = DataFrame(p_list)
                v = DataFrame(v_list)
                final = concat([p, v], axis=1)

                final_filename = join(settings.METATRADER_HISTORY_PATHS[0][1], "hist-{}.csv".format(symbol))
                final.dropna().to_csv(final_filename)
    except Exception as e:
        print(e)
        continue

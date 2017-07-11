"""
import time
from os.path import join

from fastparquet import ParquetFile, write
from pandas import read_csv, to_datetime

from django.conf import settings

from .models import FileData

pf = ParquetFile('myfile.parq')
df = pf.to_pandas()
df2 = pf.to_pandas(['col1', 'col2'], categories=['col1'])

def write(filename, df):
    write(filename, df, compression='GZIP')


def data_model_csv():
    filenames = FileData.objects.all()

    #do it for all files
    start_time = time.time()
    for filename in filenames:
        try:
            if "DATA_MODEL" in str(filename.filename):
                spl = str(filename.filename).split('_')
                broker = spl[2]
                symbol = spl[3]
                period = spl[4].split('.')[0]

                df = read_csv(filepath_or_buffer=join(filename.folder, filename.filename), sep=',', delimiter=None, \
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

                #make some returns
                if symbol == 'VXX':
                    d = df['CLOSE'].diff()
                    d_pct = df['CLOSE'].pct_change()
                    df['tmp'] = where(d < 20.0, d, 0.0)
                    df['tmp_pct'] = where(d_pct < 100.0, d_pct, 0.0)
                    df['DIFF'] = df['tmp']
                    df['PCT'] = df['tmp_pct']
                    del df['tmp']
                    del df['tmp_pct']
                else:
                    df['DIFF'] = df['CLOSE'].diff()
                    df['PCT'] = df['CLOSE'].pct_change()

                df['cl'] = abs(df['CLOSE'] - df['LOW'])
                df['hc'] = abs(df['HIGH'] - df['CLOSE'])

                #cleanuup
                df = df.dropna()

                filename = join(settings.DATA_PATH, "parquet", "{0}=={1}=={2}.parq".format(broker, symbol, period))
                #CHANGE TO DF_MULTI!!!!
                #df.to_pickle(path=filename)
                write(filename=filename, df=df)
                print colored.green("Done for {0} {1}.".format(symbol, period))
        except Exception as e:
            print colored.red(e)
            #filename = join(settings.DATA_PATH, "{0}=={1}=={2}.mp".format(broker, symbol, period))
            #symbol_cleaner(symbol=symbol, broker=broker)
            #file_cleaner(filename=filename)
            continue

    print("CSV read: %s seconds ---" % (time.time() - start_time))
"""

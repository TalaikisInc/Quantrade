from datetime import datetime
import time
import collections
import pprint

import ystockquote
import pandas as pd
from arctic import Arctic


def get_stock_history(ticker, start_date, end_date):
    data = ystockquote.get_historical_prices(ticker, start_date, end_date)
    df = pd.DataFrame(collections.OrderedDict(sorted(data.items()))).T
    df = df.convert_objects(convert_numeric=True)
    return df


def load_all_stock_history_NYSE():
    # Data downloaded from BBG Open Symbology:
    #
    nyse = pd.read_csv('/users/is/jblackburn/git/arctic/howtos/nyse.csv')
    stocks = [x.split('/')[0] for x in nyse['Ticker']]
    print((len(stocks), " symbols"))
    for i, stock in enumerate(stocks):
        try:
            now = datetime.now()
            data = get_stock_history('aapl', '1980-01-01', '2015-07-07')
            lib.write(stock, data)
            print(("loaded data for: ", stock, datetime.now() - now))
        except Exception as e:
            print(("Failed for ", stock, str(e)))


def read_all_data_from_lib(lib):
    start = time.time()
    rows_read = 0
    for s in lib.list_symbols():
        rows_read += len(lib.read(s).data)
    print(("Symbols: %s Rows: %s  Time: %s  Rows/s: %s" % (len(lib.list_symbols()),
                                                          rows_read,
                                                          (time.time() - start),
                                                          rows_read / (time.time() - start))))

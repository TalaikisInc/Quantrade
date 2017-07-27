from os.path import join
from itertools import combinations
from datetime import datetime, date
from asyncio import gather

from numpy import corrcoef
from pandas import concat, DataFrame, date_range
from clint.textui import colored

from django.conf import settings

from .models import Corr, Symbols
from .utils import multi_filenames, nonasy_df_multi_reader

ignored_symbols = ['AI50']


async def write_corr(symbol_a, symbol_b, corr):
    try:
        try:
            c = Corr.objects.create(symbol_a=symbol_a, symbol_b=symbol_b, value=corr)
            c.save()
        except:
            c = Corr.objects.get(symbol_a=symbol_a, symbol_b=symbol_b)
            c.value = corr
            c.save()
    except Exception as err:
        print(colored.red("At making corr: {}\n".format(err)))


async def process_corr(subset, dates):
    try:
        s1 = Symbols.objects.get(symbol=subset[0])
        s2 = Symbols.objects.get(symbol=subset[1])
        f1 = join(settings.DATA_PATH, "incoming_pickled", "{0}=={1}==1440".format(s1.broker.title, s1.symbol))
        f2 = join(settings.DATA_PATH, "incoming_pickled", "{0}=={1}==1440".format(s2.broker.title, s2.symbol))

        df1 = nonasy_df_multi_reader(filename=f1)
        df2 = nonasy_df_multi_reader(filename=f2)

        if (len(df1.index) > 0) & (len(df2.index) > 0):
            data = concat([df1.DIFF, df2.DIFF], axis=1, join_axes=[DataFrame(index=dates).index]).fillna(0.0)
            data.columns = ['A', 'B']
            data = data.loc[data['A'] != 0]
            data = data.loc[data['B'] != 0]
            corr = round(corrcoef(data['A'].as_matrix(), data['B'].as_matrix())[0,1], 4)
            await write_corr(symbol_a=s1, symbol_b=s2, corr=corr)
    except Exception as err:
        print(colored.red("At process_corr {}".format(err)))


def generate_correlations(loop):
    path_to = join(settings.DATA_PATH, "incoming_pickled")
    filenames = multi_filenames(path_to_history=path_to)

    symbols = Symbols.objects.filter().exclude(symbol__in=ignored_symbols)
    symbols_list = [symbol.symbol for symbol in symbols]
    combinated = combinations(symbols_list, 2)
    dates = date_range(end=date(datetime.now().year, datetime.now().month, \
        datetime.now().day),periods=20*252, freq='D', name='DATE_TIME', tz=None)

    loop.run_until_complete(gather(*[process_corr(subset=subset, \
        dates=dates) for subset in combinated],return_exceptions=True))
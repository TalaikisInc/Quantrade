from asyncio import set_event_loop, gather, new_event_loop
from os.path import join
from multiprocessing import Process

from django.conf import settings
from django.db import IntegrityError

from pandas import read_csv, to_datetime
from numpy import where, sum
from clint.textui import colored

from .tasks import adjustment_bureau, get_commission
from .utils import ext_drop, filename_constructor, name_deconstructor, multi_filenames, \
    multi_remove, df_multi_reader, nonasy_df_multi_reader, df_multi_writer, \
    file_cleaner
from .models import Indicators, Systems, Symbols
from _private.strategies import ExportedIndicators, ExportedSystems


async def init_calcs(df, symbol):
    try:
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
    except Exception as err:
        print(colored.red("At init_calcs {}".format(err)))
    
    return df


def nonasy_init_calcs(df, symbol):
    """
    Non-async initial calculator, used by Monte Carlo.
    """
    try:
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
    except Exception as err:
        print(colored.red("At init_calcs {}".format(err)))
    
    return df


async def symbol_cleaner(symbol, broker):
    try:
        s = Symbols.objects.filter(symbol=symbol, broker__title=broker).delete()
        #print("Removed failing symbol from db.")
    except Exception as e:
        print("At symbol cleaner {}\n".format(e))


async def make_initial_file(filename):
    try:
        info = {}
        spl = str(filename).split('_')
        info["broker"] = spl[2]
        info["symbol"] = spl[3]
        info["period"] = spl[4].split('.')[0]
        info["filename"] = filename

        df = read_csv(filepath_or_buffer=filename_constructor(info=info, folder="incoming"), sep=',', \
            delimiter=None, header=0, names=None, index_col=0, usecols=None, \
            squeeze=False, prefix=None, mangle_dupe_cols=True, dtype=None, \
            engine=None, converters=None, true_values=None, false_values=None, \
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

        df.sort_index(axis=0, ascending=True, inplace=True)
        df.index = to_datetime(df.index).to_pydatetime()
        df.index.name = "DATE_TIME"

        df = await init_calcs(df=df, symbol=info["symbol"])
        out_filename = filename_constructor(info=info, folder="incoming_pickled")
        await df_multi_writer(df=df, out_filename=out_filename)

        if settings.SHOW_DEBUG:
            print(colored.green("Pickled for {0} {1}.".format(info["symbol"], info["period"])))
    except Exception as err:
        print(colored.red("At initial {}".format(err)))
        out_filename = filename_constructor(info=info, folder="incoming_pickled")
        await symbol_cleaner(symbol=symbol, broker=broker)
        await file_cleaner(filename=out_filename)


def data_model_csv():
    path_to = join(settings.DATA_PATH, "incoming")
    filenames = multi_filenames(path_to_history=path_to, csv=True)
    cnt = len(filenames)
    batch_size = int(cnt/settings.CPUS)
    diff = cnt - (settings.CPUS * batch_size)

    def start_loop(loop, filenames):
        set_event_loop(loop)
        loop.run_until_complete(gather(*[make_initial_file(filename=filename) \
            for filename in filenames], return_exceptions=True))

    processes = []
    for cpu in range(settings.CPUS):
        if (cpu+1) == settings.CPUS:
            p = Process(target=start_loop, args=(new_event_loop(), filenames[cpu*batch_size:(cpu+1)*batch_size+diff]))
        else:
            p = Process(target=start_loop, args=(new_event_loop(), filenames[cpu*batch_size:(cpu+1)*batch_size]))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()


class IndicatorBase(ExportedIndicators):
    def __init__(self, name, description, per, filenames, mc=False):
        self.name = name
        self.description = description
        self.per = per
        self.mc = mc
        if self.mc:
            self.path_to_history = join(settings.DATA_PATH, "monte_carlo")
        else:
            self.path_to_history = join(settings.DATA_PATH, "incoming_pickled")
        self.filenames = filenames

    async def create_indicator(self):
        try:
            indicator = Indicators.objects.create(title=self.name)
            indicator.save()
            print("Created indicator.")
        except IntegrityError:
            pass
        except Exception as err:
            print(colored.red("At create_indicator: {0}".format(err)))
            if settings.UPDATE_DESCRIPTIONS:
                indicator = Indicators.objects.get(title=self.name)
                indicator.description=self.description
                indicator.save()

    async def starter(self):
        await self.create_indicator()

        for filename in self.filenames:
            try:
                self.info = name_deconstructor(filename=filename, t="", mc=self.mc)
                self.info["indicator"] = self.name

                file_name = join(self.path_to_history, self.info["filename"])

                df = nonasy_df_multi_reader(filename=file_name)

            except Exception as err:
                print(colored.red("IndicatorBase for filename {}".format(err)))
                df = None

            await self.indicators(df=df, file_name=file_name)

    async def insert(self, df, file_name):
        try:
            df = df.dropna()

            if self.mc:
                out_filename = filename_constructor(info=self.info, folder="indicators", mc=self.mc)
            else:
                out_filename = filename_constructor(info=self.info, folder="indicators")

            await df_multi_writer(df=df, out_filename=out_filename)

            if settings.SHOW_DEBUG:
                print(colored.green("Saved indicator data {} to pickle.".format(out_filename)))

        except Exception as err:
            print(colored.red("IndicatorBase insert {}".format(err)))
            file_name = filename_constructor(info=info, folder="indicators")
            await file_cleaner(filename=file_name)


async def get_margin(broker: str, symbol: str) -> float:
    """
    Get margin for a symbol.
    """
    try:
        margin = float(Symbols.objects.get(symbol=symbol).margin_initial)

        return margin

    except Exception as err:
        if settings.SHOW_DEBUG:
            print(colored.red("get_margin {}".format(err)))


async def long_short(system, commission, margin, df):
    try:
        if system is 'HOLD':
            df['LONG_PL'] = where(df['BUY_SIDE'] == 1, df['DIFF'], 0.0)
            df['SHORT_PL'] = where(df['SELL_SIDE'] == -1, -df['DIFF'], 0.0)

            df['LONG_TRADES'] = 0
            df['SHORT_TRADES'] = 0
        else:
            df['commissions_long'] = where((df['BUY_SIDE'].shift() == 1) & (df['BUY_SIDE'].shift(2) == 0), commission*2.0, 0)
            df['commissions_short'] = where((df['SELL_SIDE'].shift() == -1) & (df['SELL_SIDE'].shift(2) == 0), commission*2.0, 0)

            df['LONG_TRADES'] = where((df['BUY_SIDE'].shift() == 1) & (df['BUY_SIDE'].shift(2) == 0), 1, 0)
            df['SHORT_TRADES'] = where((df['SELL_SIDE'].shift() == -1) & (df['SELL_SIDE'].shift(2) == 0), 1, 0)

            df['LONG_PL'] = where(df['BUY_SIDE'].shift() == 1, df['DIFF'] - df['commissions_long'], 0)
            df['SHORT_PL'] = where(df['SELL_SIDE'].shift() == -1, -df['DIFF'] - df['commissions_long'], 0)

            df['LONG_MARGIN'] = where(df['BUY_SIDE'].shift() == 1, margin, 0.0)
            df['SHORT_MARGIN'] = where(df['SELL_SIDE'].shift() == -1, margin, 0.0)

            del df['commissions_long']
            del df['commissions_short']
    except Exception as err:
        print(colored.red("long_short {}".format(err)))

    return df


async def maes(system, df):
    try:
        if system is 'HOLD':
            df['LONG_MAE'] = df['LONG_PL_CUMSUM'] - df['MAE']
            df['LONG_MFE'] = df['LONG_PL_CUMSUM'] + df['MFE']
            df['SHORT_MAE'] = df['SHORT_PL_CUMSUM'] - df['MFE']
            df['SHORT_MFE'] = df['SHORT_PL_CUMSUM'] + df['MAE']
        else:
            df['mae_tmp_long'] = where(df['BUY_SIDE'].shift() == 1, df['MAE'], 0.0)
            df['mfe_tmp_long'] = where(df['BUY_SIDE'].shift() == 1, df['MFE'], 0.0)
            df['mae_tmp_short'] = where(df['SELL_SIDE'].shift() == -1, df['MFE'], 0.0)
            df['mfe_tmp_short'] = where(df['SELL_SIDE'].shift() == -1, df['MAE'], 0.0)

            df['LONG_MAE'] = df['LONG_PL_CUMSUM'] - df['mae_tmp_long']
            df['LONG_MFE'] = df['LONG_PL_CUMSUM'] + df['mfe_tmp_long']
            df['SHORT_MAE'] = df['SHORT_PL_CUMSUM'] - df['mfe_tmp_short']
            df['SHORT_MFE'] = df['SHORT_PL_CUMSUM'] + df['mae_tmp_short']
    except Exception as err:
        print(colored.red("maes {}".format(err)))

    return df


async def clean(info: dict, mc: bool=False) -> None:
    try:
        if not mc:
            folders = ['performance', 'systems']
            for folder in folders:
                out_filename = filename_constructor(info=info, folder=folder)
                await file_cleaner(filename=out_filename)
    except Exception as err:
        if settings.SHOW_DEBUG:
            print(colored.red("clean {}".format(err)))


async def perf_point(filename, path_to, mc=False):
    try:
        info = name_deconstructor(filename=filename, t="s", mc=mc)
        
        if settings.SHOW_DEBUG:
            print("Working with {0} {1} {2}".format(info["symbol"], info["period"], info["system"]))

        margin = await get_margin(broker=info["broker"], symbol=info["symbol"])
        commission = await get_commission(symb=info["symbol"], broker=info["broker"])

        if (not commission is None) & (not margin is None):
            if commission > 0:
                df = await df_multi_reader(filename=join(path_to, info["filename"]))

                if len(df.index) > settings.MIN_TRADES:
                    df.index = to_datetime(df.index).to_pydatetime()

                    df['dif'] = await adjustment_bureau(data=df['DIFF'], \
                        symbol_name=info["symbol"], broker_name=info["broker"], \
                        period_name=info["period"])
                    
                    if not df['dif'] is None:
                        del df['DIFF']

                    df.rename(columns={'dif': 'DIFF'}, inplace=True)

                    df = await long_short(system=info["system"], commission=commission, margin=margin, df=df)

                    df['LONG_PL_CUMSUM'] = df['LONG_PL'].cumsum()
                    df['SHORT_PL_CUMSUM'] = df['SHORT_PL'].cumsum()

                    df['mae'] = await adjustment_bureau(data=df['cl'], symbol_name=info["symbol"], \
                        broker_name=info["broker"], period_name=info["period"])
                    df['mfe'] = await adjustment_bureau(data=df['hc'], symbol_name=info["symbol"], \
                        broker_name=info["broker"], period_name=info["period"])

                    df.rename(columns={'mae': 'MAE', 'mfe': 'MFE'}, inplace=True)
                    del df['cl']
                    del df['hc']

                    df = await maes(system=info["system"], df=df)

                    df['LONG_DIFF_CUMSUM'] = df['DIFF'].cumsum()
                    df['SHORT_DIFF_CUMSUM'] = -df['DIFF'].cumsum()

                    df = df.dropna()

                    del df['mae_tmp_long']
                    del df['mfe_tmp_long']
                    del df['mae_tmp_short']
                    del df['mfe_tmp_short']

                    if mc:
                        out_filename = filename_constructor(info=info, folder="performance", mc=mc)
                    else:
                        out_filename = filename_constructor(info=info, folder="performance")

                    await df_multi_writer(df=df, out_filename=out_filename)
                        
                    if settings.SHOW_DEBUG:
                        print(colored.green("Saved performance {} to pickle.".format(iinfo["filename"])))
                    if mc:
                        await multi_remove(filename=join(path_to, info["filename"]))
                else:
                    await clean(info=info, mc=mc)
            else:
                await clean(info=info, mc=mc)
        else:
            await clean(info=info, mc=mc)
    except Exception as err:
        print(colored.red("At generating performance {}".format(err)))


def generate_performance(loop, filenames, mc=False):
    """
    Generates performance numbers from systems.
    """
    if mc:
        path_to = join(settings.DATA_PATH, "monte_carlo", "systems")
    else:
        path_to = join(settings.DATA_PATH, "systems")

    loop.run_until_complete(gather(*[perf_point(filename=filename, path_to=path_to, mc=mc) \
        for filename in filenames if 'AI50' not in filename], return_exceptions=True
    ))


class SignalBase(ExportedSystems):
    def __init__(self, name, description, indicator, mean_rev, buy_threshold, sell_threshold, filenames, mc=False):
        self.name = name
        self.indicator = Indicators.objects.get(title=indicator)
        self.description = description
        self.mean_rev = mean_rev
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.mc = mc
        if self.mc:
            self.path_to_history = join(settings.DATA_PATH, "monte_carlo", "indicators")
        else:
            self.path_to_history = join(settings.DATA_PATH, "indicators")
        self.filenames = filenames

    async def create_system(self):
        try:
            system = Systems.objects.create(title=self.name, indicator=self.indicator)
            system.save()
            print("Created system.")
        except IntegrityError:
            pass
        except Exception as err:
            print(colored.red("At create_system(): {0}".format(err)))
            if settings.UPDATE_DESCRIPTIONS:
                system = Systems.objects.get(title=self.name)
                system.description=self.description
                system.indicator=self.indicator
                system.save()

    async def starter(self):
        await self.create_system()

        for filename in self.filenames:
            self.info = name_deconstructor(filename=filename, t="", mc=self.mc)
            self.info["indicator"] = self.indicator
            self.info["system"] = self.name

            file_name = filename_constructor(info=self.info, folder="indicators", mc=self.mc)
            df = nonasy_df_multi_reader(filename=file_name)
            await self.signals(df=df, file_name=file_name)

    async def insert(self, df, file_name):
        try:
            df = df.dropna()

            if self.mc:
                out_filename = filename_constructor(info=self.info, folder="systems", mc=self.mc)
            else:
                out_filename = filename_constructor(info=self.info, folder="systems")
            
            await df_multi_writer(df=df, out_filename=out_filename)
            if not self.mc:
                json_filename = filename_constructor(info=self.info, folder="json")
                df.to_json(json_filename, orient="table")
            else:
                await multi_remove(filename=file_name)

            if settings.SHOW_DEBUG:
                print(colored.green("Saved system signals {} to pickle.".format(out_filename)))
        except Exception as err:
            print(colored.red("SystemBase insert  {}".format(err)))
            filename = filename_constructor(info=self.info, folder="systems")
            await file_cleaner(filename=filename)

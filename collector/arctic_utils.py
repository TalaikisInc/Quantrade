import asyncio
import time, datetime
from os import listdir, remove
from os.path import isfile, join
from functools import lru_cache

from django.conf import settings
from django.db import IntegrityError

from pandas import read_csv, to_datetime, concat, DataFrame, date_range
from numpy import where, sum
from clint.textui import colored

from .tasks import adjustment_bureau, get_commission, clean_folder, \
    error_email, file_cleaner, df_multi_reader, df_multi_writer, \
    multi_filenames, ext_drop, hdfone_filenames, multi_remove
from .models import Indicators, Systems, Symbols
from .strategies import ExportedIndicators, ExportedSystems


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


async def symbol_cleaner(symbol, broker):
    try:
        s = Symbols.objects.filter(symbol=symbol, broker__title=broker).delete()
        #print("Removed failing symbol from db.")
    except Exception as e:
        print("At symbol cleaner {}\n".format(e))


async def make_initial_file(path_to, filename):
    try:
        spl = str(filename).split('_')
        broker = spl[2]
        symbol = spl[3]
        period = spl[4].split('.')[0]

        df = read_csv(filepath_or_buffer=join(path_to, filename), sep=',', \
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

        #make index
        df.sort_index(axis=0, ascending=True, inplace=True)
        df.index = to_datetime(df.index).to_pydatetime()
        df.index.name = "DATE_TIME"

        #make some returns
        df = await init_calcs(df=df, symbol=symbol)

        out_filename = join(settings.DATA_PATH, "incoming_pickled", "{0}=={1}=={2}".format(broker, symbol, period))
        await df_multi_writer(df=df, out_filename=out_filename)

        if settings.SHOW_DEBUG:
            print(colored.green("Pickled for {0} {1}.".format(symbol, period)))
    except Exception as err:
        print(colored.red("At initial {}".format(e)))
        out_filename = join(settings.DATA_PATH, "incoming_pickled", "{0}=={1}=={2}.mp".format(broker, symbol, period))
        #await symbol_cleaner(symbol=symbol, broker=broker)
        await file_cleaner(filename=out_filename)


def data_model_csv(loop):
    path_to = join(settings.DATA_PATH, 'incoming')
    filenames = multi_filenames(path_to_history=path_to, csv=True)

    loop.run_until_complete(asyncio.gather(*[make_initial_file(path_to=path_to, \
        filename=filename) for filename in filenames], return_exceptions=True
    ))


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
        self.excluded_symbols = ['M1']
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
                if settings.DATA_TYPE != "hdfone":
                    filename = await ext_drop(filename=filename)

                spl = filename.split('==')
                self.broker = spl[0]
                self.symbol = spl[1]
                if self.mc:
                    self.period = spl[2]
                    self.path = spl[3]
                else:
                    self.period = spl[2]

                file_name = join(self.path_to_history, filename)

                df = await df_multi_reader(filename=file_name)

            except Exception as err:
                print(colored.red("IndicatorBase for filename {}".format(err)))
                df = None

            await self.indicators(df=df, file_name=file_name)

    async def insert(self, df, file_name):
        try:
            df = df.dropna()

            if self.mc:
                out_filename = join(settings.DATA_PATH, "monte_carlo", "indicators", \
                    "{0}=={1}=={2}=={3}=={4}".format(self.broker, self.symbol, self.period, self.name, self.path))
            else:
                out_filename = join(settings.DATA_PATH, "indicators", \
                    "{0}=={1}=={2}=={3}".format(self.broker, self.symbol, self.period, self.name))

            await df_multi_writer(df=df, out_filename=out_filename)

            if settings.SHOW_DEBUG:
                print(colored.green("Saved indicator data {} to pickle.".format(out_filename)))
            
            if self.mc:
                await multi_remove(filename=file_name)
        except Exception as err:
            print(colored.red("IndicatorBase insert {}".format(err)))
            filename = join(settings.DATA_PATH, 'indicators', "{0}=={1}=={2}=={3}.mp".format(self.broker, self.symbol, self.period, self.name))
            await file_cleaner(filename=filename)

    async def clean_df(self, df):
        try:
            if not self.mc:
                del df['HIGH']
                del df['LOW']
                del df['OPEN']
                del df['VOLUME']
        except Exception as err:
            if settings.SHOW_DEBUG:
                print(colored.red("IndicatorBase clean {}".format(err)))
        return df


#TODO this also requires special case for sending and svinh signals if this would appear on systems pg
#TODO probably just needs a separate class to handle *portfoloios* of symbols
async def special_case(path_to, period):
    syms = ['M1']
    broker = 'Ava Trade EU Ltd.'

    for symbol in syms:
        try:
            #symbol equal system
            df = await df_multi_reader(filename=join(path_to, '{0}=={1}=={2}=={3}.mp'.format(broker, symbol, period, symbol)))

            bond_commission = await get_commission('30YTBOND')
            eu_commission = await get_commission('DJEUR50')
            sp_commission = await get_commission('SP500')

            bond_margin = float(Symbols.objects.get(broker__title=broker, symbol='30YTBOND').margin_initial)
            eu_margin = float(Symbols.objects.get(broker__title=broker, symbol='DJEUR50').margin_initial)
            sp_margin = float(Symbols.objects.get(broker__title=broker, symbol='SP500').margin_initial)

            df['bond_commissions_long'] = where((df['BOND_SIDE'].shift() == 1) & (df['BOND_SIDE'].shift(2) == 0), bond_commission*2.0, 0)
            df['eu_commissions_long'] = where((df['EU_SIDE'].shift() == 1) & (df['EU_SIDE'].shift(2) == 0), eu_commission*2.0, 0)
            df['sp_commissions_long'] = where((df['SP_SIDE'].shift() == 1) & (df['SP_SIDE'].shift(2) == 0), sp_commission*2.0, 0)

            df['BOND_LONG_TRADES'] = where((df['BOND_SIDE'].shift() == 1) & (df['BOND_SIDE'].shift(2) == 0), 1, 0)
            df['EU_LONG_TRADES'] = where((df['EU_SIDE'].shift() == 1) & (df['EU_SIDE'].shift(2) == 0), 1, 0)
            df['SP_LONG_TRADES'] = where((df['SP_SIDE'].shift() == 1) & (df['SP_SIDE'].shift(2) == 0), 1, 0)

            df['BOND_LONG_PL'] = where(df['BOND_SIDE'].shift() == 1, df['BOND'] - df['bond_commissions_long'], 0)
            df['EU_LONG_PL'] = where(df['EU_SIDE'].shift() == 1, df['EU'] - df['eu_commissions_long'], 0)
            df['SP_LONG_PL'] = where(df['SP_SIDE'].shift() == 1, df['SP'] - df['sp_commissions_long'], 0)

            df['BOND_LONG_MARGIN'] = where(df['BOND_SIDE'].shift() == 1, bond_margin, 0.0)
            df['EU_LONG_MARGIN'] = where(df['EU_SIDE'].shift() == 1, eu_margin, 0.0)
            df['SP_LONG_MARGIN'] = where(df['SP_SIDE'].shift() == 1, sp_margin, 0.0)

            del df['bond_commissions_long']
            del df['eu_commissions_long']
            del df['sp_commissions_long']

            df['BOND_LONG_PL_CUMSUM'] = df['BOND_LONG_PL'].cumsum()
            df['EU_LONG_PL_CUMSUM'] = df['EU_LONG_PL'].cumsum()
            df['SP_LONG_PL_CUMSUM'] = df['SP_LONG_PL'].cumsum()

            df['bond_mae'] = await adjustment_bureau(data=df['BOND_CL'], symbol_name='30YTBOND', broker_name=broker, period_name=period)
            df['eu_mae'] = await adjustment_bureau(data=df['EU_CL'], symbol_name='DJEUR50', broker_name=broker, period_name=period)
            df['sp_mae'] = await adjustment_bureau(data=df['SP_CL'], symbol_name='SP500', broker_name=broker, period_name=period)

            df['bond_mfe'] = await adjustment_bureau(data=df['BOND_HC'], symbol_name='30YTBOND', broker_name=broker, period_name=period)
            df['eu_mfe'] = await adjustment_bureau(data=df['EU_HC'], symbol_name='DJEUR50', broker_name=broker, period_name=period)
            df['sp_mfe'] = await adjustment_bureau(data=df['SP_HC'], symbol_name='SP500', broker_name=broker, period_name=period)

            df.rename(columns={
                    'bond_mae': 'MAE',
                    'bond_mfe': 'MFE',
                    'eu_mae': 'MAE',
                    'eu_mfe': 'MFE',
                    'sp_mae': 'MAE',
                    'sp_mfe': 'MFE',
                    'BOND': 'DIFF',
                    'EU': 'DIFF',
                    'SP': 'DIFF',
                    'BOND_LONG_MARGIN': 'LONG_MARGIN',
                    'EU_LONG_MARGIN': 'LONG_MARGIN',
                    'SP_LONG_MARGIN': 'LONG_MARGIN',
                    'BOND_LONG_PL_CUMSUM': 'LONG_PL_CUMSUM',
                    'EU_LONG_PL_CUMSUM': 'LONG_PL_CUMSUM',
                    'SP_LONG_PL_CUMSUM': 'LONG_PL_CUMSUM',
                    'BOND_LONG_PL': 'LONG_PL',
                    'EU_LONG_PL': 'LONG_PL',
                    'SP_LONG_PL': 'LONG_PL',
                    'BOND_SIDE': 'LONG_SIDE',
                    'EU_SIDE': 'LONG_SIDE',
                    'SP_SIDE': 'LONG_SIDE',
                    'BOND_LONG_TRADES': 'LONG_TRADES',
                    'EU_LONG_TRADES': 'LONG_TRADES',
                    'SP_LONG_TRADES': 'LONG_TRADES',
                }, inplace=True)

            del df['BOND_CL']
            del df['BOND_HC']
            del df['EU_CL']
            del df['EU_HC']
            del df['SP_CL']
            del df['SP_HC']

            #sum portfolio trades
            tmp = sum(df.LONG_MARGIN, axis=1)
            del df['LONG_MARGIN']
            df['LONG_MARGIN'] = tmp
            tmp = sum(df.LONG_PL, axis=1)
            del df['LONG_PL']
            df['LONG_PL'] = tmp
            tmp = sum(df.LONG_PL_CUMSUM, axis=1)
            del df['LONG_PL_CUMSUM']
            df['LONG_PL_CUMSUM'] = tmp
            tmp = sum(df.LONG_TRADES, axis=1)
            del df['LONG_TRADES']
            df['LONG_TRADES'] = tmp
            tmp = sum(df.MAE, axis=1)
            del df['MAE']
            df['MAE'] = tmp
            tmp = sum(df.MFE, axis=1)
            del df['MFE']
            df['MFE'] = tmp
            tmp = sum(df.LONG_SIDE, axis=1)
            del df['LONG_SIDE']
            df['LONG_SIDE'] = tmp
            tmp = sum(df.DIFF, axis=1)
            del df['DIFF']
            df['DIFF'] = tmp

            df['mae_tmp_long'] = where(df['LONG_SIDE'].shift() == 1, df['MAE'], 0.0)
            df['mfe_tmp_long'] = where(df['LONG_SIDE'].shift() == 1, df['MFE'], 0.0)

            df['LONG_MAE'] = df['LONG_PL_CUMSUM'] - df['mae_tmp_long']
            df['LONG_MFE'] = df['LONG_PL_CUMSUM'] + df['mfe_tmp_long']

            df['LONG_DIFF_CUMSUM'] = df['DIFF'].cumsum()

            df = df.dropna()

            del df['mae_tmp_long']
            del df['mfe_tmp_long']

            out_filename = join(settings.DATA_PATH, 'performance', \
                "{0}=={1}=={2}=={3}".format(broker, symbol, period, symbol))

            await df_multi_writer(df=df, out_filename=out_filename)
            
            if settings.SHOW_DEBUG:
                print("Saved performance {} to pickle.".format(filename))
        except Exception as e:
            print("At special case performance {}".format(e))


async def get_margin(broker, symbol):
    """
    Get margin for a symbol.
    """
    try:
        margin = float(Symbols.objects.get(broker__title=broker, \
            symbol=symbol).margin_initial)
    except:
        margin = None
    
    return margin


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


async def clean(broker: str, symbol: str, period: str, system: str, mc: bool) -> None:
    try:
        if not mc:
            folders = ['performance', 'systems']
            for folder in folders:
                out_filename = join(settings.DATA_PATH, folder, "{0}=={1}=={2}=={3}.mp".format(broker, symbol, period, system))
                await file_cleaner(filename=out_filename)
    except Exception as err:
        if settings.SHOW_DEBUG:
            print(colored.red("Ar perf point {}".format(err)))


async def perf_point(filename, path_to, mc):
    try:
        if settings.DATA_TYPE != "hdfone":
            filename = await ext_drop(filename=filename)

        if settings.SHOW_DEBUG:
            print("Working with {}".format(filename))
        spl = filename.split('==')
        broker = spl[0]
        symbol = spl[1]
        if settings.SHOW_DEBUG:
            print("Processing {}".format(symbol))
        period = spl[2]
        system = spl[3]
        if mc:
            path = spl[4]
        
        if settings.SHOW_DEBUG:
            print("Working with {0} {1} {2}".format(symbol, period, system))

        margin = await get_margin(broker=broker, symbol=symbol)
        commission = await get_commission(symb=symbol)

        if (not commission is None) & (not margin is None):
            if commission > 0:
                df = await df_multi_reader(filename=join(path_to, filename))

                if len(df.index) > settings.MIN_TRADES:

                    df.index = to_datetime(df.index).to_pydatetime()

                    df['dif'] = await adjustment_bureau(data=df['DIFF'], \
                        symbol_name=symbol, broker_name=broker, period_name=period)
                    
                    if not (df['dif'] is None):
                        del df['DIFF']

                    df.rename(columns={'dif': 'DIFF'}, inplace=True)

                    df = await long_short(system=system, commission=commission, margin=margin, df=df)

                    df['LONG_PL_CUMSUM'] = df['LONG_PL'].cumsum()
                    df['SHORT_PL_CUMSUM'] = df['SHORT_PL'].cumsum()

                    df['mae'] = await adjustment_bureau(data=df['cl'], symbol_name=symbol, \
                        broker_name=broker, period_name=period)
                    df['mfe'] = await adjustment_bureau(data=df['hc'], symbol_name=symbol, \
                        broker_name=broker, period_name=period)

                    df.rename(columns={'mae': 'MAE', 'mfe': 'MFE'}, inplace=True)
                    del df['cl']
                    del df['hc']

                    df = await maes(system=system, df=df)

                    df['LONG_DIFF_CUMSUM'] = df['DIFF'].cumsum()
                    df['SHORT_DIFF_CUMSUM'] = -df['DIFF'].cumsum()

                    df = df.dropna()

                    del df['mae_tmp_long']
                    del df['mfe_tmp_long']
                    del df['mae_tmp_short']
                    del df['mfe_tmp_short']

                    if mc:
                        out_filename = join(settings.DATA_PATH, 'monte_carlo', 'performance', \
                            "{0}=={1}=={2}=={3}=={4}".format(broker, symbol, period, system, path))
                    else:
                        out_filename = join(settings.DATA_PATH, 'performance', "{0}=={1}=={2}=={3}".format(\
                            broker, symbol, period, system))

                    await df_multi_writer(df=df, out_filename=out_filename)
                        
                    if settings.SHOW_DEBUG:
                        print(colored.green("Saved performance {} to pickle.".format(filename)))
                    if mc:
                        await multi_remove(filename=join(path_to, filename))
                else:
                    await clean(broker=broker, symbol=symbol, period=period, system=system, mc=mc)
            else:
                await clean(broker=broker, symbol=symbol, period=period, system=system, mc=mc)
        else:
            await clean(broker=broker, symbol=symbol, period=period, system=system, mc=mc)
    except Exception as err:
        print(colored.red("At generating performance {0} with {1}".format(err, filename)))


def generate_performance(loop, filenames, mc=False, batch=0, batch_size=100):
    """
    Doesn't implement hdfone.
    """
    if mc:
        filenames = filenames[batch*batch_size:(batch+1)*batch_size-1]

    loop.run_until_complete(asyncio.gather(*[perf_point(filename=filename, path_to=path_to, mc=mc) \
        for filename in filenames if 'M1' not in filename], return_exceptions=True
    ))

    #special cases
    #no longer used
    #periods = ['1440', '10080', '43200']
    #loop.run_until_complete(asyncio.gather(*[special_case(path_to=path_to, period=period) \
        #for period in periods], return_exceptions=True
    #))


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
        self.excluded_symbols = ['M1']
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

    async def clean_df(self, df):
        try:
            del df['VALUE']
        except Exception as err:
            #if settings.SHOW_DEBUG:
            print(colored.red("Signal clean_df {}".format(err)))
        return df

    async def starter(self):
        await self.create_system()

        for filename in self.filenames:
            if settings.DATA_TYPE != "hdfone":
                filename = await ext_drop(filename=filename)

            spl = filename.split('==')

            self.broker = spl[0]
            self.symbol = spl[1]
            self.period = spl[2]
            #self.system = self.name or spl[3]

            if self.mc:
                self.path = spl[4]

            file_name = join(self.path_to_history, filename)

            df = await df_multi_reader(filename=file_name)

            await self.signals(df=df, file_name=file_name)

    async def insert(self, df, file_name):
        try:
            df = df.dropna()

            if self.mc:
                out_filename = join(settings.DATA_PATH, "monte_carlo", "systems", "{0}=={1}=={2}=={3}=={4}".\
                    format(self.broker, self.symbol, self.period, self.name, self.path))
            else:
                out_filename = join(settings.DATA_PATH, "systems", "{0}=={1}=={2}=={3}".format(self.broker, \
                    self.symbol, self.period, self.name))
            
            await df_multi_writer(df=df, out_filename=out_filename)
            json_filename = join(settings.DATA_PATH, "systems", "json", "{0}=={1}=={2}=={3}.json".format(self.broker, \
                    self.symbol, self.period, self.name))
            df.to_json(json_filename, orient="table")

            if self.mc:
                await multi_remove(filename=file_name)

            if settings.SHOW_DEBUG:
                print(colored.green("Saved system signals {} to pickle.".format(out_filename)))
        except Exception as err:
            print(colored.red("SystemBase insert  {}".format(err)))
            filename = join(settings.DATA_PATH, 'systems', "{0}=={1}=={2}=={3}.mp".format(self.broker, self.symbol, self.period, self.name) )
            await file_cleaner(filename=filename)

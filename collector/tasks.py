from string import ascii_uppercase, digits, ascii_lowercase
from random import choice
from os import stat, remove
from os.path import join
from math import sqrt
from datetime import datetime
from asyncio import set_event_loop, gather, new_event_loop, coroutine
from functools import lru_cache
from multiprocessing import Process

import quandl
from pandas import to_datetime, read_csv
from clint.textui import colored
from numpy import power

from django.db import IntegrityError
from django.core.mail import send_mail
from django.conf import settings

from . import easywebdav
from .mysql_utils import create_symbol, mysql_connect_db #, _signals_to_mysql
from .models import Symbols, Brokers, Periods, Stats, Systems, QtraUser, \
    Signals, Indicator
from .utils import ext_drop, filename_constructor, name_deconstructor, multi_filenames, \
    multi_remove, df_multi_reader, nonasy_df_multi_reader, df_multi_writer, \
    nonasy_df_multi_writer, hdfone_filenames, file_cleaner

ignored_symbols = ['AI50']


async def fitness_rank(average_trade, trades, gross_profit, gross_loss):
    try:
        if trades > 0:
            if gross_loss < 0:
                if gross_profit > 0:
                    if average_trade != 0:
                        rank_ = (float(average_trade) * (1.0 - \
                            1.0/sqrt(float(trades)))*float(\
                            gross_profit)/abs(float(gross_loss)))
                    else:
                        rank_ = 0.0
                else:
                    rank_ = 0.0
            else:
                rank_ = (float(average_trade) * (1.0 - 1.0/sqrt(float(trades)))*float(gross_profit))
        else:
            rank_ = 0.0
    except Exception as e:
        print("At fitness rank {}".format(e))
        rank_ = 0.0

    return rank_ / 1000.0


def get_portf_d(df_cumsum, df_mae, df_trade, list_mae, list_returns, margin, fr, list_margin, list_portfolio):
    list_mae.append(df_mae)
    list_returns.append(df_trade)
    list_margin.append(margin)
    list_portfolio.append(df_cumsum)
    fr += fr

    return (list_portfolio, list_mae, list_returns, list_margin, fr)


@lru_cache(maxsize=1024*10)
def get_strategies(broker):
    if broker:
        strategies = Stats.objects.select_related('symbol', 'period', 'broker', \
            'system').filter(sharpe__gt=settings.MIN_MACHINE_SHARPE, trades__gt=40, \
            win_rate__gt=settings.MIN_MACHINE_WIN_RATE, broker=broker \
            ).exclude(avg_trade__isnull=True, trades__isnull=True, \
            symbol__commission__isnull=True).order_by('sharpe').reverse().values('symbol__symbol', \
            'symbol__currency', 'direction', 'period__name', \
            'period__period', 'period', 'system', 'system__title', 'broker__slug', \
            'sharpe', 'avg_trade', 'avg_win', 'avg_loss', 'win_rate', \
            'trades', 'fitness', 'symbol__description', 'symbol__commission', \
            'max_dd', 'intraday_dd', 'symbol__margin_initial', 'symbol', \
            'symbol__broker__title', 'broker', 'symbol__symbol')
    else:
        strategies = Stats.objects.select_related('symbol', 'period', 'broker', \
            'system').filter(sharpe__gt=settings.MIN_MACHINE_SHARPE, trades__gt=40, \
            win_rate__gt=settings.MIN_MACHINE_WIN_RATE \
            ).exclude(avg_trade__isnull=True, trades__isnull=True, \
            symbol__commission__isnull=True).order_by('sharpe').reverse().values('symbol__symbol', \
            'symbol__currency', 'direction', 'period__name', \
            'period__period', 'period', 'system', 'system__title', 'broker__slug', \
            'sharpe', 'avg_trade', 'avg_win', 'avg_loss', 'win_rate', \
            'trades', 'fitness', 'symbol__description', 'symbol__commission', \
            'max_dd', 'intraday_dd', 'symbol__margin_initial', 'symbol', \
            'symbol__broker__title', 'broker', 'symbol__symbol')
    return strategies


async def save_quandl_file(sym, quandl_periods):
    try:
        symbol = sym.split('/')[1]
        for p in quandl_periods:
            if not (p[0] is None):
                data = quandl.get(sym, collapse=p[0])
            else:
                data = quandl.get(sym)
            out_filename = join(settings.DATA_PATH, "quandl", "{0}=={1}".format(symbol, p[1]))
            await df_multi_writer(df=data, out_filename=out_filename)
    except Exception as err:
        print(colored.red("save_quandl_file {}".format(err)))


def quandl_process(loop):
    quandl.ApiConfig.api_key = settings.QUANDL_API_KEY
    quandl_symbols = ["YAHOO/INDEX_VIX", "CBOE/VXV"]
    quandl_periods = [("monthly", 43200), (None, 1440), ("weekly", 10080)]

    loop.run_until_complete(gather(*[save_quandl_file(\
        sym=sym, quandl_periods=quandl_periods) for sym in quandl_symbols], \
        return_exceptions=True
    ))


#not used anywhere
async def convert_to_csv(path_to, filename):
    try:
        filename = ext_drop(filename=filename)
        out = join(path_to, 'csv', filename+'.csv')
        filename = join(path_to, filename)
        df = await df_multi_reader(filename=filename)
        df.to_csv(out)
        print("CSVed into {}\n".format(out))
    except Exception as err:
        print(err)


def pickle_to_svc(folder, loop):
    path_to = join(settings.DATA_PATH, folder)

    filenames = multi_filenames(path_to_history=path_to)

    loop.run_until_complete(gather(*[convert_to_csv(path_to=path_to, \
        filename=filename) for filename in filenames], \
        return_exceptions=True))


async def expand(records):
    d = [dict(r.items()) for r in records]
    return d


@coroutine
def gather_bad_file(filename, path_to, list_failing):
    try:
        if settings.SHOW_DEBUG:
            print("Checking {}\n".format(filename))

        y = datetime.now().year
        m = datetime.now().month
        d = datetime.now().day
        h = datetime.now().hour

        dow = datetime.now().weekday()
        if '1440' in filename:
            df = read_csv(filepath_or_buffer=join(path_to, filename), sep=',', delimiter=None, \
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

            df.sort_index(axis=0, ascending=True, inplace=True)
            df['ts'] = df.index
            df['ts'] = to_datetime(df['ts'])
            df.index.name = "DATE_TIME"

            if dow in range(6):
                if h > 12 & h < 22:
                    if (df['ts'].ix[-1].to_pydatetime().year == y) & \
                            (df['ts'].ix[-1].to_pydatetime().month == m) & \
                            (df['ts'].ix[-1].to_pydatetime().day != d):
                        list_failing.append([filename, "DOW: {}".format(dow), \
                            "Day: {}\n".format(d), "DataFrame day: {}".format(\
                            df['ts'].ix[-1].to_pydatetime().day )])

    except Exception as e:
        if settings.SHOW_DEBUG:
            print("At gather_bad_file {}".format(e))
        try:
            list_failing.append([filename, "DOW: {}".format(dow), "Day: {}".format(d), \
                "Day from DataFrame: {}\n".format(df['ts'].ix[-1].to_pydatetime().day)])
        except:
            list_failing.append([filename, 'Empty'])
    return list_failing


def read_failing(filenames, path_to, loop, list_failing):

    def start_loop(loop, filenames):
        set_event_loop(loop)
        loop.run_until_complete(gather(*[gather_bad_file(filename=filename, \
            path_to=path_to, list_failing=list_failing) for filename in filenames]))

    cnt = len(filenames)
    batch_size = int(cnt/settings.CPUS)
    diff = cnt - (settings.CPUS * batch_size)

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

    return list_failing


@coroutine
def clean_failed_file(path_to, file_name):
    try:
        if settings.SHOW_DEBUG:
            print("Deleting: {}\n".format(file_name[0]))
            remove(join(path_to, file_name[0]))
    except Exception as err:
        print("At deleting: {}\n".format(err))


def data_checker(loop):
    list_failing = []
    p = join(settings.DATA_PATH, "incoming")
    filenames = multi_filenames(path_to_history=p, csv=True)

    list_failing = read_failing(filenames=filenames, path_to=p, loop=loop, list_failing=list_failing)

    if settings.SHOW_DEBUG:
        print("Failing symbols: {}\n".format(list_failing))

    cnt = len(list_failing)
    print("Failing number: {}\n".format(cnt))

    if (cnt > 0) & (cnt < 10):
        subject = "Failing datafiles: {}".format(cnt)
        message = "{0}\n\n".format(list_failing)
        sender = settings.DEFAULT_FROM_EMAIL
        send_mail(subject, message, sender, settings.NOTIFICATIONS_EMAILS)

        loop.run_until_complete(gather(*[clean_failed_file(path_to=p, \
            file_name=file_name) for file_name in list_failing], return_exceptions=True
        ))


def clean_folder(path_to):
    filenames = multi_filenames(path_to_history=path_to)
    for filename in filenames:
        try:
            remove(join(path_to, filename))
        except Exception as err:
            print(colored.red("clean_folder {}".format(err)))


def error_email(e):
    if settings.NOTIFICATIONS_ENABLED:
        send_mail('ERROR', '{}.\n\n'.format(e), \
            settings.DEFAULT_FROM_EMAIL,
            settings.NOTIFICATIONS_EMAILS, \
            fail_silently=True)


def create_periods():
    data = settings.PUBLIC_PERIODS

    for per in data:
        try:
            p = Periods.objects.create(period=per[0], name=per[1])
            p.save()
            print("Created period: {}\n".format(per[0]))
        except IntegrityError:
            pass
        except Exception as e:
            print(colored.red("[ERROR] At period creation: {0}\n".format(e)))


async def create_broker(_broker):
    try:
        broker = Brokers.objects.create(title=_broker)
        broker.save()
        if settings.SHOW_DEBUG:
            print("Broker created.")
    except IntegrityError:
        pass
    except Exception as e:
        print(colored.red("At create_bhroker {}\n".format(e)))

    return _broker


def create_symbol_postgres(name, broker):
    try:
        if len(broker) > 0:
            if len(name) > 0:
                broker_id = Brokers.objects.get(title=broker)
                symbol = Symbols.objects.create(symbol=name, broker=broker_id)
                symbol.save()
                print(colored.green("Created symbol at Postgres."))
    except IntegrityError:
        pass
    except Exception as e:
        print(colored.red("At creating symbol: {}\n".format(e)))


async def get_commission(symb: str, broker: str) -> float:
    try:
        symbol = Symbols.objects.filter(broker__title=broker, symbol=symb).values('commission')

        if symbol.count() > 0:
            try:
                value = float(symbol[0]['commission'])
            except Exception as err:
                if settings.SHOW_DEBUG:
                    print(colored.red("At getting commission {}\n".format(err)))
                value = None
        else:
            value = None

        return value

    except Exception as err:
        print(colored.red("get_commission {}".format(err)))


async def data_downloader(item, webdav):
    try:
        from_filename = item[0]
        to_filename = from_filename[3:].replace('%20', ' ')
        if 'DATA_MODEL' in to_filename:
            f = open(join(settings.DATA_PATH, 'incoming', to_filename), 'w')
            webdav.download(from_filename, f)
            f.close()
            if settings.SHOW_DEBUG:
                print("File downloaded {}".format(to_filename))
    except Exception as e:
        print(colored.red("At generate remote file {}\n".format(e)))


def generate_remote_files():
    def start_loop(loop, filelist):
        set_event_loop(loop)
        loop.run_until_complete(gather(*[data_downloader(item=item, webdav=webdav \
            ) for item in filelist], return_exceptions=True
        ))

    for resource in settings.WEBDAV_SOURCES:
        webdav = easywebdav.Client(host=resource[0], port=resource[1], 
            username=settings.WEBDAV_USERNAME, password=settings.WEBDAV_PASSWORD)
        filelist = webdav.ls()
        cnt = len(filelist)
        adjusted = int(settings.CPUS/2)
        batch_size = int(cnt/adjusted)
        diff = cnt - (adjusted * batch_size)

        processes = []
        for cpu in range(adjusted):
            if (cpu+1) == settings.CPUS:
                p = Process(target=start_loop, args=(new_event_loop(), filelist[cpu*batch_size:(cpu+1)*batch_size+diff]))
            else:
                p = Process(target=start_loop, args=(new_event_loop(), filelist[cpu*batch_size:(cpu+1)*batch_size]))
            processes.append(p)
            p.start()

        for p in processes:
            p.join()


async def key_gen(user):
    if not user.key:
        user.key = ''.join(choice(chars) for _ in range(size))
        user.save()
        print(colored.green("Generated key for: {}\n".format(user)))


def generate_keys(loop):
    size = settings.USER_KEY_SYMBOLS
    chars = ascii_uppercase + digits + ascii_lowercase

    users = QtraUser.objects.filter(user_type=1)

    loop.run_until_complete(gather(*[key_gen(user=user) \
        for user in users], return_exceptions=True
    ))


async def create_sym(filename):
    spl = filename.split('_')
    broker_ = spl[2]
    symbol = spl[3]

    broker = await create_broker(_broker=broker_)
    create_symbol(name=symbol, broker=broker)
    create_symbol_postgres(name=symbol, broker=broker)


def create_special_symbol(sym, broker):
    create_symbol(name=sym, broker=broker)
    create_symbol_postgres(name=sym, broker=broker)


def create_symbols(loop):
    path_to = join(settings.DATA_PATH, "incoming")
    filenames = multi_filenames(path_to_history=path_to, csv=True)

    loop.run_until_complete(gather(*[create_sym(\
        filename=filename) for filename in filenames],
        return_exceptions=True))

    syms = ['AI50']
    broker = 'Ava Trade EU Ltd.'
    for sym in syms:
        create_special_symbol(sym=sym, broker=broker)


async def get_currency(currency, broker_name, period):
    df = None
    if currency == 'AUD':
        filename = "{0}=={1}=={2}".format(broker_name, 'AUDUSD', period)
    elif currency == 'CAD':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDCAD', period)
    elif currency == 'GBP':
        filename = "{0}=={1}=={2}".format(broker_name, 'GBPUSD', period)
    elif currency == 'JPY':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDJPY', period)
    elif currency == 'HUF':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDHUF', period)
    elif currency == 'DKK':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDDKK', period)
    elif currency == 'NOK':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDNOK', period)
    elif currency == 'NZD':
        filename = "{0}=={1}=={2}".format(broker_name, 'NZDUSD', period)
    elif currency == 'ILS':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDILS', period)
    elif currency == 'SEK':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDSEK', period)
    elif currency == 'TRY':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDTRY', period)
    elif currency == 'RUB':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDRUB', period)
    elif currency == 'PLN':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDPLN', period)
    elif currency == 'CZK':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDCZK', period)
    elif currency == 'CNH':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDCNH', period)
    elif currency == 'THB':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDTHB', period)
    elif currency == 'CNY':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDCNY', period)
    elif currency == 'CHF':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDCHF', period)
    elif currency == 'ZAR':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDZAR', period)
    elif currency == 'SGD':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDSGD', period)
    elif currency == 'HKD':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDHKD', period)
    elif currency == 'MXN':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDMXN', period)
    elif currency == 'EUR':
        filename = "{0}=={1}=={2}".format(broker_name, 'EURUSD', period)
    elif currency == 'PNC':
        filename = "{0}=={1}=={2}".format(broker_name, 'GBPUSD', period)
    elif currency == 'CLP':
        filename = "{0}=={1}=={2}".format(broker_name, 'USDCLP', period)
    elif currency == 'USD':
        filename = None
    
    try:
        if not filename is None:
            df = df_multi_reader(filename=join(settings.DATA_PATH, "incoming_pickled", filename))
        else:
            df = 1.0
    except Exception as err:
        print("At get_currency {0} with {1}".format(err, currency))

    return df


async def process_commissions(symbol, multiplied_symbols):
    try:
        symbol_ = Symbols.objects.filter(symbol=symbol).values('currency', 'spread', 'digits', 'tick_size', 'tick_value', 'broker', 'symbol')
        if settings.SHOW_DEBUG:
            print("Processing commisions for {}".format(symbol_))

        if any(symbol_[0]['symbol'] in s for s in multiplied_symbols):
            value = (((power(10.0, -symbol_[0]['digits']) * \
                float(symbol_[0]['spread'])) / float(symbol_[0]['tick_size'])) * \
                float(symbol_[0]['tick_value'])) * 100.0
        else:
            value = (((power(10.0, -symbol_[0]['digits']) * \
                float(symbol_[0]['spread'])) / float(symbol_[0]['tick_size'])) * \
                float(symbol_[0]['tick_value']))

        symbol.commission = value
        symbol.save()
    except Exception as err:
        print(colored.red("At process commissions {}".format(err)))
        symbol.commission = None
        symbol.save()
    if settings.SHOW_DEBUG:
        print("Updated commision value for {0}\n".format(symbol.symbol))


def create_commissions(loop):
    multiplied_symbols = ['LTCMini', 'LTCWeekly']
    symbols = Symbols.objects.filter().exclude(symbol__in=ignored_symbols)

    loop.run_until_complete(gather(*[process_commissions(symbol=symbol, \
        multiplied_symbols=multiplied_symbols) for symbol in symbols],
        return_exceptions=True
    ))


async def adjustment_bureau(data, symbol_name, broker_name, period_name):
    try:
        broker = Brokers.objects.get(title=broker_name)
        symbol = Symbols.objects.filter(symbol=symbol_name, broker=broker).values('symbol', 'tick_size', 'tick_value', 'currency')

        if symbol.count() > 0:
            curr = await get_currency(currency=symbol[0]['currency'], broker_name=broker_name, period=period_name)
        else:
            curr = None

        if not (curr is None):
            try:
                tick_size = float(symbol[0]['tick_size'])
                tick_value = float(symbol[0]['tick_value'])
                tmp = ((data / tick_size) * tick_value)

                value = tmp * curr.CLOSE
                #print "Made currency adjusted df {0}.".format(value.dropna().head())
            except Exception as e:
                if settings.SHOW_DEBUG:
                    print("At making adjustment bureau {0}. Going for original.".format(e))
                value = ((data / tick_size) * tick_value)
        else:
            value = None
    except Exception as e:
        print("At adjustment bureau {0} with {1}".format(e, symbol))
        value = None

    return value


async def process_symbols_to_postgress(row):
    try:
        if settings.SHOW_DEBUG:
            print(row)

        if not (any(s in row[6] for s in ignored_symbols)):
            symbol = None
            try:
                symbol = Symbols.objects.get(symbol=row[6])
            except:
                pass
            if not symbol is None:
                broker = Brokers.objects.get(title=row[10])

                symbol.description = row[7]
                symbol.spread = row[0]
                symbol.tick_size = row[1]
                symbol.tick_value = row[2]
                symbol.digits = row[3]
                symbol.currency = row[4]
                symbol.price = row[9]
                symbol.profit_type = row[5]
                symbol.margin_initial = row[8]
                symbol.broker = broker
                symbol.save()
                if settings.SHOW_DEBUG:
                    print("Updated Postgres symbol data for {}\n".format(row[6]))
    except Exception as e:
        print(colored.red("At process_symbols_to_postgress symbols {0} with {1}".format(e, row)))


def symbol_data_to_postgres(dbsql, loop):
    c = dbsql.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    c.execute(query)
    query = "SELECT spread, tick_size, tick_value, digits, profit_currency, \
        profit_calc, symbol, description, margin_initial, price_at_calc_time, \
        broker FROM collector_symbols;"
    c.execute(query)
    res = c.fetchall()

    loop.run_until_complete(gather(*[process_symbols_to_postgress(row=row) for row in res],
        return_exceptions=True
    ))


"""
def portf_weights(data):
    try:
        d = ffn.get(tickers=[], provider=None, common_dates=False, forward_fill=False, \
            clean_tickers=False, column_names=None, ticker_field_sep=':', mrefresh=False, \
            existing=data)
        weights = d.calc_mean_var_weights(covar_method='ledoit-wolf', options={'maxiter': 5000000, 'disp': True })
    except Exception as e:
        print(colored.red("At portf_weights: {}\n".format(e)))
        weights = None

    return weights

def min_variance(loop):
    broker = Brokers.objects.get(slug='ava_trade_eu_ltd')
    idx = qindex(broker=broker)
    df_out, syms = [], []

    for i in idx:
        df = nonasy_df_multi_reader(filename=join(settings.DATA_PATH, "performance", \
            "{0}=={1}=={2}=={3}".format(i.broker.title, i.symbol.symbol, \
            i.period.period, i.system.title)))

        if i.direction == 1:
            df_out.append(df.LONG_PL_CUMSUM.resample('D').pad())
            syms.append("{0}_{1}_{2}_1".format(i.symbol.symbol, i.period.period, i.system.title))
        elif i.direction == 2:
            df_out.append(df.SHORT_PL_CUMSUM.resample('D').pad())
            syms.append("{0}_{1}_{2}_2".format(i.symbol.symbol, i.period.period, i.system.title))
        else:
            df_out.append(df.LONG_PL_CUMSUM.resample('D').pad())
            syms.append("{0}_{1}_{2}_0".format(i.symbol.symbol, i.period.period, i.system.title))
            df_out.append(df.SHORT_PL_CUMSUM.resample('D').pad())
            syms.append("{0}_{1}_{2}_0".format(i.symbol.symbol, i.period.period, i.system.title))

    returns_df = concat(df_out, axis=1).fillna(0.0)
    returns_df.columns = syms
    print("Concated")
    print(returns_df.tail())

    returns_df = returns_df.groupby(returns_df.columns, axis=1).sum()
    print("Grouped")
    print(returns_df.tail())

    weights = portf_weights(data=returns_df.pct_change().dropna())
    print("weights")
    print(weights)

    #except Exception as e:
        #print(colored.red("At min_variance ".format(e)))


#NOT USED anywhere, experimental
def execute_indicator(source):
    code = compile(source, "string", "exec")

    exec(code)

def get_indicator_source(title):
    indicator = Indicator.objects.filter(title=title)
    return indicator.content
"""
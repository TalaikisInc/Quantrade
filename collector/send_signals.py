from asyncio import gather
from typing import List, TypeVar
from datetime import date

from clint.textui import colored
from pandas import to_datetime

from django.conf import settings
from django.core.mail import send_mail
from django.core.exceptions import ObjectDoesNotExist

from .models import QtraUser, Signals, Brokers
from .indexes import qindex
from .utils import filename_constructor, nonasy_df_multi_reader

PandasDF = TypeVar('pandas.core.frame.DataFrame')


async def update_signal_sent_status(user, strategy, direction, df, i):
    try:
        s = Signals.objects.get(broker=strategy.broker, symbol=strategy.symbol,
            system=strategy.system, period=strategy.period, direction=direction,
            date_time=df.ix[i].name)
        if settings.SHOW_DEBUG:
            print("Got something from signals {}\n".format(s))
        s.sent_email = True
        s.save()
        if settings.SHOW_DEBUG:
            print(colored.green("Updated signal status."))
    except Exception as e:
        print(colored.red("At signal status update {}".format(e)))


async def send_signal(strategy, strings, df, i, user, direction):
    try:
        #if settings.SHOW_DEBUG:
        print("Trying to send signal from df {}...".format(df.ix[i].name))
        if direction == 1:
            direction_name = "Longs only"
        elif direction == 2:
            direction_name = "Shorts only"
        else:
            direction_name = "Longs and shorts"

        try:
            s = Signals.objects.filter(broker=strategy.broker,
                symbol=strategy.symbol, system=strategy.system, period=strategy.period,
                direction=direction, date_time=df.ix[i].name,
                sent_email=False).count()
            print("Signals that aren't sent for this day: {}\n".format(s))
        except Exception as err:
            print(colored.red("At send_signal collecting the list {}".format(err)))

        if s != 0:
            if len(str(user.email)) > 0:
                recipients = [user.email]
                subject = "{0} on Quantrade Trading Signals".format(strings[0])
                # FIXME as systems send separate signals for longs and shoerts,
                # it is impossible to show correct url for 'longs_shorts' for
                # strategy at this time
                #url = "https://quantrade.co.uk/{0}/{1}/{2}/{3}/{4}/".format(p.broker.slug, p.symbol.symbol, p.period.period, p.system.title, strings[1])
                #message = "{0}: {1} for {2} ({3}) - {4}. Using system: {5} ({6}). Size: {7}\n\nSystem URL:{8}\n\n".format(df.ix[i].name, strings[0], p.symbol.symbol, p.broker.title, p.period.name, p.system.title, direction_name, 1, url)
                message = "{0}: {1} for {2} ({3}) - {4}. Using system: {5} ({6}). Size: {7}\n\n".format(df.ix[i].name, strings[0], strategy.symbol.symbol, strategy.broker.title, strategy.period.name, strategy.system.title, direction_name, 1)
                sender = settings.DEFAULT_FROM_EMAIL

                try:
                    send_mail(subject, message, sender, recipients)
                    print(colored.green("Signal sent."))
                except Exception as e:
                    print(colored.red("At send_signal actual sending {}".format(e)))

                await update_signal_sent_status(user=user, strategy=strategy, direction=direction, df=df, i=i)
    except Exception as err:
        print(colored.red("At sending signal: {}\n".format(err)))


async def update_returns(strategy, direction, date_time, perf):
    try:
        try:
            signl = Signals.objects.get(broker=strategy.broker, symbol=strategy.symbol,
                period=strategy.period, system=strategy.system, direction=direction,
                date_time=date_time)
        except ObjectDoesNotExist:
            signl = None

        if not signl is None:
            df = perf.ix[date_time]

            if direction == 1:
                signl.returns = float(df.LONG_PL)
                signl.save()
                print(colored.green("Updated signal result"))
            elif direction == 2:
                signl.returns = float(df.SHORT_PL)
                signl.save()
                print(colored.green("Updated signal result"))
    except Exception as err:
        print(colored.red("At update_returns {}\n".format(err)))


async def save_signal(strategy, df, i, perf, direction):
    try:
        signl = Signals.objects.create(broker=strategy.broker, symbol=strategy.symbol,
            period=strategy.period, system=strategy.system, direction=direction,
            date_time=df.ix[i].name, returns=None)
        signl.save()
        print(colored.green("Signal saved for {0} -> {1}.".format(df.ix[i].name, strategy.symbol)))
    except IntegrityError:
        if not perf is None:
            if len(perf.index) > 0:
                await update_returns(strategy=strategy, direction=direction, \
                    date_time=df.ix[i].name, perf=perf.shift(-1))
    except Exception as err:
        print(colored.red("At save_signal {}".format(err)))


async def get_prev_day(d: int, mo: int) -> List[int]:
    """
    Get previous day from rovided day.
    """
    if (d > 1) & (d <= 31):
        prev_day = [d - 1]
    else:
        if mo == any([1, 4, 6, 8, 9, 11]):
            prev_day = [31]
        elif mo == any([5, 7, 10, 12]):
            prev_day = [30]
        else:
            prev_day = [28, 29]

    return prev_day


async def get_prev_mo(mo: int) -> int:
    """
    Get previous month from rovided month.
    """
    if mo == 1:
        prev_mo = 12
    else:
        prev_mo = mo - 1

    return prev_mo


async def mask_df(cnt: int, df: PandasDF, s) -> PandasDF:
    """
    Gets a DataFrame and returns only data that isn't found in Stats table.
    """
    try:
        if cnt == 0:
            fk_dt = datetime(2016, 10, 1)
            mask = (to_datetime(df.index).to_pydatetime() >= fk_dt)
        else:
            mask = (to_datetime(df.index).to_pydatetime() > s.date_time)

    except Exception as err:
        print(colored.red("mask_df {}".format(err)))

    return df[mask]


async def send_signal_point(t, strategy, user, df):
    """
    Processes everything one signal sending related.
    """
    try:
        if( ((t["df_year"] == t["ye"]) & (t["df_month"] == t["mo"]) & (df['ts'].ix[-1].to_pydatetime().day == t["to_day"])) | \
                ((t["df_year"] == t["ye"]) & (t["df_month"] == t["mo"]) & (df['ts'].ix[-1].to_pydatetime().day == t["to_day"]) & (strategy.period == 43200)) | \
                ((t["df_year"] == t["ye"]) &  (df['ts'].ix[-1].to_pydatetime().month == t["mo"]) & (t["df_day"] == any(t["prev_day"])) &  (t["df_weekday"] == 6) & (t["dow"] == 0) & (strategy.period == 10080)) | \
                ((t["df_year"] == t["ye"]) &  (t["df_month"] == t["prev_mo"]) & (t["df_day"] == any(t["end_prev_day"])) &  (t["df_weekday"] == 6) & (t["dow"] == 0) & (strategy.period == 10080)) ):

            if strategy.direction == 1:
                if df.ix[i].BUY_SIDE == 1:
                    if settings.SHOW_DEBUG:
                        print("Trying to send buy side...\n")
                    await send_signal(strategy=strategy, strings=['BUY', 'longs'], df=df, i=i, user=user, direction=1)
            elif strategy.direction == 2:
                if df.ix[i].SELL_SIDE == 1:
                    if settings.SHOW_DEBUG:
                        print("Trying to send sell side...\n")
                    await send_signal(strategy=strategy, strings=['SELL', 'shorts'], df=df, i=i, user=user, direction=2)
    except Exception as err:
        print(colored.red("save_signal_point {}\n".format(err)))


async def gen_time_data(df):
    t = {}
    now = date.today()
    t["ye"] = now.year
    t["mo"] = now.month
    t["to_day"] = now.day
    t["dow"] = now.weekday()
    t["prev_day"] = await get_prev_day(d=t["to_day"], mo=t["mo"])
    t["prev_mo"] = await get_prev_mo(mo=t["mo"])
    t["end_prev_day"] = [30, 31]
    df['ts'] = df.index
    df['ts'] = to_datetime(df['ts'])
    t["df_year"] = df['ts'].ix[-1].to_pydatetime().year
    t["df_month"] = df['ts'].ix[-1].to_pydatetime().month
    t["df_day"] = df['ts'].ix[-1].to_pydatetime().day
    t["df_weekday"] = df['ts'].ix[-1].to_pydatetime().weekday()

    return t, df


async def _save_signals(cnt, strategy, users, s, signals_df, perf_df):
    """
    Saves signals and sends further for user emails.
    """
    try:
        df = await mask_df(cnt=cnt, df=signals_df, s=s)
        l = len(df.index)
        if l > 0:
            perf = await mask_df(cnt=cnt, df=perf_df, s=s)
            t, df = await gen_time_data(df=df)

            for i in range(l):
                if strategy.direction == 1:
                    if df.ix[i].BUY_SIDE == 1:
                        if settings.SHOW_DEBUG:
                            print("Trying to save buy side...")
                        await save_signal(strategy=strategy, df=df, i=i, perf=perf, direction=1)
                elif strategy.direction == 2:
                    if df.ix[i].SELL_SIDE == 1:
                        if settings.SHOW_DEBUG:
                            print("Trying to save sell side...")
                        await save_signal(strategy=strategy, df=df, i=i, perf=perf, direction=2)

            gather(*[send_signal_point(t=t, strategy=strategy, user=user, df=df) \
                for user in users], return_exceptions=True)
    except Exception as err:
        print(colored.red("At _save_signals {}\n".format(err)))


def get_signal_status(strategy):
    try:
        s = Signals.objects.filter(broker=strategy.broker, \
            symbol=strategy.symbol, period=strategy.period, \
            system=strategy.system, direction=strategy.direction).latest("date_time")
        cnt = -1
    except ObjectDoesNotExist:
        cnt = 0
        s = None
    return cnt, s


async def user_signals(users, strategy):
    """
    Gets the status of latest signal date for each strategy and sends for save.
    """
    try:
        cnt, s = get_signal_status(strategy=strategy)
        info = {"broker": strategy.broker.title, "symbol": strategy.symbol.symbol, \
            "period": strategy.period.period, "system": strategy.system.title}

        file_name = filename_constructor(info=info, folder="systems")
        signals_df = nonasy_df_multi_reader(filename=file_name)
        file_name = filename_constructor(info=info, folder="performance")
        perf_df = nonasy_df_multi_reader(filename=file_name)

        if len(signals_df.index) > 0:
            await _save_signals(cnt=cnt, strategy=strategy, users=users, s=s, \
                signals_df=signals_df, perf_df=perf_df)
    except Exception as err:
        print(colored.red("user_signals {}".format(err)))


def generate_signals(loop):
    """
    Main function that generates signals for each strategy for users.
    """
    users = QtraUser.objects.all()
    brokers = Brokers.objects.all()

    #TODO query user specific broker only!!!
    #Also needs an user form to choose broker signals
    for broker in brokers:
        strategies = qindex(broker=broker)

        loop.run_until_complete(gather(*[user_signals(users=users, \
            strategy=strategy) for strategy in strategies], return_exceptions=True))

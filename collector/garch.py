from os import listdir
from os.path import (join, isfile)
from datetime import (datetime, timedelta, date)
import asyncio
from decimal import Decimal

from pandas import (to_datetime, DataFrame)
from arch import arch_model
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import statsmodels.api as sm
from clint.textui import colored

from django.conf import settings
from django.template.defaultfilters import slugify

from .models import (GARCH, Brokers, Symbols, Periods)
from .tasks import read_df


async def save_garch(broker, symbol, period, change):
    try:
        g = GARCH.objects.create(broker=broker, symbol=symbol, period=period, \
            date_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), change=Decimal(change.item()))
        g.save()
        if settings.SHOW_DEBUG:
            print(colored.green("Created GARCH change for {}".format(symbol)))
    except Exception as e:
        print(colored.red(e))


async def gtdb(filename):
    try:
        spl = filename.split("==")
        broker_slug = spl[0]
        symbol_slug = spl[1]
        broker = Brokers.objects.get(title=broker_slug)
        symbol = Symbols.objects.get(symbol=symbol_slug)
        period_slug = spl[2].split('.')[0]
        period = Periods.objects.get(period=period_slug)

        if '1440' in period_slug:
            df = await read_df(filename=join(settings.DATA_PATH, 'garch', filename))
            df['ts'] = df.index
            df['ts'] = to_datetime(df['ts'])

            yesterday = df.ix[(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")]
            today = df.ix[datetime.now().strftime("%Y-%m-%d")]
            tomorrow = df.ix[(datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")]

            change = tomorrow.values[0]*100 - yesterday.values[0]*100
            if settings.SHOW_DEBUG:
                print("Day change {}".format(change))

            await save_garch(broker=broker, symbol=symbol, period=period, change=change)

        #weekly
        if '10080' in period_slug:
            df = await read_df(filename=join(settings.DATA_PATH, 'garch', filename))
            df['ts'] = df.index
            df['ts'] = to_datetime(df['ts'])

            this_week = df.ix[-6]
            next_week = df.ix[-5]

            #print("This week")
            #rint(this_week)
            #rint("Next week")
            #rint(next_week)

            change = next_week.values[0]*100 - this_week.values[0]*100
            if settings.SHOW_DEBUG:
                print("Week change {}".format(change))

            await save_garch(broker=broker, symbol=symbol, period=period, change=change)

        #monthly
        if '43200' in period_slug:
            df = await read_df(filename=join(settings.DATA_PATH, 'garch', filename))
            df['ts'] = df.index
            df['ts'] = to_datetime(df['ts'])

            this_month = df.ix[-6]
            next_month = df.ix[-5]

            #print("This month")
            #print(this_month)
            #print("Next month")
            #print(next_month)

            change = next_month.values[0]*100 - this_month.values[0]*100
            if settings.SHOW_DEBUG:
                print("Month change {}".format(change))

            await save_garch(broker=broker, symbol=symbol, period=period, change=change)
    except Exception as e:
        print(colored.red("At garch gtdb {}".format(e)))


def clean_garch():
    try:
        g = GARCH.objects.get(date_time=(datetime.now() - timedelta(days=5)\
            ).strftime("%Y-%m-%d %H:%M:%S")).delete()
    except Exception as e:
        print(colored.red("At deleting GARCH {}".format(e)))
        g = GARCH.objects.filter().delete()


def garch_to_db(loop):
    filenames = [f for f in listdir(join(settings.DATA_PATH, 'garch')) if \
        isfile(join(settings.DATA_PATH, 'garch', f))]

    loop.run_until_complete(asyncio.gather(*[gtdb(filename=filename) for \
        filename in filenames], return_exceptions=True
    ))


async def write_g(fl):
    mdpi = 72
    try:
        filename = join(settings.DATA_PATH, fl)

        df = await read_df(filename=filename)

        df['return'] = df['CLOSE'].pct_change().dropna()
        #df['std'] = df['return'].rolling(21).std()*(252**0.5)
        #df['variance'] = df['std']**2
        df = df.dropna()
        returns = df['return']

        am = arch_model(returns, vol='Garch', p=1, o=1, q=1, dist='Normal')
        dte = returns.index[-1]
        res = am.fit(update_freq=1)
        d = res.conditional_volatility ** 2.0
        forecasts = res.forecast(horizon=5)

        if '1440' in fl:
            f = DataFrame(forecasts.variance.dropna().head().as_matrix()[0], index=[dte+timedelta(days=1),
                dte+timedelta(days=2),
                dte+timedelta(days=3),
                dte+timedelta(days=4),
                dte+timedelta(days=5)])
            final = d[-1000:].append(f)
            #print("final")
            #print(final)
        elif '10080' in fl:
            f = DataFrame(forecasts.variance.dropna().head().as_matrix()[0], index=[dte+timedelta(days=1*7),
                dte+timedelta(days=2*7),
                dte+timedelta(days=3*7),
                dte+timedelta(days=4*7),
                dte+timedelta(days=5*7)])
            final = d.append(f)
        if '43200' in fl:
            f = DataFrame(forecasts.variance.dropna().head().as_matrix()[0], index=[dte+timedelta(days=1*30),
                dte+timedelta(days=2*30),
                dte+timedelta(days=3*30),
                dte+timedelta(days=4*30),
                dte+timedelta(days=5*30)])
            final = d.append(f)

        file_name = fl.split(".mp")[0]
        broker = str(slugify(file_name.split("==")[0])).replace('-', '_')
        symbol = file_name.split("==")[1]
        period = file_name.split("==")[2]
        out_filename = join(settings.DATA_PATH, 'garch', fl)
        ofl = "{0}=={1}=={2}.png".format(broker, symbol, period)
        out_image = join(settings.STATIC_ROOT, 'collector', 'images', 'garch', ofl)
        title = "{0} {1} GJR-GARCH forecast".format(symbol, period)

        final.to_pickle(path=out_filename)

        plt.figure(figsize=(int(900/mdpi), int(720/mdpi)), dpi=mdpi)
        plt.plot(final, label=title, color='r', lw=1)
        plt.plot(f, label='Forecast', color='r', lw=4)
        plt.savefig(out_image)
        plt.close()
        print(colored.green("Made GARCH for {}".format(symbol)))
    except Exception as e:
        print(colored.red("At write garch ".format(e)))


def garch(loop):
    filenames = [f for f in listdir(settings.DATA_PATH) if isfile(join(settings.DATA_PATH, f))]

    loop.run_until_complete(asyncio.gather(*[write_g(fl=fl) for fl \
        in filenames], return_exceptions=True
    ))

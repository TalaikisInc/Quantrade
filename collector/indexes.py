from asyncio import gather
from datetime import datetime, date
from os.path import join

from clint.textui import colored
from pandas import date_range, concat

from django.conf import settings

from .models import Stats, Brokers
from .utils import nonasy_df_multi_reader, filename_constructor, nonasy_df_multi_writer


def qindex(broker):
    try:
        stats = Stats.objects.select_related("symbol", "period", "broker", \
            "system").filter(sharpe__gt=settings.SHARPE, trades__gt=settings.MIN_TRADES, \
            broker=broker, win_rate__gt=settings.WIN_RATE).exclude(system__title="AI50", \
            avg_trade__isnull=True, avg_trade__lt=0.0, trades__isnull=True, \
            symbol__commission__isnull=True, sortino__lt=0.2).exclude(\
            symbol__symbol="AI50").order_by('sortino').reverse()[:50]
    except Exception as err:
        print(colored.red(" At qindex {}".format(err)))

    return stats


async def collect_idx_dfs(df):
    return df


def generate_qindex(loop):
    try:
        brokers = Brokers.objects.all()
        for broker in brokers:
            print("Going to make index for {}".format(broker.title))
            idx = qindex(broker=broker)

            df_out = loop.run_until_complete(gather(*[collect_idx_dfs(df=nonasy_df_multi_reader(\
                filename=filename_constructor(info={"broker": i.broker.title, \
                "symbol": i.symbol.symbol, "period": i.period.period, "system": i.system.title}, \
                folder="performance"))) for i in idx], return_exceptions=True))

            dates = date_range(end=date(datetime.now().year, datetime.now().month, \
                datetime.now().day),periods=20*252, freq="D", name="DATE_TIME", tz=None)
            df = concat(df_out, axis=1, join_axes=[dates])

            try:
                df.rename(columns={'SHORT_PL_CUMSUM': 'LONG_PL_CUMSUM',
                    'SHORT_PL': 'LONG_PL',
                    'SHORT_TRADES': 'LONG_TRADES',
                    'SHORT_MAE': 'LONG_MAE',
                    'SHORT_MFE': 'LONG_MFE',
                    'SHORT_MARGIN': 'LONG_MARGIN'
                    }, inplace=True)

                df["LONG_PL_CUMSUM"] = df["LONG_PL_CUMSUM"].fillna(method='ffill')
                df["LONG_MAE"] = df["LONG_MAE"].fillna(method='ffill')
                df = df.fillna(0.0)
                df = df.groupby(df.columns, axis=1).sum()

                final = concat([df.DIFF/100.0, df.LONG_MAE/100.0, df.LONG_MARGIN/100.0, \
                    df.LONG_PL/100.0, df.LONG_PL_CUMSUM/100.0, df.LONG_TRADES], axis=1)
                final.columns = ['DIFF', 'LONG_MAE', 'LONG_MARGIN', 'LONG_PL', \
                    'LONG_PL_CUMSUM', 'LONG_TRADES']

                out_filename = join(settings.DATA_PATH, 'portfolios', '{}_qndx'.format(broker.slug))
                nonasy_df_multi_writer(df=final, out_filename=out_filename)

                print(colored.green("Wrote qindex at {}".format(out_filename)))
            except Exception as err:
                print(colored.red("At generate_qindex {}".format(err)))
    except Exception as err:
        print(colored.red("At generate_qindex {}".format(err)))
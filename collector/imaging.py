from asyncio import gather
from os.path import join, isfile, getmtime
from datetime import datetime, timedelta

from clint.textui import colored
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import cm
import seaborn as sns
import warnings
warnings.simplefilter("ignore")

from django.template.defaultfilters import slugify
from django.conf import settings

from .models import Stats, Brokers
from .utils import multi_filenames, name_deconstructor, ext_drop, df_multi_reader, filename_constructor


async def make_strat_image(info, data):
    mdpi = 300
    try:
        title = "{0} on {1} {2} [{3}]".format(info["system"], info["symbol"], \
            info["period"], info["broker"])

        if len(data) > 0:
            data.rename(columns={'LONG_PL_CUMSUM': 'Strategy, longs', \
                'SHORT_PL_CUMSUM': 'Strategy, shorts', 'LONG_DIFF_CUMSUM': \
                'Buy & hold', 'SHORT_DIFF_CUMSUM': 'Short & hold'}, inplace=True)

            plt.figure(figsize=(3840/mdpi, 2160/mdpi), dpi=mdpi)
            data['Strategy, longs'].plot(x='Date', y='Value, $', legend=True, \
                style='r', lw=3).scatter(x=data.index, \
                y=data.LONG_MAE, label='MAE', color='DarkGreen')
            data['Buy & hold'].plot(legend=True, style='g').set_title(title+', Longs')
            plt.axhline(y=0.0)
            info["direction"] = "longs"
            plt.savefig(filename_constructor(info=info, folder="meta"))
            plt.close()

            plt.figure(figsize=(3840/mdpi, 2160/mdpi), dpi=mdpi)
            data['Strategy, shorts'].plot(x='Date', y='Value, $', legend=True, \
                style='r', lw=3).scatter(x=data.index, \
                y=data.SHORT_MAE, label='MAE', color='DarkGreen')
            data['Short & hold'].plot(legend=True, style='g').set_title(title+', Shorts')
            plt.axhline(y=0.0)
            info["direction"] = "shorts"
            plt.savefig(filename_constructor(info=info, folder="meta"))
            plt.close()

            plt.figure(figsize=(3840/mdpi, 2160/mdpi), dpi=mdpi)
            data['Strategy, longs & shorts'] = data['Strategy, shorts'] + \
                data['Strategy, longs']
            data['Strategy, longs & shorts'].plot(x='Date', \
                y='Value, $', legend=True, style='r', \
                lw=3).scatter(x=data.index, y=(data.SHORT_MAE+data.LONG_MAE), \
                label='MAE', color='DarkGreen')
            data['Buy & hold'].plot(legend=True, style='g'\
                ).set_title(title+', Longs & shorts')
            plt.axhline(y=0.0)
            info["direction"] = "longs_shorts"
            plt.savefig(filename_constructor(info=info, folder="meta"))
            plt.close()
            if settings.SHOW_DEBUG:
                print(colored.green("Made images"))
    except Exception as err:
        print(colored.red("At make_strat_image {}".format(err)))


async def make_image(path_to, filename):
    try:
        data = await df_multi_reader(filename=join(path_to, filename))

        if len(data.index) > 0:
            info = name_deconstructor(filename=filename, t="s")
            info["broker"] = slugify(info["broker"]).replace('-', '_')
            info["direction"] = "longs"
            image_filename = filename_constructor(info=info, folder="meta")
            data = data.loc[data['CLOSE'] != 0]

            if (not isfile(image_filename)) | (datetime.fromtimestamp(getmtime(image_filename)) < \
                    (datetime.now() - timedelta(days=30))):
                await make_strat_image(info=info, data=data)
    except Exception as err:
        print(colored.red("At making images {}\n".format(err)))


def make_images(loop):
    path_to = join(settings.DATA_PATH, "performance")
    filenames = multi_filenames(path_to_history=path_to)

    loop.run_until_complete(gather(*[make_image(path_to=path_to, \
        filename=filename) for filename in filenames], \
        return_exceptions=True))


async def aggregate_returns(returns, convert_to):
    if convert_to == 'weekly':
        return returns.groupby(
            [lambda x: x.year,
             lambda x: x.month,
             lambda x: x.isocalendar()[1]]).apply(cumulate_returns)
    if convert_to == 'monthly':
        return returns.groupby(
            [lambda x: x.year, lambda x: x.month]).apply(cumulate_returns)
    if convert_to == 'yearly':
        return returns.groupby(
            [lambda x: x.year]).apply(cumulate_returns)


async def write_y(returns, image_filename):
    try:
        ax = plt.gca()
        ax.yaxis.grid(linestyle=':')

        ret_plt = await aggregate_returns(returns, 'yearly') #* 100.0
        ret_plt.plot(kind="bar")
        ax.set_title('Yearly Returns, %', fontweight='bold')
        ax.set_ylabel('')
        ax.set_xlabel('')
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
        ax.xaxis.grid(False)
        plt.savefig(image_filename)
        plt.close()
        if settings.SHOW_DEBUG:
            print(colored.green("Wrote yearly graph {}".format(image_filename)))
    except Exception as err:
        print(colored.red("At write_y".format(err)))


async def make_yearly_returns(returns, info):
    image_filename = filename_constructor(info=info, folder="yearly")

    if (not (isfile(image_filename))) | (datetime.fromtimestamp(getmtime(image_filename)) < \
            (datetime.now() - timedelta(days=30))):
        await write_y(returns=returns, image_filename=image_filename)


async def save_qindex_heatmap(data, image_filename):
    try:
        monthly_ret = await aggregate_returns(returns=data, convert_to='monthly')
        monthly_ret = monthly_ret.unstack()
        monthly_ret = round(monthly_ret, 3)
        monthly_ret.rename(
            columns={1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
                     5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
                     9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'},
            inplace=True
        )
        ax = plt.gca()

        sns.heatmap(
            monthly_ret.fillna(0), # * 100.0,
            annot=True,
            fmt="0.1f",
            annot_kws={"size": 8},
            alpha=1.0,
            center=0.0,
            cbar=False,
            cmap=cm.RdYlGn,
            ax=ax)
        ax.set_title('A.I. Returns, %', fontweight='bold')

        plt.savefig(image_filename)
        plt.close()
        if settings.SHOW_DEBUG:
            print(colored.green("Wrote heatmap image for {}\n".format(image_filename)))
    except Exception as err:
        print(colored.red("At save_qindex_heatmap {}".format(err)))


async def convert_to_perc(data, info):
    try:
        stats = Stats.objects.get(broker__slug=info["broker"], symbol__symbol=info["symbol"], \
            period__period=info["period"], system__title=info["system"], direction=info["direction"])
        acc_min = stats.acc_minimum
        if settings.SHOW_DEBUG:
            print("Account minimum {}".format(acc_min))
        p = (data / float(acc_min)) * 100.0
    except Exception as err:
        if settings.SHOW_DEBUG:
            print(colored.red("At convert_to_perc {}".format(err)))
        p = None

    return p


async def qindex_heatmap(broker):
    try:
        info = {"broker": broker, "symbol": "AI50", "period": "1440", \
            "system": "AI50", "direction": "longs"}
        filename = join(settings.DATA_PATH, 'portfolios', '{}_qndx'.format(broker))
        image_filename = filename_constructor(info=info, folder="heatmap")
        data = await df_multi_reader(filename=filename)

        info["direction"] = 1
        returns = await convert_to_perc(data=data.last('108M').LONG_PL, info=info)

        if not returns is None:
            returns.columns = ['LONG_PL']
            if (not isfile(image_filename)) | (datetime.fromtimestamp(getmtime(image_filename)) < \
                    (datetime.now() - timedelta(days=30))):
                await save_qindex_heatmap(data=returns, image_filename=image_filename)
        await make_yearly_returns(returns=returns, info=info)
    except Exception as err:
        print(colored.red("At qindex_heatmap {}".format(err)))


async def write_h(image_filename, data):
    try:
        monthly_ret = await aggregate_returns(returns=data, convert_to='monthly')
        monthly_ret = monthly_ret.unstack()
        monthly_ret = round(monthly_ret, 3)
        monthly_ret.rename(
            columns={1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
                     5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
                     9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'},
            inplace=True
        )
        ax = plt.gca()

        sns.heatmap(
            monthly_ret.fillna(0), # * 100.0,
            annot=True,
            fmt="0.1f",
            annot_kws={"size": 8},
            alpha=1.0,
            center=0.0,
            cbar=False,
            cmap=cm.RdYlGn,
            ax=ax)
        ax.set_title('Returns heatmap, %', fontweight='bold')

        plt.savefig(image_filename)
        plt.close()
        if settings.SHOW_DEBUG:
            print(colored.green("Wrote heatmap image for {}\n".format(image_filename)))
    except Exception as err:
        print(colored.red("At write_h".format(err)))


async def save_heatmap(data, info):
    try:
        image_filename = filename_constructor(info=info, folder="heatmap")

        if (not isfile(image_filename)) | (datetime.fromtimestamp(getmtime(image_filename)) < \
                (datetime.now() - timedelta(days=30))):
            await write_h(image_filename=image_filename, data=data)
    except Exception as err:
        print(colored.red("At save_heatmap {}".format(err)))


async def make_heat_img(path_to, filename):
    try:
        info = name_deconstructor(filename=filename, t="s")
        info["broker"] = str(slugify(info["broker"])).replace("-", "_")

        file_name = join(path_to, info["filename"])
        file_name = ext_drop(filename=file_name)
        df = await df_multi_reader(filename=file_name)

        if len(df.index) > settings.MIN_TRADES:
            info["direction"] = 1
            longs = await convert_to_perc(data=df.LONG_PL, info=info)
            info["direction"] = 2
            shorts = await convert_to_perc(data=df.SHORT_PL, info=info)
            info["direction"] = 0
            long_short = await convert_to_perc(data=(df.LONG_PL + df.SHORT_PL), info=info)

            if not longs is None:
                info["direction"] = "longs"
                await save_heatmap(data=longs, info=info)
                await make_yearly_returns(returns=longs, info=info)
            if not shorts is None:
                info["direction"] = "shorts"
                await save_heatmap(data=longs, info=info)
                await make_yearly_returns(returns=longs, info=info)
            if not long_short is None:
                info["direction"] = "longs_shorts"
                await save_heatmap(data=longs, info=info)
                await make_yearly_returns(returns=longs, info=info)

    except Exception as err:
        print(colored.red("At make_heat_img {}".format(err)))


def generate_monthly_heatmaps(loop):
    brokers = Brokers.objects.all()
    path_to = join(settings.DATA_PATH, "performance")
    filenames = multi_filenames(path_to_history=path_to)

    loop.run_until_complete(gather(*[make_heat_img(\
        path_to=path_to, filename=filename) for filename in filenames], \
        return_exceptions=True))

    #AI50 index heatmap
    loop.run_until_complete(gather(*[qindex_heatmap(broker=broker.slug) for broker in brokers],
        return_exceptions=True))


async def image_to_db(path_to, s):
    try:
        partial_path = 'static/collector/images/'

        if s.direction == 1:
            dir_slug = 'longs'
        if s.direction == 2:
            dir_slug = 'shorts'
        if s.direction == 0:
            dir_slug = 'longs_shorts'

        f = "{0}=={1}=={2}=={3}=={4}.png".format(s.broker.slug, s.symbol.symbol, s.period.period, s.system.title, dir_slug)
        if isfile(join(path_to, 'meta', f)):
            filename = "https://quantrade.co.uk/{0}meta/{1}".format(partial_path, f)
            s.img = filename
            if settings.SHOW_DEBUG:
                print(colored.green("Wrote images urls to db for {}".format(filename)))
        if isfile(join(path_to, 'heatmap', f)):
            filename = "https://quantrade.co.uk/{0}heatmap/{1}".format(partial_path, f)
            s.heatmap = filename
            if settings.SHOW_DEBUG:
                print(colored.green("Wrote images urls to db for {}".format(filename)))
        if isfile(join(path_to, 'yearly', f)):
            filename = "https://quantrade.co.uk/{0}yearly/{1}".format(partial_path, f)
            s.yearly_ret = filename
            if settings.SHOW_DEBUG:
                print(colored.green("Wrote images urls to db for {}".format(filename)))
        if isfile(join(path_to, 'mc', f)):
            filename = "https://quantrade.co.uk/{0}mc/{1}".format(partial_path, f)
            s.yearly_ret = filename
            if settings.SHOW_DEBUG:
                print(colored.green("Wrote images urls to db for {}".format(filename)))

        strategy_url = "https://quantrade.co.uk/{0}/{1}/{2}/{3}/{4}/".format(s.broker.slug, s.symbol.symbol, s.period.period, s.system.title, dir_slug)
        s.strategy_url = strategy_url

        s.save()
    except Exception as err:
        print(colored.red("At writing urls {}".format(err)))


def process_urls_to_db(loop):
    path_to = join(settings.STATIC_ROOT, 'collector', 'images')

    stats = Stats.objects.all()

    loop.run_until_complete(gather(*[image_to_db(path_to=path_to, s=s) for s in stats],
        return_exceptions=True))
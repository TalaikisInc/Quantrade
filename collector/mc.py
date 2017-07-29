from asyncio import gather
from os.path import join, isfile
from typing import TypeVar

from clint.textui import colored
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from clint.textui import colored
from scipy import stats
from pandas import DataFrame, concat, read_pickle

from django.template.defaultfilters import slugify
from django.conf import settings

from .utils import ext_drop, filename_constructor, name_deconstructor, multi_filenames, \
    nonasy_df_multi_writer, nonasy_df_multi_reader, df_multi_writer
from .arctic_utils import nonasy_init_calcs, generate_performance
from _private.strategies_list import indicator_processor, strategy_processor
from .models import Stats
from .tasks import clean_folder

PandasDF = TypeVar('pandas.core.frame.DataFrame')


def mc_maker(loop, filename):
    try:
        print("Working with {}".format(filename))
        seed_size = 3000

        info = name_deconstructor(filename=filename, t="")

        file_name = join(settings.DATA_PATH, "incoming_pickled", info["filename"])
        df = nonasy_df_multi_reader(filename=file_name)

        try:
            close= df.CLOSE.diff().dropna()
        except Exception as err:
            close = None

        if (len(df.index) > 0) & (close is not None):
            close_params = stats.t.fit(close)

            for path in range(100):
                close_roll = stats.t.rvs(df=close_params[0], loc=close_params[1], \
                    scale=close_params[2], size=seed_size)

                out_filename = join(settings.DATA_PATH, "monte_carlo", info["filename"] + "==" + str(path))
                out_df = DataFrame({"CLOSE": close_roll.reshape((-1,1))[:,0],
                    "HIGH": close_roll.reshape((-1,1))[:,0],
                    "LOW": close_roll.reshape((-1,1))[:,0]}).cumsum()

                final = nonasy_init_calcs(df=out_df, symbol=info["symbol"])
                nonasy_df_multi_writer(df=final, out_filename=out_filename)

        print("Indicators...")
        path_to = join(settings.DATA_PATH, "monte_carlo")
        filenames = multi_filenames(path_to_history=path_to)
        mc_trader(loop=loop, filenames=filenames, t="i")

        print("Strategies...")
        path_to_iindicators = join(settings.DATA_PATH, "monte_carlo", "indicators")
        mc_trader(loop=loop, filenames=filenames, t="s")

        print("Performance...")
        path_to_systems = join(settings.DATA_PATH, 'monte_carlo', 'systems')
        filenames = multi_filenames(path_to_history=path_to_systems)
        mc_trader(loop=loop, filenames=filenames, t="p")

        print("Finalizing...")
        path_to_performance = join(settings.DATA_PATH, "monte_carlo", "performance")
        filenames = multi_filenames(path_to_history=path_to_performance)
        mc_trader(loop=loop, filenames=filenames, t="a")

        print("Cleaning...")
        clean_folder(path_to=path_to)
        clean_folder(path_to=path_to_iindicators)
        clean_folder(path_to=path_to_systems)
        clean_folder(path_to=path_to_performance)

    except Exception as err:
        print(colored.red(" At mc_maker {}".format(err)))


def mc(loop):
    """
    First function to start Monte Carlo.
    """
    filenames = multi_filenames(path_to_history=join(settings.DATA_PATH, "incoming_pickled"))

    for filename in filenames:
        mc_maker(loop=loop, filename=filename)


async def img_writer(info: dict, pdfm: list, df: PandasDF) -> None:
    try:
        out_image = filename_constructor(info=info, folder="mc", mc=True)

        df.plot(lw=3, color='r')
        for pdf in pdfm:
            pdf.plot()
        plt.savefig(out_image)
        plt.close()

        img = "{0}==_{1}=={2}=={3}=={4}.png".format(info["broker"], info["symbol"], \
            info["period"], info["system"], info["direction"])

        if info["direction"] == "longs":
            info["direction"] = 1
        if info["direction"] == "shorts":
            info["direction"] = 2
        if info["direction"] == "longs_shorts":
            info["direction"] = 0

        stats = Stats.objects.get(broker__slug=info["broker"], symbol__symbol=info["symbol"], \
            period__period=info["period"], system__title=info["system"], direction=info["direction"])
        stats.mc = "https://quantrade.co.uk/static/collector/images/mc/" + img
        stats.save()
        print(colored.green("Image saved {}.".format(img)))
    except Exception as err:
        print(colored.red("img_writer {}".format(err)))


async def path_writer(info: dict) -> None:
    try:
        pdfm = []
        for path in range(100):
            try:
                info["path"] = path
                file_name = filename_constructor(info=info, folder="performance", mc=True)

                pdf = nonasy_df_multi_reader(filename=file_name).reset_index()

                if len(pdf.index) > 0:
                    del pdf["index"]
                    if info["direction"] == "longs":
                        pdfm.append(pdf['LONG_PL_CUMSUM'])
                    elif info["direction"] == "shorts":
                        pdfm.append(pdf['SHORT_PL_CUMSUM'])
                    else:
                        pdfm.append(pdf['LONG_PL_CUMSUM']+pdf['SHORT_PL_CUMSUM'])
            except Exception as err:
                print(colored.red("path_writer for path {}".format(err)))

        if len(pdfm) > 20:
            d = concat([d for d in pdfm], axis=1)
            d.columns = [info["symbol"]+" "+info["period"]+" "+info["system"]] * len(d.columns)

            df = d.groupby(d.columns, axis=1).sum() / len(d.columns)

            info["broker"] = slugify(info["broker"]).replace("-", "_")
            out_filename = filename_constructor(info=info, folder="avg")

            await df_multi_writer(df=df, out_filename=out_filename)
            print(colored.green("Average saved: {}".format(out_filename)))

            await img_writer(info=info, pdfm=pdfm, df=df)
    except Exception as err:
        print(colored.red("path_writer {}".format(err)))


def unique_strats(filenames):
    unique_filenames = []
        
    for filename in filenames:
        filename = ext_drop(filename=filename)
        spl = filename.split("==")
        unique_filenames.append(spl)

    df = DataFrame(unique_filenames)
    df.columns = ["BROKER", "SYMBOL", "PERIOD", "SYSTEM", "PATH"]
    del df["PATH"]

    return df.drop_duplicates().reset_index()


async def mc_agg_point(udf, s):
    try:
        info = {}
        info["broker"] = udf.ix[s]["BROKER"]
        info["symbol"] = udf.ix[s]["SYMBOL"]
        info["period"] = udf.ix[s]["PERIOD"]
        info["system"] = udf.ix[s]["SYSTEM"]

        directions = ["longs", "shorts", "longs_shorts"]
        for direction in directions:
            info["direction"] = direction
            await path_writer(info=info)
    except Exception as err:
        print(colored.red("mc_agg_point {}".format(err)))


def aggregate(loop, filenames):
    udf = unique_strats(filenames=filenames)

    loop.run_until_complete(gather(*[mc_agg_point(udf=udf, s=s) for \
        s in range(len(udf.index))], return_exceptions=True))


def mc_trader(loop, filenames, t):
    if t == "i":
        indicator_processor(mc=True, filenames=filenames)
    if t == "s":
        strategy_processor(mc=True, filenames=filenames)
    if t == "p":
        generate_performance(loop=loop, mc=True, filenames=filenames)
    if t == "a":
        aggregate(loop=loop, filenames=filenames)

from asyncio import gather
from os.path import join, isfile

from clint.textui import colored
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from clint.textui import colored
from scipy import stats
from pandas import DataFrame

from django.template.defaultfilters import slugify
from django.conf import settings

from .utils import ext_drop, filename_constructor, name_deconstructor, multi_filenames, \
    df_multi_reader, df_multi_writer
from .arctic_utils import init_calcs, generate_performance
from _private.strategies_list import indicator_processor, strategy_processor
from .models import Stats


async def mc_maker(filename):
    try:
        size = 3000

        info = name_deconstructor(filename=filename, t="")

        file_name = join(settings.DATA_PATH, "incoming_pickled", info["filename"])
        df = await df_multi_reader(filename=file_name)

        try:
            close= df.CLOSE.diff().dropna()
        except Exception as err:
            close = None

        if (len(df.index) > 0) & (close is not None):
            close_params = stats.t.fit(close)

            for path in range(100):
                close_roll = stats.t.rvs(df=close_params[0], loc=close_params[1], \
                    scale=close_params[2], size=size)

                out_filename = join(settings.DATA_PATH, "monte_carlo", info["filename"] + "==" + str(path))
                out_df = DataFrame({"CLOSE": close_roll.reshape((-1,1))[:,0],
                    "HIGH": close_roll.reshape((-1,1))[:,0],
                    "LOW": close_roll.reshape((-1,1))[:,0]}).cumsum()

                final = await init_calcs(df=out_df, symbol=info["symbol"])
                await df_multi_writer(df=final, out_filename=out_filename)
    except Exception as err:
        print(colored.red(" At mc_maker {}".format(err)))


def mc(loop):
    filenames = multi_filenames(path_to_history=join(settings.DATA_PATH, "incoming_pickled"))

    loop.run_until_complete(gather(*[mc_maker(filename=filename) for \
        filename in filenames], return_exceptions=True
    ))


async def img_writer(info: dict) -> None:
    out_image = filename_constructor(info=info, folder="mc")
    plt.savefig(out_image)
    plt.close()

    stats = Stats.objects.get(broker__slug=info["broker"], symbol__symbol=info["symbol"], \
        period__period=info["period"], system__title=info["system"])
    stats.mc = "https://quantrade.co.uk/static/collector/images/mc/" + img
    stats.save()

    print(colored.green("Image saved {}.".format(img)))


async def path_writer(info: dict) -> None:
    s_paths = 0
    for path in range(100):
        try:
            file_name = filename_constructor(info=info, folder="performance", mc=True)

            pdf = await df_multi_reader(filename=file_name).reset_index()
            del pdf["index"]

            if len(pdf.index) > 0:
                if info["direction"] == "longs":
                    pdf['LONG_PL_CUMSUM'].plot()
                    pdfm = pdf['LONG_PL_CUMSUM']
                elif info["direction"] == "shorts":
                    pdf['SHORT_PL_CUMSUM'].plot()
                    pdfm = pdf['SHORT_PL_CUMSUM']
                else:
                    (pdf['LONG_PL_CUMSUM']+pdf['SHORT_PL_CUMSUM']).plot()
                    pdfm = (pdf['LONG_PL_CUMSUM']+pdf['SHORT_PL_CUMSUM'])
                pdfm += pdfm
                s_paths += 1
        except Exception as err:
            print(colored.red("path_writer {}".format(err)))

    if (len(pdfm.index) > 0) & (len(s_paths) == 100):
        out_filename = filename_constructor(info=info, folder="avg")
        
        #reduce by aprox. one (first) path
        pdfm = pdfm*0.99
        (pdfm/100).plot(lw=3, color='r')
                
        await df_multi_writer(df=pdfm, out_filename=out_filename)
        print(colored.green("Average saved {}.".format(out_filename)))

        await img_writer(info=info)


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
        info["broker"] = slugify(udf.ix[s]["BROKER"]).replace("-", "_")
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
        s in range(len(udf.index))], return_exceptions=True
    ))


def mc_trader(loop, batch, batch_size, filenames, t):
    filenames = filenames[batch*batch_size:(batch+1)*batch_size-1]

    if t == "i":
        indicator_processor(mc=True, filenames=filenames)
    if t == "s":
        strategy_processor(mc=True, filenames=filenames)
    if t == "p":
        generate_performance(loop=loop, mc=True, filenames=filenames)
    if t == "a":
        aggregate(filenames=filenames)

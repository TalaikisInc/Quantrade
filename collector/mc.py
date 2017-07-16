import asyncio
from os import listdir, name
from os.path import join, isfile

from clint.textui import colored
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from clint.textui import colored
from scipy import stats
from pandas import DataFrame
from numpy import zeros

from django.template.defaultfilters import slugify
from django.conf import settings

from .tasks import df_multi_reader, multi_filenames, df_multi_writer, ext_drop
from .arctic_utils import init_calcs, generate_performance
from _private.strategies_list import indicator_processor, strategy_processor
from .models import Stats


async def mc_maker(filename):
    try:
        size = 3000
        filename = ext_drop(filename=filename)
        print(filename)

        file_name = join(settings.DATA_PATH, "incoming_pickled", filename)
        df = await df_multi_reader(filename=file_name)

        try:
            close= df.CLOSE.diff().dropna()
        except Exception as err:
            close = None

        if (len(df.index) > 0) & (close is not None):
            spl = filename.split('==')
            broker = slugify(spl[0]).replace("-", "_")
            symbol = spl[1]
            period = spl[2]

            close_params = stats.t.fit(close)

            for path in range(100):
                close_roll = stats.t.rvs(df=close_params[0], loc=close_params[1], \
                    scale=close_params[2], size=size)

                out_filename = join(settings.DATA_PATH, "monte_carlo", filename + "==" + str(path))
                out_df = DataFrame({"CLOSE": close_roll.reshape((-1,1))[:,0],
                    "HIGH": close_roll.reshape((-1,1))[:,0],
                    "LOW": close_roll.reshape((-1,1))[:,0]}).cumsum()

                final = await init_calcs(df=out_df, symbol=symbol)
                await df_multi_writer(df=final, out_filename=out_filename)
    except Exception as err:
        print(colored.red(" At mc_maker {}".format(err)))


def mc(loop):
    filenames = multi_filenames(path_to_history=join(settings.DATA_PATH, "incoming_pickled"))

    loop.run_until_complete(asyncio.gather(*[mc_maker(filename=filename) for \
        filename in filenames], return_exceptions=True
    ))


async def img_writer(broker: str, symbol: str, period: str, system: str, direction: str) -> None:
    broker_slug = slugify(broker).replace("-", "_")
    img = "{0}=={1}=={2}=={3}=={4}.png".format(broker_slug, symbol, period, \
        system, direction)

    out_image = join(settings.STATIC_ROOT, "collector", "images", "mc", img)
    plt.savefig(out_image)
    plt.close()

    stats = Stats.objects.get(broker__title=broker, symbol__symbol=symbol, \
        period__period=period, system__title=system)
    stats.mc = "https://quantrade.co.uk/static/collector/images/mc/" + img
    stats.save()

    print(colored.green("Image saved {}.".format(img)))


async def path_writer(broker: str, symbol: str, period: str, system: str, direction: str) -> None:
    s_paths = 0
    for path in range(100):
        try:
            flname_construct = "{0}=={1}=={2}=={3}=={4}".format(broker, symbol, period, system, path)
            file_name = join(settings.DATA_PATH, "monte_carlo", "performance", flname_construct)

            pdf = await df_multi_reader(filename=file_name).reset_index()
            del pdf["index"]

            if len(pdf.index) > 0:
                if direction == "longs":
                    pdf['LONG_PL_CUMSUM'].plot()
                    pdfm = pdf['LONG_PL_CUMSUM']
                elif direction == "shorts":
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
        out_filename = join(settings.DATA_PATH, "monte_carlo", "avg", \
            "{0}=={1}=={2}=={3}=={4}".format(broker, symbol, period, system, direction))
        
        #reduce by aprox. one (first) path
        pdfm = pdfm*0.99
        (pdfm/100).plot(lw=3, color='r')
                
        await df_multi_writer(df=pdfm, out_filename=out_filename)
        print(colored.green("Average saved {}.".format(out_filename)))

        await img_writer(broker=broker, symbol=symbol, period=period, system=system, direction=direction)


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
        broker = udf.ix[s]["BROKER"]
        symbol = udf.ix[s]["SYMBOL"]
        period = udf.ix[s]["PERIOD"]
        system = udf.ix[s]["SYSTEM"]
            
        directions = ["longs", "shorts", "longs_shorts"]
            
        for direction in directions:
            await path_writer(broker=broker, symbol=symbol, period=period, \
                system=system, direction=direction)
            
    except Exception as err:
        print(colored.red("mc_agg_point {}".format(err)))


def aggregate(loop, filenames):
    udf = unique_strats(filenames=filenames)

    loop.run_until_complete(asyncio.gather(*[mc_agg_point(udf=udf, s=s) for \
        s in range(len(udf.index))], return_exceptions=True
    ))


def mc_trader(loop, batch, batch_size, filenames, t):
    filenames = filenames[batch*batch_size:(batch+1)*batch_size-1]
    print(filenames)

    if t == "i":
        indicator_processor(loop=loop, mc=True, filenames=filenames)
    if t == "s":
        strategy_processor(loop=loop, mc=True, filenames=filenames)
    if t == "p":
        generate_performance(loop=loop, mc=True, filenames=filenames)
    if t == "a":
        aggregate(loop=loop, filenames=filenames)

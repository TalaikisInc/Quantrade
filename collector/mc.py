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


async def mc_maker(filename):
    try:
        size = 3000
        filename = ext_drop(filename=filename)
        print(filename)

        file_name = join(settings.DATA_PATH, "incoming_pickled", filename)
        df = df_multi_reader(filename=file_name)

        try:
            close= df.CLOSE.diff().dropna()
        except Exception as err:
            close = None

        if (len(df.index) > 0) & (close is not None):
            spl = filename.split('==')
            broker = slugify(spl[0]).replace("-", "_")
            symbol = spl[1]
            period = spl[2]
            system = spl[3].split(".")[0]

            close_params = stats.t.fit(close)

            for path in range(100):
                close_roll = stats.t.rvs(df=close_params[0], loc=close_params[1], \
                    scale=close_params[2], size=size)

                out_filename = join(settings.DATA_PATH, "monte_carlo", filename + "==" + str(path))
                out_df = DataFrame({"CLOSE": close_roll.reshape((-1,1))[:,0],
                    "HIGH": close_roll.reshape((-1,1))[:,0],
                    "LOW": close_roll.reshape((-1,1))[:,0]}).cumsum()

                final = await init_calcs(df=out_df, symbol=symbol)
                df_multi_writer(df=final, out_filename=out_filename)
    except Exception as err:
        print(colored.red(" At mc_maker {}".format(err)))


def mc(loop):
    filenames = multi_filenames(path_to_history=join(settings.DATA_PATH, "incoming_pickled"))

    loop.run_until_complete(asyncio.gather(*[mc_maker(filename=filename) for \
        filename in filenames], return_exceptions=True
    ))


def img_writer(broker: str, symbol: str, period: str, system: str) -> None:
    img = "{0}=={1}=={2}=={3}==longs.png".format(broker, symbol, period, system)
    out_image = join(settings.STATIC_ROOT, "collector", "images", "mc", img)
    plt.savefig(out_image)
    plt.close()
    print(colored.green("Image saved {}.".format(img)))


def aggregate():
    filenames = multi_filenames(path_to_history=join(settings.DATA_PATH, \
        "monte_carlo", "performance"))
    unique_filenames = []
    
    for filename in filenames:
        filename = ext_drop(filename=filename)
        spl = filename.split("==")
        unique_filenames.append(spl)

    df = DataFrame(unique_filenames)
    df.columns = ["BROKER", "SYMBOL", "PERIOD", "SYSTEM", "PATH"]
    tmp = df
    del tmp["PATH"]
    udf = tmp.drop_duplicates().reset_index()
    
    for s in range(len(udf.index)):
        try:
            broker = udf.ix[s]["BROKER"]
            symbol = udf.ix[s]["SYMBOL"]
            period = udf.ix[s]["PERIOD"]
            system = udf.ix[s]["SYSTEM"]
            
            s_paths = 0
            for path in range(100):
                try:
                    filename = "{0}=={1}=={2}=={3}=={4}".format(broker, symbol, period, system, path)
                    file_name = join(settings.DATA_PATH, "monte_carlo", "performance", filename)

                    pdf = df_multi_reader(filename=file_name).reset_index()
                    del pdf["index"]

                    if len(pdf.index) > 0:
                        pdf['LONG_PL_CUMSUM'].plot()
                        pdfm = pdf['LONG_PL_CUMSUM']
                        pdfm += pdfm
                        s_paths += 1
                except Exception as err:
                    print(colored.red(err))

            if (len(pdfm.index) > 0) & (len(s_paths) == 100):
                out_filename = join(settings.DATA_PATH, "monte_carlo", "avg", \
                    "{0}=={1}=={2}=={3}==longs".format(broker, symbol, period, system))
                
                df_multi_writer(df=pdfm*0.99, out_filename=out_filename)
                print(colored.green("Average saved {}.".format(out_filename)))

                img_writer(broker=broker, symbol=symbol, period=period, system=system)
        except Exception as err:
            print(colored.red(err))


def mc_trader(loop):
    indicator_processor(loop=loop, mc=True)
    strategy_processor(loop=loop, mc=True)
    generate_performance(loop=loop, mc=True)
    aggregate()

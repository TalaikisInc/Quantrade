
from os.path import join, isfile
from typing import TypeVar

from clint.textui import colored
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from clint.textui import colored
from scipy import stats
from pandas import DataFrame, concat, read_pickle

from django.db import IntegrityError
from django.template.defaultfilters import slugify
from django.conf import settings

from .utils import ext_drop, filename_constructor, name_deconstructor, multi_filenames, \
    nonasy_df_multi_writer, nonasy_df_multi_reader
from .arctic_utils import nonasy_init_calcs, generate_performance
from _private.strategies_list import indicator_processor, strategy_processor
from .models import Stats, MCJobs, Systems
from .tasks import clean_folder

strategies = Systems.objects.all()
PandasDF = TypeVar('pandas.core.frame.DataFrame')

#TODO it works even if stats matching query doesn't exist, should check at start

def cycle(loop):
    try:
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

    except Exception as err:
        print(colored.red("cycle {}".format(err)))


def clean():
    print("Cleaning...")
    path_to = join(settings.DATA_PATH, "monte_carlo")
    clean_folder(path_to=path_to)
    path_to_iindicators = join(settings.DATA_PATH, "monte_carlo", "indicators")
    clean_folder(path_to=path_to_iindicators)
    path_to_systems = join(settings.DATA_PATH, 'monte_carlo', 'systems')
    clean_folder(path_to=path_to_systems)
    path_to_performance = join(settings.DATA_PATH, "monte_carlo", "performance")
    clean_folder(path_to=path_to_performance)


def mc_maker(loop, job):
    try:
        clean()

        print("Working with {0}".format(job.filename))
        seed_size = 3000

        info = name_deconstructor(filename=job.filename, t="")

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

    except Exception as err:
        print(colored.red(" At mc_maker {}".format(err)))


def mc(loop):
    """
    First function to start Monte Carlo.
    """
    #j = MCJobs.objects.filter().delete()
    jobs = MCJobs.objects.filter(status=0, direction=1)

    if jobs.count() == 0:
        filenames = multi_filenames(path_to_history=join(settings.DATA_PATH, "incoming_pickled"))
        directions = [0, 1, 2]

        for filename in filenames:
            try:
                for direction in directions:
                    j = MCJobs.objects.create(filename=filename, direction=direction)
                    j.save()
                    print(colored.green("Saved MC job to database: {}".format(filename)))
            except IntegrityError:
                pass
            except Exception as err:
                print(colored.red("mc at creating jobs: {}".format(err)))
    else:
        for job in jobs:
            mc_maker(loop=loop, job=job)
            cycle(loop=loop)

            directions = [0, 1, 2]
            for d in directions:
                info = name_deconstructor(filename=job.filename, t="")
                try:
                    j = MCJobs.objects.get(filename=job.filename, direction=d)
                    
                    print("Finalizing...")
                    mc_trader(loop=loop, filenames=[], job=j, t="a")

                    print("Updating status...")
                    j.status = 1
                    j.save()
                except Exception as err:
                    print("mc j {}".format(err))


def img_writer(info: dict, pdfm: list, df: PandasDF, job) -> None:
    try:
        out_image = filename_constructor(info=info, folder="mc", mc=True)

        df.plot(lw=3, color='r')
        for pdf in pdfm:
            pdf.plot()
        plt.savefig(out_image)
        plt.close()

        img = "{0}=={1}=={2}=={3}=={4}.png".format(info["broker"], info["symbol"], \
            info["period"], info["system"], info["direction"])

        stats = Stats.objects.get(broker__slug=info["broker"], symbol__symbol=info["symbol"], \
            period__period=info["period"], system__title=info["system"], direction=job.direction)
        stats.mc = "https://quantrade.co.uk/static/collector/images/mc/" + img
        stats.save()
        print(colored.green("Image saved {}.".format(img)))
    except Exception as err:
        print(colored.red("img_writer {}".format(err)))


def aggregate(loop, job):
    try:
        info = name_deconstructor(filename=job.filename, t="")

        for s in strategies:
            info["system"] = s.title

            if job.direction == 1:
                info["direction"] = "longs"
            elif job.direction == 2:
                info["direction"] = "shorts"
            else:
                info["direction"] = "longs_shorts"

            pdfm = []
            for path in range(100):
                try:
                    info["path"] = path
                    info["broker"] = info["filename"].split("==")[0]

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

                nonasy_df_multi_writer(df=df, out_filename=out_filename)
                print(colored.green("Average saved: {}".format(out_filename)))

                img_writer(info=info, pdfm=pdfm, df=df, job=job)
    except Exception as err:
        print(colored.red("path_writer {}".format(err)))


def mc_trader(loop, filenames, t, job=None):
    if t == "i":
        indicator_processor(filenames=filenames, mc=True)
    if t == "s":
        strategy_processor(filenames=filenames, mc=True)
    if t == "p":
        generate_performance(loop=loop, filenames=filenames, mc=True)
    if t == "a":
        aggregate(loop=loop, job=job)

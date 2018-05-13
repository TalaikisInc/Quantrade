from functools import lru_cache
from typing import List, TypeVar
from os import listdir, remove
from os.path import isfile, join
from subprocess import Popen

from clint.textui import colored
from pandas import DataFrame, read_msgpack, read_json, read_hdf, read_feather, HDFStore, read_pickle

from django.conf import settings

PandasDF = TypeVar('pandas.core.frame.DataFrame')


def ext_drop(filename: str) -> str:
    assert isinstance(filename, str)

    if settings.DATA_TYPE == "pickle":
        file_name = filename.replace(".mp", "")
    if settings.DATA_TYPE == "proto2":
        file_name = filename.replace(".pr2", "")
    if settings.DATA_TYPE == "messagepack":
        file_name = filename.replace(".pack", "")
    if settings.DATA_TYPE == "json":
        file_name = filename.replace(".json", "")
    if settings.DATA_TYPE == "feather":
        file_name = filename.replace(".fth", "")
    if settings.DATA_TYPE == "hdf":
        file_name = filename.replace(".hdf", "")
    
    return file_name


def filename_constructor(info: dict, folder: str, mc: bool=False) -> str:
    assert isinstance(info, dict)
    assert isinstance(folder, str)
    assert isinstance(mc, bool)

    if mc:
        if folder == "mc":
            filename = join(settings.STATIC_ROOT, "collector", "images", "mc", \
                "{0}=={1}=={2}=={3}=={4}.png".format(info["broker"], info["symbol"], \
                info["period"], info["system"], info["direction"]))
        if folder == "indicators":
            filename = join(settings.DATA_PATH, "monte_carlo", folder, \
                "{0}=={1}=={2}=={3}=={4}".format(info["broker"], \
                info["symbol"], info["period"], info["indicator"], info["path"]))
        if (folder == "systems") | (folder == "performance"):
            filename = join(settings.DATA_PATH, "monte_carlo", folder, \
                "{0}=={1}=={2}=={3}=={4}".format(info["broker"], info["symbol"], \
                info["period"], info["system"], info["path"]))
    else:
        if folder == "incoming_pickled":
            filename = join(settings.DATA_PATH, folder, "{0}=={1}=={2}".format(\
                info["broker"], info["symbol"], info["period"]))
        if folder == "indicators":
            filename = join(settings.DATA_PATH, folder, "{0}=={1}=={2}=={3}".format(\
                info["broker"], info["symbol"], info["period"], info["indicator"]))
        if (folder == "systems") | (folder == "performance"):
            filename = join(settings.DATA_PATH, folder, "{0}=={1}=={2}=={3}".format(\
                info["broker"], info["symbol"], info["period"], info["system"]))
        if folder == "json":
            filename = join(settings.DATA_PATH, "systems", "json", "{0}=={1}=={2}=={3}.json".\
                format(info["broker"], info["symbol"], info["period"], info["system"]))
        if folder == "incoming":
            filename = join(settings.DATA_PATH, "incoming", info["filename"])
        if folder == "garch":
            filename = join(settings.STATIC_ROOT, 'collector', 'images', 'garch', \
                "{0}=={1}=={2}.png".format(info["broker"], info["symbol"], info["period"]))
        if folder == "avg":
            filename = join(settings.DATA_PATH, "monte_carlo", "avg", "{0}=={1}=={2}=={3}=={4}".\
                format(info["broker"], info["symbol"], info["period"], \
                info["system"], info["direction"]))
        if folder == "meta":
            filename = join(settings.STATIC_ROOT, "collector", "images", "meta", \
                "{0}=={1}=={2}=={3}=={4}.png".format(info["broker"], info["symbol"], \
                info["period"], info["system"], info["direction"]))
        if folder == "yearly":
            filename = join(settings.STATIC_ROOT, "collector", "images", "yearly", \
                "{0}=={1}=={2}=={3}=={4}.png".format(info["broker"], info["symbol"], \
                info["period"], info["system"], info["direction"]))
        if folder == "heatmap":
            filename = join(settings.STATIC_ROOT, "collector", "images", "heatmap", \
                "{0}=={1}=={2}=={3}=={4}.png".format(info["broker"], \
            info["symbol"], info["period"], info["system"], info["direction"]))

    assert isinstance(filename, str), "filename isn't a string: %s" % filename

    return filename


def name_deconstructor(filename: str, t: str, mc: bool=False) -> dict:
    try:
        assert isinstance(filename, str), "filename isn't string!"
        assert isinstance(t, str), "t value isn't string, should be: 's' or 'i'"
        assert isinstance(mc, bool), "mc should be boolean"

        if settings.DATA_TYPE != "hdfone":
            filename = ext_drop(filename=filename)

        spl = filename.split('==')
        broker = spl[0]
        symbol = spl[1]
        period = spl[2]
        if t == "s":
            system = spl[3]
        else:
            system = None
        if t == "i":
            indicator = spl[3]
        else:
            indicator = None
        if mc:
            if t != "":
                path = spl[4]
            else:
                path = spl[3]
        else:
            path = None

        info = {"filename": filename, "broker": broker, "symbol": symbol, "period": period, 
            "system": system, "path": path, "indicator": indicator }

        assert isinstance(info, dict), "Deconstruct should be dictionary."

        return info

    except Exception as err:
        print(colored.red("name_deconstructor {}".format(err)))


def multi_filenames(path_to_history: str, csv: bool=False) -> List[str]:
    filenames = []
    try:
        if settings.DATA_TYPE == "pickle":
            filenames = [f for f in listdir(path_to_history) \
                if isfile(join(path_to_history, f)) & \
                ("mp" == f.split(".")[-1])]
        if settings.DATA_TYPE == "proto2":
            filenames = [f for f in listdir(path_to_history) \
                if isfile(join(path_to_history, f)) & \
                ("pr2" == f.split(".")[-1])]
        if settings.DATA_TYPE == "messagepack":
            filenames = [f for f in listdir(path_to_history) \
                if isfile(join(path_to_history, f)) & \
                ("pack" == f.split(".")[-1])]
        if settings.DATA_TYPE == "json":
            filenames = [f for f in listdir(path_to_history) \
                if isfile(join(path_to_history, f)) & \
                ("json" == f.split(".")[-1])]
        if settings.DATA_TYPE == "feather":
            filenames = [f for f in listdir(path_to_history) \
                if isfile(join(path_to_history, f)) & \
                ("fth" == f.split(".")[-1])]
        if settings.DATA_TYPE == "hdf":
            filenames = [f for f in listdir(path_to_history) \
                if isfile(join(path_to_history, f)) & \
                ("hdf" == f.split(".")[-1])]
        if csv:
            filenames = [f for f in listdir(path_to_history) \
                if isfile(join(path_to_history, f)) & \
                ("csv" == f.split(".")[-1])]
    except Exception as err:
        print(colored.red("multi_filenames {}".format(err)))
    
    return filenames


async def multi_remove(filename: str) -> None:
    try:
        if settings.DATA_TYPE == "pickle":
            f = filename + ".mp"
        if settings.DATA_TYPE == "proto2":
            f = filename + ".pr2"
        if settings.DATA_TYPE == "messagepack":
            f = filename + ".pack"
        if settings.DATA_TYPE == "json":
            f = filename + ".json"
        if settings.DATA_TYPE == "feather":
            f = filename + ".fth"
        if settings.DATA_TYPE == "hdf":
            f = filename + ".hdf"
        if isfile(f):
            remove(f)
    except Exception as err:
        print(colored.red("multi_remove {}".format(err)))


@lru_cache(maxsize=None)
async def df_multi_reader(filename: str, limit: bool=False) -> PandasDF:
    df = DataFrame()

    try:
        assert isinstance(filename, str), "filename isn't string %s" % filename
        assert isinstance(limit, bool), "limit isn't bool %s" % limit

        if settings.DATA_TYPE == "pickle":
            f = filename + ".mp"
            if isfile(f):
                df = read_pickle(f)
        if settings.DATA_TYPE == "proto2":
            f = filename + ".pr2"
            if isfile(f):
                df = read_pickle(f)
        if settings.DATA_TYPE == "messagepack":
            f = filename + ".pack"
            if isfile(f):
                df = read_msgpack(f)
        if settings.DATA_TYPE == "json":
            f = filename + ".json"
            if isfile(f):
                df = read_json(f)
        if settings.DATA_TYPE == "feather":
            #TODO feather doesn't handle indexes
            f = filename + ".fth"
            if isfile(f):
                df = read_feather(f).reset_index()
        if settings.DATA_TYPE == "hdf":
            f = filename + ".hdf"
            if isfile(f):
                df = read_hdf(f, key=filename)
        if settings.DATA_TYPE == "hdfone":
            f = join(settings.DATA_PATH, "hdfone.hdfone")
            if isfile(f):
                df = read_hdf(f, key=filename, mode='r')
        
        if limit:
            if len(df.index) > 0:
                df = df.last(settings.LIMIT_MONTHS)
    except Exception as err:
        print(colored.red("MultiReader {}".format(err)))

    return df


@lru_cache(maxsize=None)
def nonasy_df_multi_reader(filename: str, limit: bool=False) -> PandasDF:
    df = DataFrame()

    try:
        assert isinstance(filename, str), "filename isn't string: %s" % filename
        assert isinstance(limit, bool), "limit isn't bool: %s" % limit

        if settings.DATA_TYPE == "pickle":
            f = filename + ".mp"
            if isfile(f):
                df = read_pickle(f)
        if settings.DATA_TYPE == "proto2":
            f = filename + ".pr2"
            if isfile(f):
                df = read_pickle(f)
        if settings.DATA_TYPE == "messagepack":
            f = filename + ".pack"
            if isfile(f):
                df = read_msgpack(f)
        if settings.DATA_TYPE == "json":
            f = filename + ".json"
            if isfile(f):
                df = read_json(f)
        if settings.DATA_TYPE == "feather":
            #TODO feather doesn't handle indexes
            f = filename + ".fth"
            if isfile(f):
                df = read_feather(f).reset_index()
        if settings.DATA_TYPE == "hdf":
            f = filename + ".hdf"
            if isfile(f):
                df = read_hdf(f, key=filename)
        if settings.DATA_TYPE == "hdfone":
            f = join(settings.DATA_PATH, "hdfone.hdfone")
            if isfile(f):
                df = read_hdf(f, key=filename, mode='r')
        
        if limit:
            df = df.last(settings.LIMIT_MONTHS)

    except Exception as err:
        print(colored.red("MultiReader {}".format(err)))

    return df


async def df_multi_writer(df: PandasDF, out_filename: str) -> None:
    try:
        assert isinstance(out_filename, str), "out_filename isn't string: %s" % out_filename

        if settings.DATA_TYPE == "pickle":
            df.to_pickle(out_filename + ".mp")
        if settings.DATA_TYPE == "proto2":
            df.to_pickle(path=out_filename + ".pr2", compression='gzip', protocol=2)
        if settings.DATA_TYPE == "messagepack":
            df.to_msgpack(out_filename + ".pack")
        if settings.DATA_TYPE == "json":
            df.to_json(out_filename + ".json")
        if settings.DATA_TYPE == "feather":
            df.to_feather(out_filename + ".fth")
        if settings.DATA_TYPE == "hdf":
            o = out_filename + ".hdf"
            df.to_hdf(o, key=out_filename, mode="w")
        if settings.DATA_TYPE == "hdfone":
            o = join(settings.DATA_PATH, "hdfone.hdfone")
            df.to_hdf(o, key=out_filename, mode="a")
    except Exception as err:
        print(colored.red("df_multi_writer {}".format(err)))


#drop this over time, used by qindex
def nonasy_df_multi_writer(df: PandasDF, out_filename: str) -> None:
    try:
        assert isinstance(out_filename, str), "out_filename isn't string: %s" % out_filename

        if settings.DATA_TYPE == "pickle":
            df.to_pickle(out_filename + ".mp")
        if settings.DATA_TYPE == "proto2":
            df.to_pickle(path=out_filename + ".pr2", compression='gzip', protocol=2)
        if settings.DATA_TYPE == "messagepack":
            df.to_msgpack(out_filename + ".pack")
        if settings.DATA_TYPE == "json":
            df.to_json(out_filename + ".json")
        if settings.DATA_TYPE == "feather":
            df.to_feather(out_filename + ".fth")
        if settings.DATA_TYPE == "hdf":
            o = out_filename + ".hdf"
            df.to_hdf(o, key=out_filename, mode="w")
        if settings.DATA_TYPE == "hdfone":
            o = join(settings.DATA_PATH, "hdfone.hdfone")
            df.to_hdf(o, key=out_filename, mode="a")
    except Exception as err:
        print(colored.red("df_multi_writer {}".format(err)))


@lru_cache(maxsize=None)
def hdfone_filenames(folder: str, path_to: str) -> List[str]:
    filenames = []
    try:
        assert isinstance(folder, str), "folder isn't string: %s" % folder
        assert isinstance(path_to, str), "path_to isn't string: %s" % path_to

        if settings.DATA_TYPE == "hdfone":
            f = join(settings.DATA_PATH, "hdfone.hdfone")
            if isfile(f):
                with HDFStore(f) as hdf:
                    filenames = [f for f in hdf.keys() if folder in f]
                hdf.close()
        else:
            filenames = multi_filenames(path_to_history=path_to)
    except Exception as err:
        print(colored.red("hdfone_filenames: {}".format(err)))
    
    return filenames


async def file_cleaner(filename: str) -> None:
    try:
        assert isinstance(filename, str), "filename isn't string: %s" % filename

        multi_remove(filename=filename)
        if settings.SHOW_DEBUG:
            print("Removed failing file from data folder {}.".format(filename))
    except Exception as err:
        print(colored.red("At file cleaning {}".format(err)))


def create_folders():
    """
    Creates required directories.
    """
    folders = ["data", "data/incoming", "data/garch", "data/incoming_pickled", 
        "data/incoming_pickled/csv", "data/indicators", "data/indicators/csv", 
        "data/monte_carlo", "data/monte_carlo/indicators", "data/monte_carlo/systems", 
        "data/monte_carlo/performance", "data/monte_carlo/avg", "data/performance", 
        "data/performance/csv", "data/portfolios", "data/portfolios/csv", "data/quandl", 
        "data/portfolios/csv", "data/quandl/csv", "data/systems", "data/systems/csv", 
        "data/systems/json"]

    for folder in folders:
        try:
            Popen("mkdir {0}/{1}".format(settings.BASE_DIR, folder))
        except Exception as err:
            print(colored.red("create_folders {}".format(err)))

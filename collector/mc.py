import asyncio
from os import listdir
from os.path import (join, isfile)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from clint.textui import colored
from scipy import stats

from django.conf import settings

from .tasks import df_multi_reader


async def mc_maker(data, params, size):
    params = stats.t.fit(data)
    roll = stats.t.rvs(df=params[0], loc=params[1], scale=params[2], size=size)


def mc(loop):
    filenames = [f for f in listdir(settings.DATA_PATH) if isfile(join(\
        settings.DATA_PATH, f))]

    loop.run_until_complete(asyncio.gather(*[distr_determiner(filename=filename) for \
        filename in filenames], return_exceptions=True
    ))

    loop.run_until_complete(asyncio.gather(*[mc_maker(filename=filename) for \
        filename in filenames], return_exceptions=True
    ))

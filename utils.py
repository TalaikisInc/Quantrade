from asyncio import set_event_loop_policy, get_event_loop
from argparse import ArgumentParser
from time import time
from os import environ, name
from os.path import join, isfile

if not name is 'nt':
    import uvloop
environ["DJANGO_SETTINGS_MODULE"] = "quantrade.settings"
import django
django.setup()
from django.conf import settings
from django.core.mail import send_mail

from collector.mysql_utils import mysql_setup, mysql_tables, drop_db, \
    mysql_connect_db
from collector.tasks import create_periods, create_symbols, create_commissions,\
    symbol_data_to_postgres, generate_keys, pickle_to_svc, \
    generate_remote_files, data_checker, quandl_process #, min_variance
from collector.indexes import generate_qindex
from collector.send_signals import generate_signals
from collector.stats import generate_stats
from collector.imaging import make_images, generate_monthly_heatmaps, process_urls_to_db
from collector.corr import generate_correlations
from collector.utils import multi_filenames, hdfone_filenames, create_folders
from collector.facebook import face_publish, heatmap_to_facebook
from collector.twitter import post_tweets, heatmap_to_twitter
from collector.garch import (garch, garch_to_db, clean_garch)
from collector.arctic_utils import data_model_csv, generate_performance
from collector.mc import mc, mc_trader
from _private.strategies_list import indicator_processor, strategy_processor


parser = ArgumentParser(description="Quantrade tasks")
parser.add_argument('--hourly')
parser.add_argument('--daily')
parser.add_argument('--monthly')
parser.add_argument('--csv')
parser.add_argument('--setup')
parser.add_argument('--minvar')
parser.add_argument('--mc')
args = parser.parse_args()


def main():
    if not name is 'nt':
        set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = get_event_loop()

    if args.hourly:
        start_time = time()

        print("Collecting data...")
        generate_remote_files()

        print("Cheking data...")
        data_checker(loop=loop)

        print("Initial pickling...")
        data_model_csv()

        print("Quandl...")
        quandl_process(loop=loop)

        print("Indicators...")
        path_to = join(settings.DATA_PATH, "incoming_pickled")
        if settings.DATA_TYPE == "hdfone":
            filenames = hdfone_filenames(folder="incoming_pickled", path_to=path_to)
        else:
            filenames = multi_filenames(path_to_history=path_to)
        indicator_processor(filenames=filenames)

        print("Strategies...")
        path_to = join(settings.DATA_PATH, "incoming_pickled")
        if settings.DATA_TYPE == "hdfone":
            filenames = hdfone_filenames(folder="incoming_pickled", path_to=path_to)
        else:
            filenames = multi_filenames(path_to_history=path_to)
        strategy_processor(filenames=filenames)

        print("Signals...")
        generate_signals(loop=loop)

        print("Signal generation tasks: %s seconds ---" % (time() - start_time))

    if args.daily:
        dbsql = mysql_connect_db()
        start_time = time()
        """

        print("Create symbols...")
        create_symbols(loop=loop)

        print("Updating symbol details...")
        symbol_data_to_postgres(dbsql=dbsql, loop=loop)

        print("Creating commissions...")
        create_commissions(loop=loop)

        print("Performance...")
        path_to = join(settings.DATA_PATH, 'systems')
        filenames = multi_filenames(path_to_history=path_to)
        generate_performance(loop=loop, filenames=filenames)
        """

        print("Generating indexes...")
        generate_qindex(loop=loop)
        """

        print("Generating stats...")
        generate_stats(loop=loop)

        print("Generating keys...")
        generate_keys(loop=loop)

        if not settings.DEV_ENV:
            print("Publishing to Facebook...")
            face_publish()
            print("Publishing to Twitter...")
            post_tweets()
            heatmap_to_facebook()
            heatmap_to_twitter()

        print("Processing GARCH...")
        garch(loop=loop)
        garch_to_db(loop=loop)
        """

        print("Daily tasks: %s seconds ---" % (time() - start_time))

    if args.monthly:
        start_time = time()

        print("Correlations...")
        generate_correlations(loop=loop)

        print("Heatmaps...")
        generate_monthly_heatmaps(loop=loop)

        print("Strategy imgs...")
        make_images(loop=loop)

        print("Images urls to db...")
        process_urls_to_db(loop=loop)

        print("Monthly tasks: %s seconds ---" % (time() - start_time))

    if args.csv:
        pickle_to_svc(folder=args.csv, loop=loop)

    #if args.minvar:
        #min_variance(loop=loop)

    if args.setup:
        dbsql = mysql_connect_db()
        if args.setup=='drop':
            drop_db(db_obj=dbsql)
        mysql_setup(db_obj=dbsql)
        mysql_tables(db_obj=dbsql)
        create_periods()
        create_folders()
        generate_remote_files(loop=loop)
        create_symbols(loop=loop)

    if args.mc:
        mc(loop=loop)

    loop.close()

main()

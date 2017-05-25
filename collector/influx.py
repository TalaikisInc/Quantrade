from django.conf import settings

from influxdb import DataFrameClient

#TODO dropped

def connect(host=settings.INFLUX_HOST, port=settings.INFLUX_PORT):
    user = settings.INFLUX_USER
    password = settings.INFLUX_PASSWORD
    dbname = settings.INFLUX_DBASE

    client = DataFrameClient(host, port, user, password, dbname)

    return client


def create_database(dbname):
    client = connect()
    client.create_database(dbname)


def df_to_influx(df, name):
    client = connect()
    client.write_points(df, name)


def df_to_influx_tags(df, name):
    client = connect()
    client.write_points(df, name, {'k1': 'v1', 'k2': 'v2'})


def read_df(name):
    client = connect()
    client.query("select * from {0}".format(name))


def show_series():
    client = connect()
    client.query("SHOW SERIES")


def show_db():
    client = connect()
    client.query("SHOW DATABASES")


def delete_database(dbname):
    client = connect()
    client.delete_database(dbname)


def influx_inserter(symbol, period, res):
    from .influx import connect, df_to_influx

    res.index = pd.to_datetime(res.index)

    client = connect()

    df_to_influx(client, res, "{0}{1}".format(symbol.symbol, period.period))

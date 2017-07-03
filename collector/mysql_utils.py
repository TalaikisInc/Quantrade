import MySQLdb
from numpy import power
from clint.textui import colored

from django.conf import settings

from .models import QtraUser


def mysql_connect_db():
    """
    Connection object to MySQL database.
    """
    db_obj = None
    try:
        db_obj = MySQLdb.connect(host=settings.MYSQL_HOST, user=settings.MYSQL_USERNAME, \
            passwd=settings.MYSQL_PASSWORD, port=settings.MYSQL_PORT)
    except Exception as err:
        print(colored.red(err))

    if db_obj is None:
        raise ValueError("[-] Not connected to MySQL database.")

    return db_obj


def mysql_setup(db_obj):
    """
    Creates project's database.
    """

    try:
        cursor = db_obj.cursor()

        query = "CREATE DATABASE admin_quantrade CHARACTER SET utf8 COLLATE utf8_general_ci"
        cursor.execute(query)
        print(colored.green("Database created."))
    except Exception as err:
        print(colored.red("At mysql_setup {}".format(err)))


def mysql_tables(db_obj):
    """
    Creates project's tables.
    """

    cursor = db_obj.cursor()

    queries = ["USE {0};".format(settings.MYSQL_DATABASE), \
            "CREATE TABLE IF NOT EXISTS collector_symbols (id bigint(20) NOT NULL AUTO_INCREMENT,\
            symbol varchar(20) NOT NULL,\
            description varchar(120) DEFAULT NULL,\
            spread decimal(10,5) DEFAULT NULL,\
            tick_value decimal(10,5) DEFAULT NULL,\
            tick_size decimal(8,5) DEFAULT NULL,\
            margin_initial decimal(10,2) DEFAULT NULL,\
            digits decimal(8,5) DEFAULT NULL,\
            profit_calc int(2) DEFAULT NULL,\
            profit_currency varchar(10) DEFAULT NULL,\
            price_at_calc_time decimal(20,5) DEFAULT NULL,\
            commission decimal(15,5) DEFAULT NULL,\
            broker varchar(40) NOT NULL,\
            points decimal(8,5) DEFAULT NULL,\
            PRIMARY KEY (id), UNIQUE KEY symbol (symbol) )\
            ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;", \
            "ALTER TABLE collector_symbols ADD UNIQUE (symbol, broker);", \
            "CREATE TABLE IF NOT EXISTS collector_signals (id bigint(20) NOT NULL AUTO_INCREMENT,\
            email varchar(100) NOT NULL,\
            _key varchar(100) NOT NULL,\
            broker varchar(40) NOT NULL,\
            symbol varchar(20) NOT NULL,\
            period int NOT NULL, \
            system varchar(20) NOT NULL,\
            date_time timestamp NOT NULL,\
            _signal int NOT NULL, \
            PRIMARY KEY (id) )\
            ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;", \
            "ALTER TABLE collector_signals ADD UNIQUE (symbol, broker, date_time, period, system);"]

    if settings.USE_MYSQL_DATA:
        queries += ["CREATE TABLE IF NOT EXISTS collector_data (id bigint(20) NOT \
            NULL AUTO_INCREMENT,\
            date_time timestamp NOT NULL,\
            symbol varchar(20) NOT NULL,\
            open double(15,5) NOT NULL,\
            high double(15,5) NOT NULL,\
            low double(15,5) NOT NULL,\
            close double(15,5) NOT NULL,\
            period smallint NOT NULL, \
            volume int NOT NULL, \
            broker varchar(20) NOT NULL,\
            PRIMARY KEY (id) )\
            ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8", \
            "ALTER TABLE collector_data ADD UNIQUE (date_time, symbol, period);"]

    for query in queries:
        cursor.execute(query)

    print(colored.green("Created tables."))


def _signals_to_mysql(db_obj, data_frame, portfolio, user, direction):
    """
    Register signals inside MySQL for further access purposes via MT4 EA/indicators.
    """

    cursor = db_obj.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    cursor.execute(query)

    usr = QtraUser.objects.filter(username=user).values('email', 'key')

    for i in range(len(data_frame.index)):
        try:
            if direction == 1:
                if data_frame.ix[i].BUY_SIDE == 1.0:
                    query = "INSERT INTO collector_signals (id, email, _key, broker, symbol, \
                        period, system, date_time, _signal) VALUES (NULL, '{0}', '{1}', '{2}', \
                        '{3}', {4}, '{5}', '{6}', {7});".format(\
                        usr[0]['email'], \
                        usr[0]['key'], \
                        portfolio['symbol__broker__title'], \
                        portfolio['symbol__symbol'], \
                        portfolio['period__period'], \
                        portfolio['system__title'], \
                        data_frame.ix[i].name.to_pydatetime(), \
                        direction)
                    cursor.execute(query)
                    db_obj.commit()
                    print(colored.green("Signal saved to MySQL."))

            if direction == 2:
                if data_frame.ix[i].SELL_SIDE == 1.0:
                    query = "INSERT INTO collector_signals (id, email, _key, broker, symbol, \
                        period, system, date_time, _signal) VALUES (NULL, '{0}', '{1}', '{2}', \
                        '{3}', {4}, '{5}', '{6}', {7});".format(\
                        usr[0]['email'], \
                        usr[0]['key'], \
                        portfolio['symbol__broker__title'], \
                        portfolio['symbol__symbol'], \
                        portfolio['period__period'], \
                        portfolio['system__title'], \
                        data_frame.ix[i].name.to_pydatetime(), \
                        direction)
                    cursor.execute(query)
                    db_obj.commit()
                    print(colored.green("Signal saved to MySQL."))
        except Exception as err:
            print(colored.red(err))


def create_symbol(name, broker):
    """
    Creates ymbol.
    """

    try:
        db_obj = mysql_connect_db()
        cursor = db_obj.cursor()

        query = "USE {0};".format(settings.MYSQL_DATABASE)
        cursor.execute(query)

        if settings.SHOW_DEBUG:
            print("At MySQL, broker is {}".format(broker))

        query = "INSERT INTO collector_symbols (id, symbol, description, spread, tick_value, \
            tick_size, margin_initial, digits, broker) VALUES (NULL, '{0}', NULL, NULL, NULL, \
            NULL, NULL, NULL, '{1}');".format(name, broker)
        cursor.execute(query)
        db_obj.commit()
        print(colored.green("Created symbol in MySQL."))
    except Exception as err:
        if not ('1062' in str(err)):
            print(colored.red("MySQL: {0}".format(err)))


def drop_db(db_obj):
    """
    Drops database.
    """
    cursor = db_obj.cursor()

    query = "DROP DATABASE admin_quantrade;"
    cursor.execute(query)


def create_commission(db_obj, value, symbol):
    """
    Creates symbol commision for MySQL.
    """
    cursor = db_obj.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    cursor.execute(query)

    query = "UPDATE collector_symbols SET commission={0} WHERE symbol = '{1}';".\
        format(value, symbol)
    cursor.execute(query)
    db_obj.commit()


def get_currency(db_obj, res):
    """
    Get currency rate (MySQL).
    """

    return 1


def get_commission(db_obj, symbol):
    """
    Gets symbol commision from MySQL.
    """
    cursor = db_obj.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    cursor.execute(query)

    query = "SELECT spread, tick_size, tick_value, digits, profit_currency, profit_calc \
        FROM collector_symbols WHERE symbol = '{0}' LIMIT 1;".format(symbol)
    cursor.execute(query)
    res = cursor.fetchone()

    try:
        curr = get_currency(db_obj=db_obj, res=res)
        if curr != 0:
            value = (((power(10.0, -float(res[3])) * float(res[0])) / float(res[1])) * \
                float(res[2])) * curr
        else:
            value = (((power(10.0, -float(res[3])) * float(res[0])) / float(res[1])) * \
                float(res[2]))
    except Exception as err:
        print(colored.red("Generating commission value: {0}".format(err)))
        value = 0.0

    print(colored.yellow("Commission {0} for {1}".format(value, symbol)))

    return value


def get_symbols():
    """
    Gets all symbols from MySQL database.
    """

    db_obj = mysql_connect_db()

    cursor = db_obj.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    cursor.execute(query)

    cursor.execute("SELECT symbol, description, spread, tick_value, tick_size, margin_initial, \
        digits, price_at_calc_time, profit_currency, broker, commission FROM collector_symbols")
    res = cursor.fetchall()

    return res


def get_data(symbol, period):
    """
    Get symvol OHLC from MySQL. It is from version, not used.
    """

    db_obj = mysql_connect_db()

    cursor = db_obj.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    cursor.execute(query)

    cursor.execute("SELECT * FROM collector_data WHERE symbol='{0}' AND period={1} ORDER \
        BY date_time ASC;".format(symbol, period))

    return cursor.fetchall()


def get_symbols_from_datamodel():
    """
    Get symbols from MySQL data table. First we have OHLC data, not symbols,
    so we then create them from this table.
    """

    db_obj = mysql_connect_db()

    cursor = db_obj.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    cursor.execute(query)

    cursor.execute("SELECT DISTINCT(symbol) FROM collector_data;")
    res = cursor.fetchall()

    return res


def adjustment_bureau(data, symbol):
    """
    Adjust Close in different currencies (MySQL).
    """

    db_obj = mysql_connect_db()

    cursor = db_obj.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    cursor.execute(query)

    query = "SELECT spread, tick_size, tick_value, digits, profit_currency, profit_\
        calc FROM collector_symbols WHERE symbol = '{0}' LIMIT 1;".format(symbol)
    cursor.execute(query)
    res = cursor.fetchone()

    try:
        curr = get_currency(db_obj=db_obj, res=res)
        if curr != 0:
            value = (data / float(res[1])) * float(res[2]) * curr
        else:
            value = (data / float(res[1])) * float(res[2])
    except ():
        value = data

    return value

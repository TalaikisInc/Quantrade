import MySQLdb
from numpy import power
from clint.textui import colored

from django.conf import settings

from .models import QtraUser


def mysql_connect_db():
    try:
        db = MySQLdb.connect(host=settings.MYSQL_HOST, user=settings.MYSQL_USERNAME, passwd=settings.MYSQL_PASSWORD, port=settings.MYSQL_PORT)
        return db
    except Exception as e:
        print(e)


def mysql_setup(db):
    try:
        c = db.cursor()

        query = "CREATE DATABASE admin_quantrade CHARACTER SET utf8 COLLATE utf8_general_ci"
        c.execute(query)
        print("Database created.")
    except Exception as e:
        print(e)


def mysql_tables(db):
    c = db.cursor()

    query = "USE {0};".format(settings.MYSQL_DATABASE)
    c.execute(query)

    query = "CREATE TABLE IF NOT EXISTS collector_symbols (id bigint(20) NOT NULL AUTO_INCREMENT,\
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
            ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8"
    c.execute(query)

    query = "ALTER TABLE collector_symbols ADD UNIQUE (symbol, broker);"
    c.execute(query)
    print(colored.green("Created quantrade_symbols table."))

    query = "CREATE TABLE IF NOT EXISTS collector_signals (id bigint(20) NOT NULL AUTO_INCREMENT,\
            email varchar(100) NOT NULL,\
            _key varchar(100) NOT NULL,\
            broker varchar(40) NOT NULL,\
            symbol varchar(20) NOT NULL,\
            period int NOT NULL, \
            system varchar(20) NOT NULL,\
            date_time timestamp NOT NULL,\
            _signal int NOT NULL, \
            PRIMARY KEY (id) )\
            ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;"
    c.execute(query)
    query = "ALTER TABLE collector_signals ADD UNIQUE (symbol, broker, date_time, period, system);"
    c.execute(query)
    print(colored.green("Created quantrade_signals table."))

    #query = "CREATE TABLE IF NOT EXISTS collector_data (id bigint(20) NOT NULL AUTO_INCREMENT,\
            #date_time timestamp NOT NULL,\
            #symbol varchar(20) NOT NULL,\
            #open double(15,5) NOT NULL,\
            #high double(15,5) NOT NULL,\
            #low double(15,5) NOT NULL,\
            #close double(15,5) NOT NULL,\
            #period smallint NOT NULL, \
            #volume int NOT NULL, \
            #broker varchar(20) NOT NULL,\
            #PRIMARY KEY (id) )\
            #ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8"
    #c.execute(query)
    #query = "ALTER TABLE collector_data ADD UNIQUE (date_time, symbol, period);"
    #c.execute(query)


def _signals_to_mysql(db, df, p, user, direction):
    c = db.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    c.execute(query)

    usr = QtraUser.objects.filter(username=user).values('email', 'key')

    for i in range(0,len(df.index)):
        try:
            if direction == 1:
                if df.ix[i].BUY_SIDE == 1.0:
                    query = "INSERT INTO collector_signals (id, email, _key, broker, symbol, period, system, date_time, _signal) VALUES (NULL, '{0}', '{1}', '{2}', '{3}', {4}, '{5}', '{6}', {7});".format(\
                        usr[0]['email'], \
                        usr[0]['key'], \
                        p['symbol__broker__title'], \
                        p['symbol__symbol'], \
                        p['period__period'], \
                        p['system__title'], \
                        df.ix[i].name.to_pydatetime(), \
                        direction)
                    c.execute(query)
                    db.commit()
                    print(colored.green("Signal saved to MySQL."))

            if direction == 2:
                if df.ix[i].SELL_SIDE == 1.0:
                    query = "INSERT INTO collector_signals (id, email, _key, broker, symbol, period, system, date_time, _signal) VALUES (NULL, '{0}', '{1}', '{2}', '{3}', {4}, '{5}', '{6}', {7});".format(\
                        usr[0]['email'], \
                        usr[0]['key'], \
                        p['symbol__broker__title'], \
                        p['symbol__symbol'], \
                        p['period__period'], \
                        p['system__title'], \
                        df.ix[i].name.to_pydatetime(), \
                        direction)
                    c.execute(query)
                    db.commit()
                    print(colored.green("Signal saved to MySQL."))
        except Exception as e:
            print(colored.red(e))
            continue


def create_symbol(name, broker):
    try:
        db = mysql_connect_db()
        c = db.cursor()

        query = "USE {0};".format(settings.MYSQL_DATABASE)
        c.execute(query)

        if settings.SHOW_DEBUG:
            print("At MySQL {}".format(broker))

        query = "INSERT INTO collector_symbols (id, symbol, description, spread, tick_value, tick_size, margin_initial, digits, broker) VALUES (NULL, '{0}', NULL, NULL, NULL, NULL, NULL, NULL, '{1}');".format(name, broker)
        c.execute(query)
        db.commit()
        print(colored.green("Created symbol in MySQL."))
    except Exception as e:
        if not ('1062' in str(e)):
            print(colored.red("MySQL: {0}".format(e)))


def drop_db(db):
    c = db.cursor()

    query = "DROP DATABASE admin_quantrade;"
    c.execute(query)


def create_commission(db, value, symbol):
    c = db.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    c.execute(query)

    query = "UPDATE collector_symbols SET commission={0} WHERE symbol = '{1}';".format(value, symbol)
    c.execute(query)
    db.commit()

def get_commission(db, symbol):
    c = db.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    c.execute(query)

    query = "SELECT spread, tick_size, tick_value, digits, profit_currency, profit_calc FROM collector_symbols WHERE symbol = '{0}' LIMIT 1;".format(symbol)
    c.execute(query)
    res = c.fetchone()

    try:
        curr = get_currency(dbm=dbm, res=res)
        if curr != 0:
            value = (((power(10.0, -float(res[3])) * float(res[0])) / float(res[1])) * float(res[2])) * curr
        else:
            value = (((power(10.0, -float(res[3])) * float(res[0])) / float(res[1])) * float(res[2]))
    except Exception as e:
        print(colored.red("Generating commission value: {0}".format(e)))
        value = 0.0

    print(colored.yellow("Commission {0} for {1}".format(value, symbol)))

    return value


def get_symbols():
    db = mysql_connect_db()

    c = db.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    c.execute(query)

    c.execute("SELECT symbol, description, spread, tick_value, tick_size, margin_initial, digits, price_at_calc_time, profit_currency, broker, commission FROM collector_symbols")
    res = c.fetchall()

    return res


def get_data(symbol, period):
    db = mysql_connect_db()

    c = db.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    c.execute(query)

    c.execute("SELECT * FROM collector_data WHERE symbol='{0}' AND period={1} ORDER BY date_time ASC;".format(symbol, period))
    res = c.fetchall()

    return res


def get_symbols_from_datamodel():
    db = mysql_connect_db()

    c = db.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    c.execute(query)

    c.execute("SELECT DISTINCT(symbol) FROM collector_data;")
    res = c.fetchall()

    return res


def adjustment_bureau(data, symbol):
    db = mysql_connect_db()

    c = db.cursor()
    query = "USE {0};".format(settings.MYSQL_DATABASE)
    c.execute(query)

    query = "SELECT spread, tick_size, tick_value, digits, profit_currency, profit_calc FROM collector_symbols WHERE symbol = '{0}' LIMIT 1;".format(symbol)
    c.execute(query)
    res = c.fetchone()

    try:
        curr = get_currency(res)
        if curr != 0:
            value = (data / float(res[1])) * float(res[2]) * curr
        else:
            value = (data / float(res[1])) * float(res[2])
    except:
        value = data

    return value

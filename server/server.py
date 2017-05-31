from datetime import (datetime, timedelta)
from functools import lru_cache

import asyncpg
from sanic import Sanic
from sanic.response import (json, html)
from sanic.blueprints import Blueprint
from sanic.exceptions import (InvalidUsage, ServerError, NotFound)
from aoiklivereload import LiveReloader

from django.template.defaultfilters import slugify
from django.utils.encoding import force_text
from django.core import serializers

import settings

#TODO refactor all this

async def conn():
    con = await asyncpg.connect(user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD, database=settings.DATABASE_NAME,
        host=settings.DATABASE_HOST, statement_cache_size=2000)
    return con


REDIRECT_HTML = """
<html xmlns="http://www.w3.org/1999/xhtml">
 <head>
   <title>Quantrade API</title>
   <meta http-equiv="refresh" content="0;URL='https://quantrade.co.uk/api/'" />
 </head>
 <body>
 </body>
</html>
"""

app = Sanic(__name__)

DOMAIN = settings.API_HOST

reloader = LiveReloader()
reloader.start_watcher_thread()


async def jsonify(records):
    return [dict(r.items()) for r in records]


@lru_cache(maxsize=None)
@app.route("/stats/<broker_slug>/<symbol>/<period>/<strategy>/<direction>/", host=DOMAIN, methods=["GET"])
async def api_stats(request, broker_slug, symbol, period, strategy, direction):
    try:
        from collector.models import Stats

        broker_slug = force_text(broker_slug, encoding='utf-8', strings_only=True, errors='strict')
        symbol = force_text(symbol, encoding='utf-8', strings_only=True, errors='strict')
        period = force_text(period, encoding='utf-8', strings_only=True, errors='strict')
        strategy = force_text(strategy, encoding='utf-8', strings_only=True, errors='strict')
        direction = force_text(direction, encoding='utf-8', strings_only=True, errors='strict')
        stats = Stats.objects.filter(broker__slug=broker_slug, symbol__symbol=symbol, period__period=period, system__title=strategy, direction=direction)

        #con = await conn()
        #systems = await con.fetch('''SELECT * FROM collector_stats WHERE ''')
        #data = [{'id': s['id'], 'title': s['title'], 'description': s['description']} for s in systems]
        #await con.close()

        return json({'data': stats})
    except:
        raise NotFound("Not foud.")


@lru_cache(maxsize=None)
async def slugifier(what):
    str = str(slugify(what)).replace('-', '_')
    return str


@lru_cache(maxsize=None)
@app.route("/indexes/ai50/<broker_slug>/components/", host=DOMAIN, methods=["GET"])
async def api_ai50(request, broker_slug):
    try:
        from collector.tasks import qindex

        broker_slug = force_text(broker_slug, encoding='utf-8', strings_only=True, errors='strict')
        s = qindex(broker=broker_slug)

        return json({'data': s})
    except:
        raise NotFound("Not foud.")


@lru_cache(maxsize=None)
@app.route("/indexes/ai50/<broker_slug>/latest/<days>/", host=DOMAIN, methods=["GET"])
async def api_ai50_latest(request, days, broker_slug):
    try:
        from collector.models import Signals

        days = force_text(days, encoding='utf-8', strings_only=True, errors='strict')
        dlt = datetime.now() - timedelta(days=int(days))
        broker_slug = force_text(broker_slug, encoding='utf-8', strings_only=True, errors='strict')

        s = Signals.objects.filter(user__id=1, broker__slug=broker_slug, date_time__gte=dlt).order_by('date_time').reverse()

        data = serializers.serialize('json', s, fields=("date_time", "broker__title", \
            "symbol__symbol", "period__period", "system__title", "direction"))

        return json({'data': data})
    except:
        raise NotFound("Not foud.")


async def get_limit_from(period):
    if period == '1440':
        limit_from = datetime.now() - timedelta(days=2)
    if period == '10080':
        limit_from = datetime.now() - timedelta(days=7)
    if period == '43200':
        limit_from = datetime.now() - timedelta(days=28)
    return limit_from


@lru_cache(maxsize=None)
@app.route("<broker_slug>/<symbol>/<period>/<strategy>/", host=DOMAIN, methods=["GET"])
async def api_strategy(request, broker_slug, symbol, period, strategy):
    try:
        from collector.tasks import asy_get_df

        broker_slug = force_text(broker_slug, encoding='utf-8', strings_only=True, errors='strict')
        symbol = force_text(symbol, encoding='utf-8', strings_only=True, errors='strict')
        period = force_text(period, encoding='utf-8', strings_only=True, errors='strict')
        strategy = force_text(strategy, encoding='utf-8', strings_only=True, errors='strict')

        df = await asy_get_df(con=await conn(), broker_slug=broker_slug, symbol=symbol, period=period, system=strategy, folder='systems', limit=False)
        try:
            del df['PCT']
            del df['hc']
            del df['cl']
            del df['VALUE']
            del df['DIFF']
        except:
            pass

        limit_from = await get_limit_from(period=period)
        df = df.loc[df.index < limit_from]
        data = {
            'data': df.to_dict(orient='index')
        }
        return json(data)
    except:
        raise NotFound("Not foud.")


@lru_cache(maxsize=None)
@app.route("/indexes/ai50/<broker_slug>/results/", host=DOMAIN, methods=["GET"])
async def autoportfolio_index(request, broker_slug):
    try:
        from collector.tasks import read_df

        broker_slug = force_text(broker_slug, encoding='utf-8', strings_only=True, errors='strict')

        filename = join(settings.DATA_PATH, 'portfolios', '{}_qndx.mp'.format(broker_slug))
        df = await read_df(filename=filename)

        #limit_from = await get_limit_from(period=period)
        #df = df.loc[df.index < limit_from]
        data = {
            'data': df.to_dict(orient='index')
        }
        return json(data)
    except:
        raise NotFound("Not foud.")


@lru_cache(maxsize=None)
@app.route("<broker_slug>/<symbol>/<period>/<strategy>/<key>/", host=DOMAIN, methods=["GET"])
async def api_strategy(request, broker_slug, symbol, period, strategy, key):
    try:
        from collector.tasks import asy_get_df

        broker_slug = force_text(broker_slug, encoding='utf-8', strings_only=True, errors='strict')
        symbol = force_text(symbol, encoding='utf-8', strings_only=True, errors='strict')
        period = force_text(period, encoding='utf-8', strings_only=True, errors='strict')
        strategy = force_text(strategy, encoding='utf-8', strings_only=True, errors='strict')

        df = await asy_get_df(con=await conn(), broker_slug=broker_slug, symbol=symbol, period=period, system=strategy, folder='systems', limit=False)
        try:
            del df['PCT']
            del df['hc']
            del df['cl']
            del df['VALUE']
            del df['DIFF']
        except:
            pass

        if key:
            try:
                from collector.models import QtraUser
                key = force_text(key, encoding='utf-8', strings_only=True, errors='strict')
                user = QtraUser.objects.filter(key=key)
                if not ((user[0].user_type == 1) & (len(user) > 0)):
                    limit_from = await get_limit_from(period=period)
                    df = df.loc[df.index < limit_from]
            except:
                limit_from = await get_limit_from(period=period)
                df = df.loc[df.index < limit_from]
        else:
            limit_from = await get_limit_from(period=period)
            df = df.loc[df.index < limit_from]

        data = {
            'data': df.to_dict(orient='index')
        }
        return json(data)
    except:
        raise NotFound("Not foud.")


@lru_cache(maxsize=None)
@app.route("/", host=DOMAIN, methods=["GET"])
async def home(request):
    return html(REDIRECT_HTML)


@lru_cache(maxsize=None)
@app.route("/symbols/", host=DOMAIN, methods=["GET"])
async def symbols(request):
    try:
        con = await conn()
        symbols = await con.fetch('''SELECT * FROM collector_symbols''')
        await con.close()
        return json({'symbols': await jsonify(symbols)})
    except:
        raise NotFound("Not foud.")


@lru_cache(maxsize=None)
@app.route("/brokers/", host=DOMAIN, methods=["GET"])
async def brokers(request):
    try:
        con = await conn()
        brokers = await con.fetch('''SELECT * FROM collector_brokers''')
        await con.close()
        return json({'brokers': await jsonify(brokers)})
    except:
        raise NotFound("Not foud.")


@lru_cache(maxsize=None)
@app.route("/systems/", host=DOMAIN, methods=["GET"])
async def systems(request):
    try:
        con = await conn()
        systems = await con.fetch('''SELECT * FROM collector_systems''')
        await con.close()
        return json({'systems': await jsonify(systems)})
    except:
        raise NotFound("Not foud.")


@lru_cache(maxsize=None)
@app.route("/periods/", host=DOMAIN, methods=["GET"])
async def periods(request):
    try:
        con = await conn()
        periods = await con.fetch('''SELECT * FROM collector_periods''')
        await con.close()
        return json({'periods': await jsonify(periods)})
    except:
        raise NotFound("Not foud.")

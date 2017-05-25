import argparse

from aiohttp import web
from aiohttp_wsgi import WSGIHandler

from django.conf import settings

from quantrade.wsgi import application


#pidfile = '/var/run/aio.pid'
parser = argparse.ArgumentParser(description="Aiohttp server")
parser.add_argument('--path')
parser.add_argument('--port')
args = parser.parse_args()

wsgi_handler = WSGIHandler(application)
app = web.Application()
app.router.add_route("*", "/{path_info:.*}", wsgi_handler)
app.make_handler(access_log=None)

if settings.DEBUG:
    web.run_app(app, host=('0.0.0.0',), port=8000)
else:
    web.run_app(app, host=('127.0.0.1',), port=int(args.port), access_log=None)
    #web.run_app(app, host=('127.0.0.1',), port=8000, access_log=None)

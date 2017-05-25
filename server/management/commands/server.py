from django.core.management.base import BaseCommand, CommandError

from django.conf import settings

import server
from server import app


class Command(BaseCommand):
    help = 'Serve Django over Sanic/ uvloop.'

    if settings.DEV_ENV:
        START_PORT = 8000
        API_HOST = '0.0.0.0'
    else:
        START_PORT = 8000+settings.WORKERS
        API_HOST = '127.0.0.1'

    try:
        #TODO multiple workers
        #for i in range(settings.API_WORKERS):
        #app.run(host=API_HOST, port=START_PORT+i, debug=settings.DEBUG)
        app.run(host=API_HOST, port=START_PORT, debug=settings.DEBUG)
        #to kill first find the pid:
        #sudo lsof -t -i:8003
    except KeyboardInterrupt:
        pass

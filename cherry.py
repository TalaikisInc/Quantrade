#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os import environ, path
import webbrowser
from threading import Timer

environ["DJANGO_SETTINGS_MODULE"] = "quantrade.settings"

tdir = path.abspath(path.dirname(__file__))

import cherrypy
import django
django.setup()
from django.conf import settings
from django.core.handlers.wsgi import WSGIHandler

#TODO not used, expeimental

class DjangoApplication(object):
    HOST = "127.0.0.1"
    PORT = 8001
    PORT1 = 8002
    PORT2 = 8003

    def mount_static(self, url, root):
        """
        :param url: Relative url
        :param root: Path to static files root
        """
        config = {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': root,
            'tools.expires.on': True,
            'tools.expires.secs': 600
            #'server.ssl_module': 'builtin',
            #'server.ssl_certificate': path.join(tdir, '../../../nginx/conf/certs/fullchain.pem'),
            #'server.ssl_private_key': path.join(tdir, '../../../nginx/conf/certs/privkey.pem'),
        }
        cherrypy.tree.mount(None, url, {'/': config})

    def open_browser(self):
        Timer(3, webbrowser.open, ("http://%s:%s" % (self.HOST, self.PORT),)).start()

    def run(self):
        if settings.DEV_ENV:
            self.mount_static(settings.STATIC_URL, settings.STATIC_ROOT)

        cherrypy.log("Loading and serving Django application")
        cherrypy.tree.graft(WSGIHandler())
        cherrypy.server.unsubscribe()

        cherrypy.config.update({
            'log.error_file': 'logs/cherypy.log',
            'tools.log_tracebacks.on': True,
            'log.screen': False,
            'engine.autoreload_on': True
        })

        server = cherrypy._cpserver.Server()
        server.threads = 40
        server.socket_host = self.HOST
        server.socket_port = self.PORT
        server.subscribe()

        server1 = cherrypy._cpserver.Server()
        server1.threads = 40
        server1.socket_host = self.HOST
        server1.socket_port = self.PORT1
        server1.subscribe()

        server2 = cherrypy._cpserver.Server()
        server2.threads = 40
        server2.socket_host = self.HOST
        server2.socket_port = self.PORT2
        server2.subscribe()

        #start servers
        cherrypy.engine.start()
        self.open_browser()
        cherrypy.engine.block()


if __name__ == "__main__":
    print "Your app is running at http://localhost:8001"
    DjangoApplication().run()

import sys
from os import environ
from os.path import (dirname, join, abspath)
from subprocess import call

from clint.textui import colored
from dotenv import load_dotenv


BASE_DIR = dirname(dirname(abspath(__file__)))
load_dotenv(join(BASE_DIR, '.env'))
name = environ.get("SITE_FOLDER")
app = environ.get("APP_NAME")


def create_dir(name):
    call(["mkdir", "/home/{}/uwsgi/uwsgi".format(name)])


def upstart_writer(name):
    cfg = "description \"uWSGI Emperor Starter\"\nstart on runlevel [2345]\nstop on runlevel [!2345]\n\nrespawn\n\nexec /bin/bash /home/{0}/uwsgi/uwsgi.sh".format(name)

    with open('/etc/init/{}.conf'.format(name), 'w') as f:
        f.write(cfg)
        print(colored.green("Wrote uWsgi Upstart script for {}.".format(name)))


def emperor_writer(name):
    cfg = "[uwsgi]\nfolder = {0}\n\nemperor = /home/%(folder)/uwsgi/uwsgi\ndisable-logging = true\nlogto = /home/%(folder)/logs/uwsgi_emperor.log\nuid = www-data\ngid = www-data\nvhost = true".format(name)

    with open('/home/{}/uwsgi/emperor.ini'.format(name), 'w') as f:
        f.write(cfg)
        print(colored.green("Wrote uWsgi Emperor config for {}.".format(name)))


def uwsgi_writer(name, app):
    cfg = "[uwsgi]\nfolder={0}\n\nchdir=/home/%(folder)\nwsgi-file=/home/%(folder)/{1}/wsgi.py\nvirtualenv=/usr/local/anaconda/envs/%(folder)\n\ncheaper-algo = spare\ncheaper = 2\ncheaper-initial = 5\nworkers = 10\ncheaper-step = 1\nprocesses = 10\n\nuid=www-data\ngid=www-data\n\nsocket=/home/%(folder)/uwsgi.sock\nchmod-socket=660\nchown-socket=www-data:www-data\n\n#harakiri=10\nthunder-lock=true\nvaccum=true\ndie-on-term=true\nenable-threads=true\n\npidfile=/tmp/%(folder).pid\ndisable-logging = true\nlogto=/home/%(folder)/logs/uwsgi.log".format(name, app)

    with open('/home/{}/uwsgi/uwsgi/uwsgi.ini'.format(name), 'w') as f:
        f.write(cfg)
        print(colored.green("Wrote uWsgi config for {}.".format(name)))


def uwsgi_bash_writer(name):
    cfg = "#!/bin/bash\n\n\nPROJECT={0}\n\n\nexport LD_LIBRARY_PATH=/usr/local/anaconda/lib:$LD_LIBRARY_PATH\n\nsource /usr/local/anaconda/bin/activate $PROJECT && /usr/local/anaconda/envs/$PROJECT/bin/uwsgi --ini /home/$PROJECT/uwsgi/emperor.ini\n\nsource /usr/local/anaconda/bin/deactivate".format(name)

    with open('/home/{}/uwsgi/uwsgi.sh'.format(name), 'w') as f:
        f.write(cfg)
        print(colored.green("Wrote uWsgi bash script for {}.".format(name)))


def chmod_uwsgi_bash(name):
    call(["chmod", "+x", "/home/{}/uwsgi/uwsgi.sh".format(name)])


def fix_permissions(name):
    call(["/bin/chown", "-R", "www-data:www-data", "/home/{}/uwsgi".format(name)])


def start_uwsgi(name):
    call(["/sbin/start", "{}".format(name)])


#TODO better than this
create_dir(name=name)
upstart_writer(name=name)
emperor_writer(name=name)
uwsgi_writer(name=name, app=app)
uwsgi_bash_writer(name=name)
chmod_uwsgi_bash(name=name)
fix_permissions(name=name)
start_uwsgi(name=name)

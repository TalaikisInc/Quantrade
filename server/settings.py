from os.path import (dirname, join)
from os import environ

import django
django.setup()
from django.conf improt settings

from dotenv import load_dotenv


BASE_DIR = dirname(dirname(__file__))
load_dotenv(join(BASE_DIR, '.env'))

DEV_ENV = int(environ.get("DEV_ENV"))
if DEV_ENV:
    HOST = environ.get("DEV_API_HOST")
    PORT = int(environ.get("DEV_API_PORT"))
    DATABASE_HOST = environ.get("DATABASE_HOST")
    DATABASE_USER = environ.get("DATABASE_USER")
    DATABASE_NAME = environ.get("DATABASE_NAME")
    DATABASE_PASSWORD = environ.get("DATABASE_PASSWORD")
else:
    HOST = environ.get("API_HOST")
    PORT = int(environ.get("API_PORT"))
    DATABASE_HOST = environ.get("DEV_DATABASE_HOST")
    DATABASE_USER = environ.get("DEV_DATABASE_USER")
    DATABASE_NAME = environ.get("DEV_DATABASE_NAME")
    DATABASE_PASSWORD = environ.get("DEV_DATABASE_PASSWORD")
DEBUG = int(environ.get("DEBUG"))
API_WORKERS = int(environ.get("API_WORKERS"))

DATA_PATH = settings.DATA_PATH

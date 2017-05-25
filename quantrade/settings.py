from os.path import (join, dirname, isdir)
from os import environ
from datetime import datetime
import logging

import psycopg2
from dotenv import load_dotenv

from django.utils.translation import ugettext_lazy as T
from django.template.defaultfilters import slugify

BASE_DIR = dirname(dirname('__file__'))
BASE_PATH = dirname(BASE_DIR)
load_dotenv(join(BASE_PATH, '.env'))

DEV_ENV  = int(environ.get("QT_DEV_ENV"))
VAGRANT = int(environ.get("QT_VAGRANT"))

TEMPLATE_NAME = environ.get("TEMPLATE_NAME")

#TODO remove with uwsgi
WORKERS = 3
API_WORKERS = 3
FOLDER = 'quantrade'
MEDIA_ROOT = join(BASE_PATH, "uploads")
STATIC_ROOT = join(BASE_PATH, "static")

if DEV_ENV:
    BASE_URL = 'http://127.0.0.1:8001/'
else:
    BASE_URL = 'https://quantrade.co.uk/'

STATICFILES_DIRS = []
MEDIA_URL = '/'
STATIC_URL = '/static/'

STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
#    'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

QUANDL_API_KEY = environ.get("QUANDL_API_KEY")

ADMINS = (
    ("me", "me@me.com")
)

MANAGERS = ADMINS

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.humanize',
    'ckeditor',
    'server',
    'oauth2_provider',
    'django.contrib.sites',
    'django.contrib.flatpages',
    'social.apps.django_app.default',
    'collector',
    'collector.templatetags',
]

LOGIN_URL = 'login'
LOGOUT_URL = 'logout'
LOGIN_REDIRECT_URL = 'settings'
SOCIAL_AUTH_LOGIN_ERROR_URL = '/login_error/'
SOCIAL_AUTH_LOGIN_REDIRECT_URL = '/members/'
SOCIAL_AUTH_RAISE_EXCEPTIONS = False #needed for templating exceptions
SOCIAL_AUTH_SLUGIFY_USERNAMES = True
SOCIAL_AUTH_SESSION_EXPIRATION = False

if not DEV_ENV:
    SOCIAL_AUTH_FORCE_POST_DISCONNECT = True

SOCIAL_AUTH_TWITTER_KEY = environ.get("QT_SOCIAL_AUTH_TWITTER_KEY")
SOCIAL_AUTH_TWITTER_SECRET = environ.get("QT_SOCIAL_AUTH_TWITTER_SECRET")
TWITTER_ACCESS_TOKEN_KEY = environ.get("QT_TWITTER_ACCESS_TOKEN_KEY")
TWITTER_ACCESS_TOKEN_SECRET = environ.get("QT_TWITTER_ACCESS_TOKEN_SECRET")

SOCIAL_AUTH_FACEBOOK_KEY = environ.get("QT_SOCIAL_AUTH_FACEBOOK_KEY")
SOCIAL_AUTH_FACEBOOK_SECRET = environ.get("QT_SOCIAL_AUTH_FACEBOOK_SECRET")
SOCIAL_AUTH_FACEBOOK_EXTRA_DATA = ['first_name', 'last_name', 'email']
SOCIAL_AUTH_FACEBOOK_SCOPE = ['email']
SOCIAL_AUTH_FACEBOOK_PROFILE_EXTRA_PARAMS = {
    'fields': 'id, name, email, age_range'
}

FACEBOOK_PAGE_ID = environ.get("QT_FACEBOOK_PAGE_ID")
FACEBOOK_APP_ID = SOCIAL_AUTH_FACEBOOK_KEY
FACEBOOK_APP_SECRET = SOCIAL_AUTH_FACEBOOK_SECRET
FACEBOOK_PAGE_ACCESS_TOKEN = environ.get("QT_FACEBOOK_PAGE_ACCESS_TOKEN")

SOCIAL_AUTH_GOOGLE_OAUTH2_KEY = environ.get("QT_SOCIAL_AUTH_GOOGLE_OAUTH2_KEY")
SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET = environ.get("QT_SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET")
SOCIAL_AUTH_GOOGLE_OAUTH2_USE_UNIQUE_USER_ID = True
SOCIAL_AUTH_GOOGLE_OAUTH2_AUTH_EXTRA_ARGUMENTS = {
    'access_type': 'offline',
    'approval_prompt': 'force'
}

SOCIAL_AUTH_LINKEDIN_OAUTH2_KEY = environ.get("QT_SOCIAL_AUTH_LINKEDIN_OAUTH2_KEY")
SOCIAL_AUTH_LINKEDIN_OAUTH2_SECRET = environ.get("QT_SOCIAL_AUTH_LINKEDIN_OAUTH2_SECRET")
SOCIAL_AUTH_LINKEDIN_OAUTH2_SCOPE = ['r_basicprofile', 'r_emailaddress', 'rw_company_admin', 'w_share']

SOCIAL_AUTH_DISCONNECT_PIPELINE = (
    'social.pipeline.disconnect.allowed_to_disconnect',
    'social.pipeline.disconnect.get_entries',
    'social.pipeline.disconnect.revoke_tokens',
    'social.pipeline.disconnect.disconnect',
)

SOCIAL_AUTH_PIPELINE = (
    'social.pipeline.social_auth.social_details',
    'social.pipeline.social_auth.social_uid',
    'social.pipeline.social_auth.auth_allowed',
    'social.pipeline.social_auth.social_user',
    'collector.social_profile.redirect_if_no_refresh_token',
    'social.pipeline.user.get_username',
    'social.pipeline.mail.mail_validation',
    'social.pipeline.user.create_user',
    'social.pipeline.social_auth.associate_by_email',  # <--- enable this one
    'social.pipeline.social_auth.associate_user',
    'social.pipeline.social_auth.load_extra_data',
    'social.pipeline.user.user_details',
    'collector.social_profile.get_avatar',
)

AUTHENTICATION_BACKENDS = (
    'django.contrib.auth.backends.ModelBackend',
    'oauth2_provider.ext.rest_framework.OAuth2Authentication',
    'social.backends.twitter.TwitterOAuth',
    'social.backends.facebook.FacebookOAuth2',
    'social.backends.google.GoogleOAuth2',
    'social.backends.linkedin.LinkedinOAuth2',
)

AUTH_USER_MODEL = 'collector.QtraUser'
SOCIAL_AUTH_USER_MODEL = 'collector.QtraUser'

IP = '127.0.0.1'

if VAGRANT:
    IP = '10.0.2.2'

if DEV_ENV:
    CACHES = {
        'default': {
            'BACKEND': 'redis_cache.RedisCache',
            'LOCATION': [
                    '{}:6379'.format(IP),
                    #'11.21.26.29:6379',
                ],
            'OPTIONS': {
                'DB': 1,
                'CONNECTION_POOL_CLASS': 'redis.BlockingConnectionPool',
                'CONNECTION_POOL_CLASS_KWARGS': {
                    'max_connections': 2048,
                    'timeout': 60*60*6,
                },
                'PARSER_CLASS': 'redis.connection.HiredisParser',
                "CLIENT_CLASS": "django_redis.client.DefaultClient",
                'COMPRESSOR_CLASS': 'redis_cache.compressors.ZLibCompressor',
                'COMPRESSOR_CLASS_KWARGS': {
                    'level': 9,  # 0 - 9; 0 - no compression; 1 - fastest, biggest; 9 - slowest, smallest
                },
                'MASTER_CACHE': '{}:6379'.format(IP),
            },
        }
    }
else:
    CACHES = {
        'default': {
            'BACKEND': 'redis_cache.RedisCache',
            'LOCATION': [
                    '{}:6379'.format(IP),
                    #'12.19.14.26:6379',
                ],
            'OPTIONS': {
                'DB': 1,
                'PASSWORD': environ.get("REDIS_PASWORD"),
                'CONNECTION_POOL_CLASS': 'redis.BlockingConnectionPool',
                'CONNECTION_POOL_CLASS_KWARGS': {
                    'max_connections': 2048,
                    'timeout': 60*60*6,
                },
                'PARSER_CLASS': 'redis.connection.HiredisParser',
                "CLIENT_CLASS": "django_redis.client.DefaultClient",
                'COMPRESSOR_CLASS': 'redis_cache.compressors.ZLibCompressor',
                'COMPRESSOR_CLASS_KWARGS': {
                    'level': 9,  # 0 - 9; 0 - no compression; 1 - fastest, biggest; 9 - slowest, smallest
                },
                'MASTER_CACHE': '{}:6379'.format(IP),
            },
        }
    }

CACHE_MIDDLEWARE_KEY_PREFIX = 'quantrade3'
CACHE_MIDDLEWARE_SECONDS = 60*60

DEFAULT_BROKER = slugify('Ava Trade EU Ltd.').replace('-', '_')
SITE_NAME = 'Quantrade trading signals'

UPDATE_DESCRIPTIONS = False

PERIODS = (
    ('129600', 'MN3'),
    ('43200', 'MN1'),
    ('10080', 'W1'),
    ('1440', 'D1'),
    ('240', 'H4'),
    ('60', 'H1'),
    ('30', 'M30'),
    ('15', 'M15'),
    ('5', 'M5'),
    ('1', 'M1'),
)

PUBLIC_PERIODS = PERIODS[1:4]

LOWEST_TF = 1440

if DEV_ENV:
    METATRADER_HISTORY_PATHS = [('Ava Trade EU Ltd.', 'C:\\Users\\Tadas\\AppData\\Roaming\\MetaQuotes\\Terminal\\88A7C6C356B9D73AC70BD2040F0D9829\\MQL4\\Files\\'),
                                ('Pepperstone Group Limited', 'C:\\Users\\Tadas\\AppData\\Roaming\\MetaQuotes\\Terminal\\3294B546D50FEEDA6BF3CFC7CF858DB7\\MQL4\\Files\\')]
else:
    METATRADER_HISTORY_PATHS = [('Ava Trade EU Ltd.', 'C:\\Users\\Administrator\\AppData\\Roaming\\MetaQuotes\\Terminal\\4CEA74113F9B03A091193EB928E38709\\MQL4\\Files\\'),
            ('Pepperstone Group Limited', 'C:\\Users\\Administrator\\AppData\\Roaming\\MetaQuotes\\Terminal\\3294B546D50FEEDA6BF3CFC7CF858DB7\\MQL4\\Files\\'),
        ]

DATA_PATH = join(BASE_DIR, 'data')

WEBDAV_SERVER = environ.get("WEBDAV_SERVER")
WEBDAV_PORTS = str(environ.get("WEBDAV_PORTS")).split(",")
#each webdav port == one broker data folder
WEBDAV_SOURCES = [(WEBDAV_SERVER, WEBDAV_PORTS[0].replace(" ", "")),
    (WEBDAV_SERVER, WEBDAV_PORTS[1].replace(" ", ""))]
WEBDAV_USERNAME = environ.get("WEBDAV_USERNAME")
WEBDAV_PASSWORD = environ.get("WEBDAV_PASSWORD")

DIRECTIONS = (
    (0, 'Long & shorts'),
    (1, 'Longs'),
    (2, 'Shorts'),
    (3, 'A.I. 50')
)

SHARPE = 0.01
MIN_TRADES = 20
WIN_RATE = 0.5
MIN_MACHINE_SHARPE = 0.1
MIN_MACHINE_PORTFOLIO_SHARPE = 0.2
MIN_PORTFOLIO_PUBLIC_SHARPE = 0.3
MIN_PORTFOLIO_PUBLIC_FITNESS = 0.1
MIN_MACHINE_WIN_RATE = 0.55
STRATEGIES_IN_MACHINE_PORTFOLIO = 4
MIN_YEARLY = 2.0
LIMIT_MONTHS = '72M'
LIMIT_ENABLED = True
LIMIT_STRATEGIES_FOR_NON_CUSTOMERS = 5
VERSION = '0.10.55-beta'

MACHINE_USERNAME = 'Quantrade'
FEED_DAYS_TO_SHOW = 90

USER_KEY_SYMBOLS = 31

if DEV_ENV:
    DATABASE_HOST = IP
    DATABASE_NAME = environ.get("QT_DEV_DATABASE_NAME")
    DATABASE_USER = environ.get("QT_DEV_DATABASE_USER")
    DATABASE_PASSWORD = environ.get("QT_DEV_DATABASE_PASSWORD")
else:
    DATABASE_HOST = environ.get("QT_DATABASE_HOST")
    DATABASE_NAME = environ.get("QT_DATABASE_NAME")
    DATABASE_USER = environ.get("QT_DATABASE_USER")
    DATABASE_PASSWORD = environ.get("QT_DATABASE_PASSWORD")

DATABASE_PORT = 5432

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',

        'NAME': DATABASE_NAME,
        'USER': DATABASE_USER,
        'PASSWORD': DATABASE_PASSWORD,
        'HOST': DATABASE_HOST,
        'PORT': DATABASE_PORT,
        'ATOMIC_REQUESTS': True,
        'OPTIONS': {
            'isolation_level': psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE,
        },
    },
}

MYSQL_USERNAME = environ.get("QT_MYSQL_USERNAME")
MYSQL_DATABASE = DATABASE_NAME

if DEV_ENV:
    MYSQL_PASSWORD = environ.get("QT_DEV_MYSQL_PASSWORD")
else:
    MYSQL_PASSWORD = environ.get("QT_MYSQL_PASSWORD")

MYSQL_HOST = environ.get("QT_MYSQL_HOST")
MYSQL_PORT = int(environ.get("QT_MYSQL_PORT"))
DEBUG = DEV_ENV
SHOW_DEBUG = int(environ.get("QT_SHOW_DEBUG"))
SINCE_WHEN = datetime(2016, 10, 1)

DEFAULT_FROM_EMAIL = environ.get("QT_DEFAULT_FROM_EMAIL")
SERVER_EMAIL = DEFAULT_FROM_EMAIL

EMAIL_HOST = environ.get("EMAIL_HOST")
EMAIL_HOST_USER = environ.get("EMAIL_HOST_USER")
EMAIL_HOST_PASSWORD = environ.get("EMAIL_HOST_PASSWORD")
EMAIL_PORT = int(environ.get("EMAIL_PORT"))
EMAIL_USE_TLS = False
EMAIL_USE_SSL = False
EMAIL_NOREPLY = environ.get("EMAIL_NOREPLY")
#EMAIL_SSL_KEYFILE = 'C:\\nginx\\conf\\certs\\privkey.pem'
#EMAIL_SSL_CERTFILE = 'C:\\\nginx\\conf\\certs\\cert.pem'
NOTIFICATIONS_EMAILS = [environ.get("NOTIFICATIONS_EMAILS")]
NOTIFICATIONS_ENABLED = True

#SOCIAL_AUTH_EMAIL_VALIDATION_FUNCTION = 'collector.social_profile.SendVerificationEmail'
#SOCIAL_AUTH_EMAIL_VALIDATION_URL = '/email_verify_sent/'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Android; Mobile; rv:13.0) Gecko/13.0',
    'From': environ.get("QT_DEFAULT_FROM_EMAIL")
}

PROXIES = {
    'http': '',
    'https': '',
}

USE_L10N = False
USE_I18N = False
LANGUAGE_CODE = 'en-us'
DEFAULT_CHARSET = 'UTF-8'
USE_THOUSAND_SEPARATOR = True
NUMBER_GROUPING = 3

USE_TZ = False
TIME_ZONE = 'UTC'
DATETIME_FORMAT = 'N j, Y, HH'
SHORT_DATETIME_FORMAT = 'Y-m-d'
DATE_FORMAT = 'N j, Y'

LANGUAGES = [
    ('en', T('English')),
    ('de', T('German')),
    ('ru', T('Russian')),
    ('lt', T('Lithuanian')),
]

LANGUAGE_COOKIE_NAME = 'quantrade_lng'
LANGUAGE_COOKIE_AGE = 100

LOCALE_PATHS = [
    join(BASE_DIR, "collector", "locale"),
]

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'file': {
            'level': 'ERROR',
            'class': 'logging.FileHandler',
            'filename': join(BASE_DIR, 'logs', '{}_django.log'.format(FOLDER)),
        },
    },
    'loggers': {
        'django': {
            'handlers': ['file'],
            'level': 'ERROR',
            'propagate': True,
        },
    },
}

MIDDLEWARE_CLASSES = [
    'django.middleware.cache.UpdateCacheMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.locale.LocaleMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.cache.FetchFromCacheMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.auth.middleware.SessionAuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
     'django.middleware.common.BrokenLinkEmailsMiddleware',
    'django.contrib.flatpages.middleware.FlatpageFallbackMiddleware',
    'social.apps.django_app.middleware.SocialAuthExceptionMiddleware',
    'social.apps.django_app.middleware.SocialAuthExceptionMiddleware',
]

REST_FRAMEWORK = {
    'DEFAULT_PERMISSION_CLASSES': [
        #'rest_framework.permissions.DjangoModelPermissions',
        'rest_framework.permissions.IsAuthenticated',
    ]
}

SECRET_KEY = 'yjfhXwsp!O,uan2mcqcdwxtntihpimpetxqhyv+7zhbhc.ujbampcjs3@e5chfhqj3oucatkCrGkiRnb'

if DEV_ENV:
    ALLOWED_HOSTS = ['127.0.0.1', 'localhost', '0.0.0.0']
    SESSION_COOKIE_SECURE = False
    CSRF_COOKIE_SECURE = False
else:
    ALLOWED_HOSTS = ['quantrade.co.uk', 'www.quantrade.co.uk', '127.0.0.1', '173.212.207.88', 'localhost']
    SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
    SECURE_SSL_REDIRECT = True
    SECURE_CONTENT_TYPE_NOSNIFF = True
    SECURE_BROWSER_XSS_FILTER = True
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    CSRF_COOKIE_HTTPONLY = True
    X_FRAME_OPTIONS = 'DENY'
    SECURE_HSTS_SECONDS = 60
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True

SITE_ID = 3

ROOT_URLCONF = 'quantrade.urls'
TEMPL_DIRS = [join(BASE_DIR, "templates")]

if DEV_ENV:
    DEBUG_CONTEXT = ['django.template.context_processors.debug']
else:
    DEBUG_CONTEXT = []

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': TEMPL_DIRS,
        'APP_DIRS': True,
        'OPTIONS': {
            'debug':  DEBUG,
            'context_processors': [
                'django.contrib.auth.context_processors.auth',
                'collector.context_processors.extra_context',
                'django.template.context_processors.request',
                'django.contrib.messages.context_processors.messages',
                'django.template.context_processors.i18n',
                'social.apps.django_app.context_processors.backends',
                'social.apps.django_app.context_processors.login_redirect',
            ] + DEBUG_CONTEXT,
        },
    },
]

WSGI_APPLICATION = 'quantrade.wsgi.application'

from functools import lru_cache
from datetime import time, datetime, timedelta
from mimetypes import MimeTypes
from os import stat
from os.path import join

from django.contrib.syndication.views import Feed
from django.urls import reverse
from django.utils.html import strip_tags
from django.conf import settings

from .models import Signals, Post


def title_generator(item):
    if item.direction == 1:
        title = "{0} buy {1} ({2}) {3} {4} signal results".format(item.date_time, \
            item.symbol.symbol, item.broker.title, item.period.name, item.system)
    else:
        title = "{0} sell {1} ({2}) {3} {4} signal results".format(item.date_time, \
            item.symbol.symbol, item.broker.title, item.period.name, item.system)

    return title


def description_generator(item, strings):
    if item.direction == 1:
        description = "Buy signal for {0} {1} ({2}) {3} {4} {5}".format(\
            item.symbol.symbol, item.period.name, item.broker, item.system.title, \
            strings[0], item.returns)
    else:
        description = "Sell signal for {0} {1} ({2}) {3} {4} {5}".format(\
            item.symbol.symbol, item.period.name, item.broker.title, \
            item.system.title, strings[0], item.returns)

    return description


@lru_cache(maxsize=128)
class LatestSignalsFeed(Feed):
    title = 'Quantrade quantitative trading sugnals.'
    link = settings.BASE_URL
    folder = join(settings.STATIC_ROOT, 'static', 'collector', 'images', 'meta')

    description = 'Latest ({} days) of signals from machine generated \
        portfolios.'.format(settings.FEED_DAYS_TO_SHOW)

    def items(self):
        signals = Signals.objects.filter(date_time__gte=(datetime.now() - \
            timedelta(days=settings.FEED_DAYS_TO_SHOW))).exclude(returns__isnull=True, \
            returns__iexact=None).order_by('date_time').reverse()
        return signals

    def item_title(self, item):
        try:
            title = title_generator(item=item)
            return title
        except Exception as e:
            print(e)

    def item_pubdate(self, item):
        return datetime.combine(item.date_time, time())

    def item_link(self, item):
        if item.direction == 1:
            return "https://quantrade.co.uk/{0}/{1}/{2}/{3}/longs/".format(\
                item.broker.slug, item.symbol.symbol, item.period.period, \
                item.system.title)
        elif item.direction == 2:
            return "https://quantrade.co.uk/{0}/{1}/{2}/{3}/shorts/".format(\
                item.broker.slug, item.symbol.symbol, item.period.period, \
                item.system.title)

    def item_description(self, item):
        try:
            if item.returns != 0:
                if item.returns > 0:
                    description = description_generator(item=item, strings=['won'])
                else:
                    description = description_generator(item=item, strings=['lost'])

                return description
        except Exception as e:
            print(e)

    def item_enclosure_url(self, item):
        try:
            if item.direction == 1:
                filename = join(self.folder, '{1}=={2}=={3}=={4}==longs.png'.format(\
                    item.broker.slug, item.symbol.symbol, item.period.period, \
                    item.system.title))
                image = "{0}static/collector/images/meta/{1}=={2}=={3}=={4}==longs.png".format(\
                    self.link, item.broker.slug, item.symbol.symbol, item.period.period, item.system.title)
            elif item.direction == 2:
                filename = join(self.folder, '{1}=={2}=={3}=={4}==shorts.png'.format(\
                    item.broker.slug, item.symbol.symbol, item.period.period, item.system.title))
                image = "{0}static/collector/images/meta/{1}=={2}=={3}=={4}==shorts.png".format(\
                    self.link, item.broker.slug, item.symbol.symbol, item.period.period, item.system.title)

            if not (filename is None):
                return image
        except:
            return None

    def item_enclosure_length(self, item):
        try:
            if item.direction == 1:
                filename = join(self.folder, '{1}=={2}=={3}=={4}==longs.png'.format(\
                    item.broker.slug, item.symbol.symbol, item.period.period, item.system.title))
            elif item.direction == 2:
                filename = join(self.folder, '{1}=={2}=={3}=={4}==shorts.png'.format(\
                    item.broker.slug, item.symbol.symbol, item.period.period, item.system.title))

            size = stat(filename).st_size
        except:
            size = None

        if not (size is None):
            return size

    def item_enclosure_mime_type(self, item):
        mime = MimeTypes()
        try:
            if item.direction == 1:
                filename = join(self.folder, '{1}=={2}=={3}=={4}==longs.png'.format(\
                    item.broker.slug, item.symbol.symbol, item.period.period, item.system.title))
            elif item.direction == 2:
                filename = join(self.folder, '{1}=={2}=={3}=={4}==shorts.png'.format(\
                    item.broker.slug, item.symbol.symbol, item.period.period, item.system.title))

            mime_type = mime.guess_type(filename)[0]
            return mime_type
        except:
            return None

    def item_author_name(self, item):
        author = "Quantrade Ltd."
        return author

    def item_author_link(self, item):
        link = self.link
        return link


@lru_cache(maxsize=128)
class NewsFeed(Feed):
    title = 'Quantrade quantitative trading news.'
    description = 'Latest news from Quantrade Ltd.'
    link = settings.BASE_URL + 'blog/'

    def items(self):
        posts = Post.objects.filter().order_by('date_time').reverse()
        return posts


    def item_title(self, item):
        return item.title

    def item_link(self, item):
        link = self.link
        return link

    def item_pubdate(self, item):
        return item.date_time

    def item_description(self, item):
        return item.content

    def item_author_name(self, item):
        author = "Quantrade Ltd."
        return author

    def item_author_link(self, item):
        link = self.link
        return link

from os.path import (isfile, join)
from datetime import date

from django.conf import settings
from django.utils.html import strip_tags

import facebook
from clint.textui import colored

from .models import Signals, QtraUser, Brokers


def signal_poster(api, signal, strings):
    try:
        returns = float(signal.returns)

        if returns > 0:
            returns_string = "won"
        elif returns < 0:
            returns_string = "lost"
        else:
            returns_string = None

        if not (returns_string is None):
            content = "{0} {1} signal for #{2} {3} ${4} #trading #signals".format(signal.date_time, strings[0], signal.symbol.symbol, returns_string, returns)
            link = "https://quantrade.co.uk/{0}/{1}/{2}/{3}/{4}/".format(signal.broker.slug, signal.symbol.symbol, signal.period.period, signal.system.title, strings[1])
            media = "https://quantrade.co.uk/static/collector/images/meta/{0}=={1}=={2}=={3}=={4}.png".format(signal.broker.slug, signal.symbol.symbol, signal.period.period, signal.system.title, strings[1])

            attachment =  {
                'name': content,
                'link': link,
                'description': "Quantrade Ltd.: free profitable automted trading signals.",
                'picture': media
            }

            status = api.put_wall_post(message=content, attachment=attachment)
            print(colored.green("Published to Facebook: {0}".format(content)))

            signal.posted_to_facebook = True
            signal.save()
    except Exception as e:
        print(colored.red("[ERROR] At Facebook publish: {0}".format(e)))


def face_publish():
    cfg = {
        "page_id"      : settings.FACEBOOK_PAGE_ID,
        "access_token" : settings.FACEBOOK_PAGE_ACCESS_TOKEN
    }

    try:
        api = get_api(cfg)
    except Exception as e:
        print(colored.red("[ERROR] At Facebook API!: {0}\n".format(e)))

    signals = Signals.objects.filter(posted_to_facebook=False).exclude(returns__isnull=True)

    for signal in signals:
        try:
            if signal.direction == 1:
                signal_poster(api=api, signal=signal, strings=['Buy', 'longs'])
            elif signal.direction == 2:
                signal_poster(api=api, signal=signal, strings=['Sell', 'shorts'])

        except Exception as e:
            print(colored.red("At Facebook signal {}".format(e)))
            continue


def get_api(cfg):
    graph = facebook.GraphAPI(cfg['access_token'])

    return graph


def heatmap_to_facebook():
    try:
        now = date.today()
        d = now.day

        if d == 2:
            cfg = {
                "page_id"      : settings.FACEBOOK_PAGE_ID,
                "access_token" : settings.FACEBOOK_PAGE_ACCESS_TOKEN
            }

            try:
                api = get_api(cfg)
            except Exception as e:
                print(colored.red("[ERROR] At Facebook API!: {0}\n".format(e)))

            for broker in Brokers.objects.all():
                image_filename = join(settings.STATIC_ROOT, 'collector', 'images', \
                    'heatmap', '{0}=={1}=={2}=={3}=={4}.png'.format(broker.slug, \
                    'AI50', '1440', 'AI50', 'longs'))

                if isfile(image_filename):
                    media = "https://quantrade.co.uk/static/collector/images/heatmap/{0}=={1}=={2}=={3}=={4}.png".\
                        format(broker.slug, 'AI50', '1440', 'AI50', 'longs')
                else:
                    media = None

                status = "Results including last month index performance for {}.".format(broker.title)

                attachment =  {
                    'name': status,
                    'link': 'https://quantrade.co.uk/',
                    'description': "Quantrade Ltd.: free profitable automted trading signals.",
                    'picture': media
                }

                api.put_wall_post(message=status, attachment=attachment)
                print(colored.green("heatmap published to Facebook: {0}".format(status)))
    except Exception as e:
        print(colored.red("At heatmap_to_facebook {}".format(e)))
